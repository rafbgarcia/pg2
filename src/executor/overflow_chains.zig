//! Overflow-chain lifecycle for oversized string values.
//!
//! Manages spilling strings that exceed the inline threshold into
//! linked overflow pages, reading them back, WAL integration for
//! chain create/relink/unlink/reclaim records, and deferred reclaim
//! of stale chains after commit.
const std = @import("std");
const page_mod = @import("../storage/page.zig");
const heap_mod = @import("../storage/heap.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const wal_mod = @import("../storage/wal.zig");
const row_mod = @import("../storage/row.zig");
const overflow_mod = @import("../storage/overflow.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const scan_mod = @import("scan.zig");

const mutation = @import("mutation.zig");
const MutationError = mutation.MutationError;
const PinnedMutationPage = mutation.PinnedMutationPage;
const mapPoolError = mutation.mapPoolError;
const mapOverflowAllocatorError = mutation.mapOverflowAllocatorError;
const mapOverflowPageError = mutation.mapOverflowPageError;
const mapWalAppendError = mutation.mapWalAppendError;
const max_assignments = mutation.max_assignments;

const Page = page_mod.Page;
const HeapPage = heap_mod.HeapPage;
const BufferPool = buffer_pool_mod.BufferPool;
const Wal = wal_mod.Wal;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;
const OverflowPage = overflow_mod.OverflowPage;
const OverflowPageIdAllocator = overflow_mod.PageIdAllocator;
const Catalog = catalog_mod.Catalog;
const TxId = tx_mod.TxId;

const allocator_meta_magic: u16 = 0x4F46; // "OF"
const allocator_meta_version: u8 = 1;

pub const OverflowChainRecordMeta = struct {
    first_page_id: u64,
    page_count: u32,
    payload_bytes: u32,
};

pub const OverflowRelinkRecordMeta = struct {
    old_first_page_id: u64,
    new_first_page_id: u64,
};

pub const OverflowChainStats = struct {
    first_page_id: u64,
    page_count: u32,
    payload_bytes: u32,
};

pub fn markOversizedStringSlots(
    schema: *const RowSchema,
    values: []const Value,
    overflow_page_ids: []u64,
) void {
    std.debug.assert(values.len >= schema.column_count);
    std.debug.assert(overflow_page_ids.len >= schema.column_count);
    for (0..schema.column_count) |i| {
        if (values[i] == .null_value) continue;
        const col = schema.columns[i];
        if (col.column_type != .string) continue;
        if (values[i].string.len > overflow_mod.string_inline_threshold_bytes) {
            // Non-zero sentinel to force pointer-sized dry-run encoding.
            overflow_page_ids[i] = 1;
        }
    }
}

pub fn spillOversizedStrings(
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    overflow_allocator: *OverflowPageIdAllocator,
    schema: *const RowSchema,
    values: []const Value,
    overflow_page_ids: []u64,
    overflow_chain_stats: []OverflowChainStats,
) MutationError!void {
    std.debug.assert(values.len >= schema.column_count);
    std.debug.assert(overflow_page_ids.len >= schema.column_count);
    std.debug.assert(overflow_chain_stats.len >= schema.column_count);
    for (0..schema.column_count) |i| {
        if (overflow_page_ids[i] == 0) continue;
        if (values[i] == .null_value) continue;
        const col = schema.columns[i];
        if (col.column_type != .string) continue;
        overflow_chain_stats[i] = try writeOverflowChain(
            pool,
            wal,
            tx_id,
            overflow_allocator,
            values[i].string,
        );
        overflow_page_ids[i] = overflow_chain_stats[i].first_page_id;
    }
}

pub fn writeOverflowChain(
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    overflow_allocator: *OverflowPageIdAllocator,
    payload: []const u8,
) MutationError!OverflowChainStats {
    std.debug.assert(payload.len > overflow_mod.string_inline_threshold_bytes);
    const chunk_capacity = OverflowPage.max_payload_len();
    std.debug.assert(chunk_capacity > 0);
    try ensureOverflowAllocatorMetadata(pool, overflow_allocator);

    const first_page_id = try allocateOverflowPage(
        pool,
        wal,
        tx_id,
        overflow_allocator,
    );
    var page_count: u32 = 0;

    var payload_offset: usize = 0;
    var current_page_id = first_page_id;
    while (payload_offset < payload.len) {
        const remaining = payload.len - payload_offset;
        const chunk_len = @min(remaining, chunk_capacity);
        const next_page_id = if (chunk_len == remaining)
            OverflowPage.null_page_id
        else
            try allocateOverflowPage(
                pool,
                wal,
                tx_id,
                overflow_allocator,
            );

        var pinned = try PinnedMutationPage.pin(pool, current_page_id);
        defer pinned.release();
        if (pinned.page.header.page_type == .free) {
            OverflowPage.init(pinned.page);
        } else if (pinned.page.header.page_type != .overflow) {
            return error.Corruption;
        }
        OverflowPage.writeChunk(
            pinned.page,
            payload[payload_offset..][0..chunk_len],
            next_page_id,
        ) catch |e| return mapOverflowPageError(e);
        pinned.markDirty();

        payload_offset += chunk_len;
        page_count += 1;
        current_page_id = next_page_id;
    }

    var payload_buf: [16]u8 = undefined;
    const payload_meta: OverflowChainRecordMeta = .{
        .first_page_id = first_page_id,
        .page_count = page_count,
        .payload_bytes = @intCast(payload.len),
    };
    const payload_len = encodeOverflowChainRecordMeta(payload_buf[0..], payload_meta);
    _ = wal.append(
        tx_id,
        .overflow_chain_create,
        first_page_id,
        payload_buf[0..payload_len],
    ) catch |e| return mapWalAppendError(e);

    return .{
        .first_page_id = first_page_id,
        .page_count = page_count,
        .payload_bytes = @intCast(payload.len),
    };
}

pub fn decodeRowWithOverflow(
    schema: *const RowSchema,
    row_data: []const u8,
    pool: *BufferPool,
    overflow_allocator: *const OverflowPageIdAllocator,
    string_arena: *scan_mod.StringArena,
    out: []Value,
) MutationError!void {
    std.debug.assert(out.len >= schema.column_count);
    var col_idx: u16 = 0;
    while (col_idx < schema.column_count) : (col_idx += 1) {
        const decoded = row_mod.decodeColumnStorageChecked(
            schema,
            row_data,
            col_idx,
        ) catch return error.Corruption;
        out[col_idx] = switch (decoded) {
            .value => |v| v,
            .string_overflow_page_id => |first_page_id| .{
                .string = try resolveOverflowStringIntoArena(
                    pool,
                    overflow_allocator,
                    first_page_id,
                    string_arena,
                ),
            },
        };
    }
}

pub fn resolveOverflowStringIntoArena(
    pool: *BufferPool,
    overflow_allocator: *const OverflowPageIdAllocator,
    first_page_id: u64,
    string_arena: *scan_mod.StringArena,
) MutationError![]const u8 {
    if (!overflow_allocator.ownsPageId(first_page_id)) return error.Corruption;
    const start = string_arena.startString();
    var current = first_page_id;
    var hops: u64 = 0;
    const max_hops = overflow_allocator.capacity();
    while (true) {
        if (hops >= max_hops) return error.Corruption;
        hops += 1;

        var pinned = try PinnedMutationPage.pin(pool, current);
        defer pinned.release();
        if (pinned.page.header.page_type != .overflow) return error.Corruption;

        const chunk = OverflowPage.readChunk(pinned.page) catch return error.Corruption;
        string_arena.appendChunk(chunk.payload) catch return error.OutOfMemory;
        if (chunk.next_page_id == OverflowPage.null_page_id) break;
        if (!overflow_allocator.ownsPageId(chunk.next_page_id)) return error.Corruption;
        current = chunk.next_page_id;
    }
    return string_arena.finishString(start);
}

pub fn collectOverflowRootsFromRow(
    schema: *const RowSchema,
    row_data: []const u8,
    out_overflow_roots: []u64,
) MutationError!usize {
    std.debug.assert(out_overflow_roots.len >= schema.column_count);
    var count: usize = 0;
    for (0..schema.column_count) |i| {
        const decoded = row_mod.decodeColumnStorageChecked(
            schema,
            row_data,
            @intCast(i),
        ) catch return error.Corruption;
        switch (decoded) {
            .value => {},
            .string_overflow_page_id => |first_page_id| {
                if (first_page_id == 0) return error.Corruption;
                var seen = false;
                for (0..count) |existing_idx| {
                    if (out_overflow_roots[existing_idx] == first_page_id) {
                        seen = true;
                        break;
                    }
                }
                if (seen) continue;
                if (count >= out_overflow_roots.len) return error.Corruption;
                out_overflow_roots[count] = first_page_id;
                count += 1;
            },
        }
    }
    return count;
}

pub fn appendOverflowRelinkWalForRow(
    wal: *Wal,
    tx_id: TxId,
    row_page_id: u64,
    new_overflow_page_ids: []const u64,
    old_first_page_id: u64,
) MutationError!void {
    for (new_overflow_page_ids) |new_first_page_id| {
        if (new_first_page_id == 0) continue;
        var payload_buf: [16]u8 = undefined;
        const payload_len = encodeOverflowRelinkRecordMeta(
            payload_buf[0..],
            .{
                .old_first_page_id = old_first_page_id,
                .new_first_page_id = new_first_page_id,
            },
        );
        _ = wal.append(
            tx_id,
            .overflow_chain_relink,
            row_page_id,
            payload_buf[0..payload_len],
        ) catch |e| return mapWalAppendError(e);
    }
}

pub fn enqueueOverflowChainForReclaim(
    catalog: *Catalog,
    wal: *Wal,
    tx_id: TxId,
    first_page_id: u64,
) MutationError!void {
    catalog.overflow_reclaim_queue.enqueue(tx_id, first_page_id) catch |e| {
        return switch (e) {
            error.InvalidChainRoot => error.Corruption,
            error.QueueFull => error.OverflowReclaimQueueFull,
            error.QueueEmpty => error.Corruption,
            error.DuplicateChainRoot => error.Corruption,
        };
    };
    catalog.recordOverflowReclaimEnqueue();
    var payload_buf: [16]u8 = undefined;
    const payload_len = encodeOverflowChainRecordMeta(
        payload_buf[0..],
        .{
            .first_page_id = first_page_id,
            .page_count = 0,
            .payload_bytes = 0,
        },
    );
    _ = wal.append(
        tx_id,
        .overflow_chain_unlink,
        first_page_id,
        payload_buf[0..payload_len],
    ) catch |e| return mapWalAppendError(e);
}

pub fn drainOverflowReclaimQueue(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    max_items: usize,
) MutationError!void {
    try ensureOverflowAllocatorMetadata(pool, &catalog.overflow_page_allocator);
    var processed: usize = 0;
    while (processed < max_items and !catalog.overflow_reclaim_queue.isEmpty()) : (processed += 1) {
        const first_page_id = (catalog.overflow_reclaim_queue.dequeueCommitted() catch
            return error.Corruption) orelse break;
        catalog.recordOverflowReclaimDequeue();
        const page_count = reclaimOverflowChain(
            catalog,
            pool,
            wal,
            tx_id,
            first_page_id,
        ) catch |err| {
            catalog.recordOverflowReclaimFailure();
            return err;
        };
        catalog.recordOverflowReclaimSuccess(page_count);
        var payload_buf: [16]u8 = undefined;
        const payload_len = encodeOverflowChainRecordMeta(
            payload_buf[0..],
            .{
                .first_page_id = first_page_id,
                .page_count = page_count,
                .payload_bytes = 0,
            },
        );
        _ = wal.append(
            tx_id,
            .overflow_chain_reclaim,
            first_page_id,
            payload_buf[0..payload_len],
        ) catch |e| return mapWalAppendError(e);
    }
}

pub fn commitOverflowReclaimEntriesForTx(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    max_items: usize,
) MutationError!void {
    catalog.overflow_reclaim_queue.commitTx(tx_id);
    try drainOverflowReclaimQueue(catalog, pool, wal, tx_id, max_items);
}

pub fn rollbackOverflowReclaimEntriesForTx(
    catalog: *Catalog,
    tx_id: TxId,
) void {
    catalog.overflow_reclaim_queue.abortTx(tx_id);
}

pub fn reclaimOverflowChain(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    first_page_id: u64,
) MutationError!u32 {
    const overflow_allocator = &catalog.overflow_page_allocator;
    if (!overflow_allocator.ownsPageId(first_page_id)) return error.Corruption;

    var page_count: u32 = 0;
    var hops: u64 = 0;
    const max_hops = overflow_allocator.capacity();
    var current = first_page_id;
    while (true) {
        if (hops >= max_hops) return error.Corruption;
        hops += 1;

        var pinned = try PinnedMutationPage.pin(pool, current);
        defer pinned.release();
        if (pinned.page.header.page_type != .overflow) return error.Corruption;

        const chunk = OverflowPage.readChunk(pinned.page) catch return error.Corruption;
        const next_page_id = chunk.next_page_id;
        if (next_page_id != OverflowPage.null_page_id and
            !overflow_allocator.ownsPageId(next_page_id))
        {
            return error.Corruption;
        }

        const prev_head = overflow_allocator.freeListHead();
        const push_lsn = try appendOverflowFreeListPushWal(
            wal,
            tx_id,
            current,
            prev_head,
            current,
            overflow_allocator.next_page_id,
        );
        writeFreeListNextPointer(pinned.page, prev_head);
        pinned.page.header.page_type = .free;
        const pushed_prev_head = overflow_allocator.pushFreeListHead(current) catch |err|
            return mapOverflowAllocatorError(err);
        std.debug.assert(pushed_prev_head == prev_head);
        pinned.page.header.lsn = push_lsn;
        pinned.markDirty();
        try persistOverflowAllocatorMetadata(
            pool,
            overflow_allocator,
            push_lsn,
        );
        page_count += 1;

        if (next_page_id == OverflowPage.null_page_id) break;
        current = next_page_id;
    }
    return page_count;
}

fn allocateOverflowPage(
    pool: *BufferPool,
    wal: *Wal,
    tx_id: TxId,
    overflow_allocator: *OverflowPageIdAllocator,
) MutationError!u64 {
    if (overflow_allocator.hasFreePages()) {
        const head = overflow_allocator.freeListHead();
        var pinned = try PinnedMutationPage.pin(pool, head);
        defer pinned.release();
        if (pinned.page.header.page_type != .free) return error.Corruption;
        const next_head = readFreeListNextPointer(pinned.page, overflow_allocator) catch
            return error.Corruption;
        const pop_lsn = try appendOverflowFreeListPopWal(
            wal,
            tx_id,
            head,
            next_head,
            overflow_allocator.next_page_id,
        );
        const page_id = overflow_allocator.popFreeListHead(next_head) catch |err|
            return mapOverflowAllocatorError(err);
        std.debug.assert(page_id == head);
        pinned.page.header.lsn = pop_lsn;
        try persistOverflowAllocatorMetadata(pool, overflow_allocator, pop_lsn);
        return page_id;
    }

    if (overflow_allocator.next_page_id >= overflow_allocator.region_end_page_id) {
        return mapOverflowAllocatorError(error.RegionExhausted);
    }
    const page_id = overflow_allocator.next_page_id;
    const next_page_id = page_id + 1;
    const pop_lsn = try appendOverflowFreeListPopWal(
        wal,
        tx_id,
        page_id,
        overflow_allocator.freeListHead(),
        next_page_id,
    );
    const allocated_page_id = overflow_allocator.allocateFresh() catch |err|
        return mapOverflowAllocatorError(err);
    std.debug.assert(allocated_page_id == page_id);
    try persistOverflowAllocatorMetadata(pool, overflow_allocator, pop_lsn);
    return page_id;
}

fn ensureOverflowAllocatorMetadata(
    pool: *BufferPool,
    overflow_allocator: *OverflowPageIdAllocator,
) MutationError!void {
    if (overflow_allocator.metadata_loaded) return;
    const meta_page_id = overflow_allocator.metadataPageId();
    var pinned = try PinnedMutationPage.pin(pool, meta_page_id);
    defer pinned.release();
    if (pinned.page.header.page_type == .free and isAllocatorMetaPage(pinned.page)) {
        const loaded = decodeAllocatorMetadataPage(pinned.page) catch return error.Corruption;
        overflow_allocator.setAllocatorState(loaded.free_list_head, loaded.next_page_id) catch |err|
            return mapOverflowAllocatorError(err);
        return;
    }

    pinned.page.header.page_type = .free;
    writeAllocatorMetadataPage(pinned.page, overflow_allocator.freeListHead(), overflow_allocator.next_page_id);
    overflow_allocator.markMetadataLoaded();
    pinned.markDirty();
}

fn persistOverflowAllocatorMetadata(
    pool: *BufferPool,
    overflow_allocator: *const OverflowPageIdAllocator,
    lsn: u64,
) MutationError!void {
    var pinned = try PinnedMutationPage.pin(pool, overflow_allocator.metadataPageId());
    defer pinned.release();
    if (pinned.page.header.page_type != .free and pinned.page.header.page_type != .overflow) {
        return error.Corruption;
    }
    pinned.page.header.page_type = .free;
    pinned.page.header.lsn = lsn;
    writeAllocatorMetadataPage(
        pinned.page,
        overflow_allocator.freeListHead(),
        overflow_allocator.next_page_id,
    );
    pinned.markDirty();
}

const AllocatorMetadata = struct {
    free_list_head: u64,
    next_page_id: u64,
};

fn writeAllocatorMetadataPage(
    page: *Page,
    free_list_head: u64,
    next_page_id: u64,
) void {
    @memset(&page.content, 0);
    @memcpy(page.content[0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, allocator_meta_magic)));
    page.content[2] = allocator_meta_version;
    page.content[3] = 0;
    @memcpy(page.content[4..12], std.mem.asBytes(&std.mem.nativeToLittle(u64, free_list_head)));
    @memcpy(page.content[12..20], std.mem.asBytes(&std.mem.nativeToLittle(u64, next_page_id)));
}

fn isAllocatorMetaPage(page: *const Page) bool {
    if (page.content[2] != allocator_meta_version) return false;
    const magic = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, page.content[0..2]).*);
    return magic == allocator_meta_magic;
}

fn decodeAllocatorMetadataPage(page: *const Page) MutationError!AllocatorMetadata {
    if (!isAllocatorMetaPage(page)) return error.Corruption;
    return .{
        .free_list_head = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, page.content[4..12]).*),
        .next_page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, page.content[12..20]).*),
    };
}

fn writeFreeListNextPointer(page: *Page, next_page_id: u64) void {
    @memset(&page.content, 0);
    @memcpy(page.content[0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, next_page_id)));
}

fn readFreeListNextPointer(
    page: *const Page,
    overflow_allocator: *const OverflowPageIdAllocator,
) MutationError!u64 {
    const next_page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, page.content[0..8]).*);
    if (next_page_id != 0 and !overflow_allocator.ownsPageId(next_page_id)) {
        return error.Corruption;
    }
    return next_page_id;
}

fn appendOverflowFreeListPushWal(
    wal: *Wal,
    tx_id: TxId,
    page_id: u64,
    previous_head: u64,
    new_head: u64,
    next_page_id: u64,
) MutationError!u64 {
    var payload: [24]u8 = undefined;
    @memcpy(payload[0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, previous_head)));
    @memcpy(payload[8..16], std.mem.asBytes(&std.mem.nativeToLittle(u64, new_head)));
    @memcpy(payload[16..24], std.mem.asBytes(&std.mem.nativeToLittle(u64, next_page_id)));
    return wal.append(tx_id, .overflow_free_list_push, page_id, payload[0..]) catch |e|
        mapWalAppendError(e);
}

fn appendOverflowFreeListPopWal(
    wal: *Wal,
    tx_id: TxId,
    page_id: u64,
    new_head: u64,
    next_page_id: u64,
) MutationError!u64 {
    var payload: [16]u8 = undefined;
    @memcpy(payload[0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, new_head)));
    @memcpy(payload[8..16], std.mem.asBytes(&std.mem.nativeToLittle(u64, next_page_id)));
    return wal.append(tx_id, .overflow_free_list_pop, page_id, payload[0..]) catch |e|
        mapWalAppendError(e);
}

pub fn encodeOverflowChainRecordMeta(
    out: []u8,
    meta: OverflowChainRecordMeta,
) usize {
    std.debug.assert(out.len >= 16);
    @memcpy(out[0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, meta.first_page_id)));
    @memcpy(out[8..12], std.mem.asBytes(&std.mem.nativeToLittle(u32, meta.page_count)));
    @memcpy(out[12..16], std.mem.asBytes(&std.mem.nativeToLittle(u32, meta.payload_bytes)));
    return 16;
}

pub fn decodeOverflowChainRecordMeta(payload: []const u8) MutationError!OverflowChainRecordMeta {
    if (payload.len != 16) return error.Corruption;
    return .{
        .first_page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[0..8]).*),
        .page_count = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, payload[8..12]).*),
        .payload_bytes = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, payload[12..16]).*),
    };
}

pub fn encodeOverflowRelinkRecordMeta(
    out: []u8,
    meta: OverflowRelinkRecordMeta,
) usize {
    std.debug.assert(out.len >= 16);
    @memcpy(out[0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, meta.old_first_page_id)));
    @memcpy(out[8..16], std.mem.asBytes(&std.mem.nativeToLittle(u64, meta.new_first_page_id)));
    return 16;
}

pub fn decodeOverflowRelinkRecordMeta(payload: []const u8) MutationError!OverflowRelinkRecordMeta {
    if (payload.len != 16) return error.Corruption;
    return .{
        .old_first_page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[0..8]).*),
        .new_first_page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[8..16]).*),
    };
}

const testing = std.testing;
const disk_mod = @import("../simulator/disk.zig");

test "reclaimOverflowChain leaves allocator/page unchanged when free-list push WAL append fails" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 8);
    defer pool.deinit();

    var wal = Wal.init(testing.allocator, disk.storage());
    defer wal.deinit();
    try wal.reserveBufferCapacity(1);

    var catalog: Catalog = .{};
    catalog.overflow_page_allocator = try OverflowPageIdAllocator.initWithBounds(40_000, 4);

    const first_page_id = catalog.overflow_page_allocator.firstAllocatablePageId();
    try catalog.overflow_page_allocator.setAllocatorState(0, first_page_id + 1);

    {
        const page = try pool.pin(first_page_id);
        OverflowPage.init(page);
        try OverflowPage.writeChunk(page, "safe", OverflowPage.null_page_id);
        pool.unpin(first_page_id, true);
    }

    try testing.expectError(
        error.OutOfMemory,
        reclaimOverflowChain(
            &catalog,
            &pool,
            &wal,
            1,
            first_page_id,
        ),
    );

    try testing.expectEqual(@as(u64, 0), catalog.overflow_page_allocator.freeListHead());

    const page = try pool.pin(first_page_id);
    defer pool.unpin(first_page_id, false);
    try testing.expectEqual(page_mod.PageType.overflow, page.header.page_type);
    const chunk = try OverflowPage.readChunk(page);
    try testing.expectEqual(OverflowPage.null_page_id, chunk.next_page_id);
    try testing.expectEqualSlices(u8, "safe", chunk.payload);
}

test "allocateOverflowPage keeps free-list head when free-list pop WAL append fails" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(testing.allocator, disk.storage(), 8);
    defer pool.deinit();

    var wal = Wal.init(testing.allocator, disk.storage());
    defer wal.deinit();
    try wal.reserveBufferCapacity(1);

    var allocator = try OverflowPageIdAllocator.initWithBounds(50_000, 4);
    const head = allocator.firstAllocatablePageId();
    try allocator.setAllocatorState(head, head + 1);

    {
        const page = try pool.pin(head);
        page.header.page_type = .free;
        writeFreeListNextPointer(page, 0);
        pool.unpin(head, true);
    }

    try testing.expectError(
        error.OutOfMemory,
        allocateOverflowPage(
            &pool,
            &wal,
            1,
            &allocator,
        ),
    );

    try testing.expectEqual(head, allocator.freeListHead());

    const page = try pool.pin(head);
    defer pool.unpin(head, false);
    try testing.expectEqual(page_mod.PageType.free, page.header.page_type);
    try testing.expectEqual(@as(u64, 0), try readFreeListNextPointer(page, &allocator));
}
