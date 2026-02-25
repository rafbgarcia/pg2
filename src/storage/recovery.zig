//! WAL replay helpers for committed overflow lifecycle recovery.
//!
//! Responsibilities in this file:
//! - Reads WAL records in bounded buffers for deterministic recovery.
//! - Filters replay to committed transactions with strict tx markers.
//! - Validates overflow lifecycle record payloads and allocator ownership.
//! - Applies idempotent overflow-chain reclaim into buffer-pool page state.
//!
//! Why this exists:
//! - Restart/recovery needs a single fail-closed path for overflow chain cleanup.
//! - Idempotent reclaim allows safe re-application across repeated recovery runs.
//!
//! Boundaries:
//! - This module focuses on overflow lifecycle replay, not full database recovery.
//! - It relies on `BufferPool` and `Wal` primitives; checkpoint orchestration and
//!   broader replay ordering policy are handled at higher layers.
//!
//! Contributor notes:
//! - Keep corruption checks strict (page ownership, payload shapes, hop bounds).
//! - Preserve allocation-free behavior in core replay paths by honoring caller
//!   supplied record/payload buffers.
//! - Treat idempotency as a correctness requirement, not an optimization.
const std = @import("std");
const buffer_pool_mod = @import("buffer_pool.zig");
const overflow_mod = @import("overflow.zig");
const page_mod = @import("page.zig");
const wal_mod = @import("wal.zig");

const BufferPool = buffer_pool_mod.BufferPool;
const OverflowPage = overflow_mod.OverflowPage;
const OverflowPageIdAllocator = overflow_mod.PageIdAllocator;
const PageType = page_mod.PageType;
const Record = wal_mod.Record;
const RecordType = wal_mod.RecordType;
const Wal = wal_mod.Wal;

pub const ReplayStats = struct {
    total_records: usize = 0,
    overflow_reclaim_records_seen: usize = 0,
    overflow_reclaim_applied: usize = 0,
    overflow_reclaim_idempotent_skips: usize = 0,
    overflow_free_list_push_records_seen: usize = 0,
    overflow_free_list_pop_records_seen: usize = 0,
    slot_reclaim_records_seen: usize = 0,
    slot_reclaim_applied: usize = 0,
    slot_reclaim_idempotent_skips: usize = 0,
};

pub const RecoveryError = error{
    Corruption,
} || buffer_pool_mod.BufferPoolError || wal_mod.WalError;

const OverflowChainRecordMeta = struct {
    first_page_id: u64,
    page_count: u32,
    payload_bytes: u32,
};

const OverflowFreeListPush = struct {
    previous_head: u64,
    new_head: u64,
    next_page_id: u64,
};

const OverflowFreeListPop = struct {
    new_head: u64,
    next_page_id: u64,
};

const TxReplayDecision = enum {
    replay,
    skip,
};

/// Replays committed overflow lifecycle WAL records into page state.
///
/// Caller supplies bounded record/payload buffers to keep recovery deterministic
/// and allocation-free in core paths.
pub fn replayCommittedOverflowLifecycle(
    pool: *BufferPool,
    wal: *Wal,
    overflow_allocator: *OverflowPageIdAllocator,
    records_buf: []Record,
    payload_buf: []u8,
) RecoveryError!ReplayStats {
    const decoded = try wal.readFromInto(1, records_buf, payload_buf);
    const records = records_buf[0..decoded.records_len];
    var stats: ReplayStats = .{
        .total_records = records.len,
    };

    for (records) |rec| {
        const tx_replay = try classifyTxReplay(records, rec.tx_id);
        if (tx_replay != .replay) continue;
        switch (rec.record_type) {
            .overflow_chain_create => {
                const meta = try decodeOverflowChainRecordMeta(rec.payload);
                if (meta.first_page_id == 0) return error.Corruption;
                if (!overflow_allocator.ownsPageId(meta.first_page_id)) return error.Corruption;
                if (meta.page_count == 0) return error.Corruption;
                if (meta.payload_bytes == 0) return error.Corruption;
            },
            .overflow_chain_relink => {
                const old_first_page_id = decodeU64(rec.payload, 0) catch return error.Corruption;
                const new_first_page_id = decodeU64(rec.payload, 8) catch return error.Corruption;
                if (new_first_page_id == 0) return error.Corruption;
                if (!overflow_allocator.ownsPageId(new_first_page_id)) {
                    return error.Corruption;
                }
                if (old_first_page_id != 0 and !overflow_allocator.ownsPageId(old_first_page_id)) {
                    return error.Corruption;
                }
            },
            .overflow_chain_unlink => {
                const meta = try decodeOverflowChainRecordMeta(rec.payload);
                if (meta.first_page_id == 0) return error.Corruption;
                if (!overflow_allocator.ownsPageId(meta.first_page_id)) return error.Corruption;
            },
            .overflow_chain_reclaim => {
                const meta = try decodeOverflowChainRecordMeta(rec.payload);
                if (meta.first_page_id == 0) return error.Corruption;
                if (!overflow_allocator.ownsPageId(meta.first_page_id)) return error.Corruption;
                if (meta.page_count == 0) return error.Corruption;

                stats.overflow_reclaim_records_seen += 1;
                const applied = try applyOverflowReclaimIdempotent(
                    pool,
                    overflow_allocator,
                    meta.first_page_id,
                );
                if (applied) {
                    stats.overflow_reclaim_applied += 1;
                } else {
                    stats.overflow_reclaim_idempotent_skips += 1;
                }
            },
            .overflow_free_list_push => {
                const push = try decodeOverflowFreeListPush(rec.payload);
                if (!overflow_allocator.ownsPageId(rec.page_id)) return error.Corruption;
                if (push.previous_head != 0 and !overflow_allocator.ownsPageId(push.previous_head)) {
                    return error.Corruption;
                }
                if (push.new_head != rec.page_id) return error.Corruption;
                if (push.next_page_id < overflow_allocator.firstAllocatablePageId() or
                    push.next_page_id > overflow_allocator.region_end_page_id)
                {
                    return error.Corruption;
                }
                try applyOverflowFreeListPush(
                    pool,
                    overflow_allocator,
                    rec.page_id,
                    push.previous_head,
                    push.next_page_id,
                );
                stats.overflow_free_list_push_records_seen += 1;
            },
            .overflow_free_list_pop => {
                const pop = try decodeOverflowFreeListPop(rec.payload);
                if (!overflow_allocator.ownsPageId(rec.page_id)) return error.Corruption;
                if (pop.new_head != 0 and !overflow_allocator.ownsPageId(pop.new_head)) {
                    return error.Corruption;
                }
                if (pop.next_page_id < overflow_allocator.firstAllocatablePageId() or
                    pop.next_page_id > overflow_allocator.region_end_page_id)
                {
                    return error.Corruption;
                }
                overflow_allocator.setAllocatorState(pop.new_head, pop.next_page_id) catch
                    return error.Corruption;
                stats.overflow_free_list_pop_records_seen += 1;
            },
            .reclaim_slot => {
                if (rec.payload.len != 2) return error.Corruption;
                const slot = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, rec.payload[0..2]).*);
                stats.slot_reclaim_records_seen += 1;
                const applied = try applySlotReclaimIdempotent(pool, rec.page_id, slot);
                if (applied) {
                    stats.slot_reclaim_applied += 1;
                } else {
                    stats.slot_reclaim_idempotent_skips += 1;
                }
            },
            else => {},
        }
    }

    return stats;
}

fn applySlotReclaimIdempotent(
    pool: *BufferPool,
    page_id: u64,
    slot: u16,
) RecoveryError!bool {
    const heap_mod = @import("heap.zig");
    const HeapPage = heap_mod.HeapPage;

    const page = try pool.pin(page_id);
    var dirty = false;
    defer pool.unpin(page_id, dirty);
    if (page.header.page_type != .heap) return error.Corruption;

    const reclaimed = HeapPage.reclaim(page, slot) catch |err| switch (err) {
        error.InvalidSlot => return error.Corruption,
        error.PageFull => return error.Corruption,
        error.RowTooLarge => return error.Corruption,
    };
    if (reclaimed) {
        dirty = true;
        return true;
    }
    return false;
}

fn classifyTxReplay(
    records: []const Record,
    tx_id: u64,
) RecoveryError!TxReplayDecision {
    var saw_begin = false;
    var saw_commit = false;
    var saw_abort = false;
    var saw_mutation = false;
    for (records) |rec| {
        if (rec.tx_id != tx_id) continue;
        switch (rec.record_type) {
            .tx_commit => {
                if (saw_commit or saw_abort) return error.Corruption;
                saw_commit = true;
            },
            .tx_abort => {
                if (saw_abort or saw_commit) return error.Corruption;
                saw_abort = true;
            },
            .tx_begin => {
                if (saw_begin) return error.Corruption;
                saw_begin = true;
            },
            // B+ tree index records use tx_id=0 (no transaction context) and
            // are structural maintenance — not transactional mutations.
            .btree_insert,
            .btree_delete,
            .btree_split_leaf,
            .btree_split_internal,
            .btree_new_root,
            .checkpoint,
            => {},
            else => saw_mutation = true,
        }
    }
    if (!saw_mutation) return .skip;
    if (!saw_begin) return error.Corruption;
    if (saw_commit) return .replay;
    return .skip;
}

fn decodeOverflowChainRecordMeta(payload: []const u8) RecoveryError!OverflowChainRecordMeta {
    if (payload.len != 16) return error.Corruption;
    return .{
        .first_page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[0..8]).*),
        .page_count = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, payload[8..12]).*),
        .payload_bytes = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, payload[12..16]).*),
    };
}

fn decodeU64(payload: []const u8, offset: usize) RecoveryError!u64 {
    if (offset + 8 > payload.len) return error.Corruption;
    return std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[offset .. offset + 8]).*);
}

fn decodeOverflowFreeListPush(payload: []const u8) RecoveryError!OverflowFreeListPush {
    if (payload.len != 24) return error.Corruption;
    return .{
        .previous_head = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[0..8]).*),
        .new_head = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[8..16]).*),
        .next_page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[16..24]).*),
    };
}

fn decodeOverflowFreeListPop(payload: []const u8) RecoveryError!OverflowFreeListPop {
    if (payload.len != 16) return error.Corruption;
    return .{
        .new_head = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[0..8]).*),
        .next_page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[8..16]).*),
    };
}

fn encodeOverflowChainRecordMetaForTest(
    out: []u8,
    meta: OverflowChainRecordMeta,
) void {
    std.debug.assert(out.len >= 16);
    @memcpy(out[0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, meta.first_page_id)));
    @memcpy(out[8..12], std.mem.asBytes(&std.mem.nativeToLittle(u32, meta.page_count)));
    @memcpy(out[12..16], std.mem.asBytes(&std.mem.nativeToLittle(u32, meta.payload_bytes)));
}

fn encodeOverflowRelinkRecordMetaForTest(
    out: []u8,
    old_first_page_id: u64,
    new_first_page_id: u64,
) void {
    std.debug.assert(out.len >= 16);
    @memcpy(out[0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, old_first_page_id)));
    @memcpy(out[8..16], std.mem.asBytes(&std.mem.nativeToLittle(u64, new_first_page_id)));
}

fn applyOverflowReclaimIdempotent(
    pool: *BufferPool,
    overflow_allocator: *const OverflowPageIdAllocator,
    first_page_id: u64,
) RecoveryError!bool {
    if (!overflow_allocator.ownsPageId(first_page_id)) return error.Corruption;

    const first_page = try pool.pin(first_page_id);
    defer pool.unpin(first_page_id, false);

    if (first_page.header.page_type == .free) return false;
    if (first_page.header.page_type != .overflow) return error.Corruption;

    var current = first_page_id;
    var hops: u64 = 0;
    const max_hops = overflow_allocator.capacity();
    while (true) {
        if (hops >= max_hops) return error.Corruption;
        hops += 1;

        var pinned = try pool.pin(current);
        defer pool.unpin(current, true);

        if (pinned.header.page_type != .overflow) return error.Corruption;
        const chunk = OverflowPage.readChunk(pinned) catch return error.Corruption;
        const next_page_id = chunk.next_page_id;
        if (next_page_id != OverflowPage.null_page_id and !overflow_allocator.ownsPageId(next_page_id)) {
            return error.Corruption;
        }

        pinned.header.page_type = .free;
        @memset(&pinned.content, 0);

        if (next_page_id == OverflowPage.null_page_id) break;
        current = next_page_id;
    }
    return true;
}

fn applyOverflowFreeListPush(
    pool: *BufferPool,
    overflow_allocator: *OverflowPageIdAllocator,
    page_id: u64,
    previous_head: u64,
    next_page_id: u64,
) RecoveryError!void {
    const page = try pool.pin(page_id);
    defer pool.unpin(page_id, true);
    page.header.page_type = .free;
    @memset(&page.content, 0);
    @memcpy(page.content[0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, previous_head)));
    overflow_allocator.setAllocatorState(page_id, next_page_id) catch return error.Corruption;
}

test "replayCommittedOverflowLifecycle reclaims chain and is idempotent" {
    const catalog_mod = @import("../catalog/catalog.zig");
    const disk_mod = @import("../simulator/disk.zig");
    const mutation_mod = @import("../executor/mutation.zig");

    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();
    var catalog: catalog_mod.Catalog = .{};
    catalog.overflow_page_allocator = try OverflowPageIdAllocator.initWithBounds(20_000, 8);

    const tx: u64 = 1;
    _ = try wal.beginTx(tx);
    const long_len = 1200;
    var long_text: [long_len]u8 = undefined;
    @memset(long_text[0..], 'r');
    const schema_text =
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    ;
    const parser_mod = @import("../parser/parser.zig");
    const tokenizer_mod = @import("../parser/tokenizer.zig");
    const source_insert_fmt = "User |> insert(id = 1, name = \"{s}\")";
    var insert_buf: [long_len + 64]u8 = undefined;
    const insert_src = try std.fmt.bufPrint(insert_buf[0..], source_insert_fmt, .{long_text[0..]});

    const schema_tokens = tokenizer_mod.tokenize(schema_text);
    const schema_parsed = parser_mod.parse(&schema_tokens, schema_text);
    try @import("../catalog/schema_loader.zig").loadSchema(
        &catalog,
        &schema_parsed.ast,
        &schema_tokens,
        schema_text,
    );
    catalog.models[0].heap_first_page_id = 100;
    catalog.models[0].total_pages = 1;
    {
        const page = try pool.pin(100);
        @import("heap.zig").HeapPage.init(page);
        pool.unpin(100, true);
    }

    var tm = @import("../mvcc/transaction.zig").TxManager.init(std.testing.allocator);
    defer tm.deinit();
    var undo_log = try @import("../mvcc/undo.zig").UndoLog.init(std.testing.allocator, 1024, 64 * 1024);
    defer undo_log.deinit();
    const tx_id = try tm.begin();
    var snap = try tm.snapshot(tx_id);
    defer snap.deinit();

    const insert_tok = tokenizer_mod.tokenize(insert_src);
    const insert_parsed = parser_mod.parse(&insert_tok, insert_src);
    const insert_root = insert_parsed.ast.getNode(insert_parsed.ast.root);
    const insert_pipeline = insert_parsed.ast.getNode(insert_root.data.unary);
    const insert_op = insert_parsed.ast.getNode(insert_pipeline.data.binary.rhs);
    _ = try mutation_mod.executeInsert(
        &catalog,
        &pool,
        &wal,
        tx,
        0,
        &insert_parsed.ast,
        &insert_tok,
        insert_src,
        insert_op.data.unary,
    );

    try pool.flushAll();

    const delete_src = "User |> where(id == 1) |> delete";
    const delete_tok = tokenizer_mod.tokenize(delete_src);
    const delete_parsed = parser_mod.parse(&delete_tok, delete_src);
    const delete_root = delete_parsed.ast.getNode(delete_parsed.ast.root);
    const delete_pipeline = delete_parsed.ast.getNode(delete_root.data.unary);
    const delete_where = delete_parsed.ast.getNode(delete_pipeline.data.binary.rhs);
    _ = try mutation_mod.executeDelete(
        &catalog,
        &pool,
        &wal,
        &undo_log,
        tx_id,
        &snap,
        &tm,
        0,
        &delete_parsed.ast,
        &delete_tok,
        delete_src,
        delete_where.data.unary,
        std.testing.allocator,
    );
    try mutation_mod.commitOverflowReclaimEntriesForTx(
        &catalog,
        &pool,
        &wal,
        tx_id,
        1,
    );
    _ = try wal.commitTx(tx);
    try wal.forceFlush();

    var replay_wal = Wal.init(std.testing.allocator, disk.storage());
    defer replay_wal.deinit();
    try replay_wal.recover();

    var records_a: [128]Record = undefined;
    var payload_a: [64 * 1024]u8 = undefined;
    const first_run = try replayCommittedOverflowLifecycle(
        &pool,
        &replay_wal,
        &catalog.overflow_page_allocator,
        records_a[0..],
        payload_a[0..],
    );
    try pool.flushAll();

    var records_b: [128]Record = undefined;
    var payload_b: [64 * 1024]u8 = undefined;
    const second_run = try replayCommittedOverflowLifecycle(
        &pool,
        &replay_wal,
        &catalog.overflow_page_allocator,
        records_b[0..],
        payload_b[0..],
    );

    try std.testing.expectEqual(first_run.total_records, second_run.total_records);
    try std.testing.expectEqual(first_run.overflow_reclaim_records_seen, second_run.overflow_reclaim_records_seen);
    try std.testing.expectEqual(@as(usize, 1), first_run.overflow_reclaim_records_seen);
    try std.testing.expectEqual(
        @as(usize, 1),
        first_run.overflow_reclaim_applied + first_run.overflow_reclaim_idempotent_skips,
    );
    try std.testing.expectEqual(@as(usize, 0), second_run.overflow_reclaim_applied);
    try std.testing.expectEqual(@as(usize, 1), second_run.overflow_reclaim_idempotent_skips);

    var records_verify: [128]Record = undefined;
    var payload_verify: [64 * 1024]u8 = undefined;
    const decoded = try replay_wal.readFromInto(1, &records_verify, &payload_verify);
    var first_overflow_root: u64 = 0;
    for (records_verify[0..decoded.records_len]) |rec| {
        if (rec.record_type != .overflow_chain_create) continue;
        const meta = try decodeOverflowChainRecordMeta(rec.payload);
        first_overflow_root = meta.first_page_id;
        break;
    }
    try std.testing.expect(first_overflow_root != 0);

    const root_page = try pool.pin(first_overflow_root);
    defer pool.unpin(first_overflow_root, false);
    try std.testing.expectEqual(PageType.free, root_page.header.page_type);
}

test "replayCommittedOverflowLifecycle skips aborted tx after overflow create" {
    const disk_mod = @import("../simulator/disk.zig");

    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    const overflow_root: u64 = 21_000;
    var allocator = try OverflowPageIdAllocator.initWithBounds(overflow_root, 8);

    {
        const page = try pool.pin(overflow_root);
        overflow_mod.OverflowPage.init(page);
        try overflow_mod.OverflowPage.writeChunk(page, "live", overflow_mod.OverflowPage.null_page_id);
        pool.unpin(overflow_root, true);
    }

    const tx_id: u64 = 91;
    _ = try wal.beginTx(tx_id);
    var create_payload: [16]u8 = undefined;
    encodeOverflowChainRecordMetaForTest(
        create_payload[0..],
        .{
            .first_page_id = overflow_root,
            .page_count = 1,
            .payload_bytes = 4,
        },
    );
    _ = try wal.append(tx_id, .overflow_chain_create, overflow_root, create_payload[0..]);
    _ = try wal.abortTx(tx_id);
    try wal.flush();

    var records: [64]Record = undefined;
    var payload: [16 * 1024]u8 = undefined;
    const stats = try replayCommittedOverflowLifecycle(
        &pool,
        &wal,
        &allocator,
        records[0..],
        payload[0..],
    );
    try std.testing.expectEqual(@as(usize, 0), stats.overflow_reclaim_records_seen);
    try std.testing.expectEqual(@as(usize, 0), stats.overflow_reclaim_applied);

    const root_page = try pool.pin(overflow_root);
    defer pool.unpin(overflow_root, false);
    try std.testing.expectEqual(PageType.overflow, root_page.header.page_type);
}

test "replayCommittedOverflowLifecycle skips aborted tx after overflow relink intent" {
    const disk_mod = @import("../simulator/disk.zig");

    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    const old_root: u64 = 22_000;
    const new_root: u64 = 22_001;
    var allocator = try OverflowPageIdAllocator.initWithBounds(old_root, 8);

    {
        const page = try pool.pin(old_root);
        overflow_mod.OverflowPage.init(page);
        try overflow_mod.OverflowPage.writeChunk(page, "old", overflow_mod.OverflowPage.null_page_id);
        pool.unpin(old_root, true);
    }
    {
        const page = try pool.pin(new_root);
        overflow_mod.OverflowPage.init(page);
        try overflow_mod.OverflowPage.writeChunk(page, "new", overflow_mod.OverflowPage.null_page_id);
        pool.unpin(new_root, true);
    }

    const tx_id: u64 = 92;
    _ = try wal.beginTx(tx_id);
    var create_payload: [16]u8 = undefined;
    encodeOverflowChainRecordMetaForTest(
        create_payload[0..],
        .{
            .first_page_id = new_root,
            .page_count = 1,
            .payload_bytes = 3,
        },
    );
    _ = try wal.append(tx_id, .overflow_chain_create, new_root, create_payload[0..]);
    var relink_payload: [16]u8 = undefined;
    encodeOverflowRelinkRecordMetaForTest(relink_payload[0..], old_root, new_root);
    _ = try wal.append(tx_id, .overflow_chain_relink, 100, relink_payload[0..]);
    _ = try wal.abortTx(tx_id);
    try wal.flush();

    var records: [64]Record = undefined;
    var payload: [16 * 1024]u8 = undefined;
    const stats = try replayCommittedOverflowLifecycle(
        &pool,
        &wal,
        &allocator,
        records[0..],
        payload[0..],
    );
    try std.testing.expectEqual(@as(usize, 0), stats.overflow_reclaim_records_seen);
    try std.testing.expectEqual(@as(usize, 0), stats.overflow_reclaim_applied);

    const old_page = try pool.pin(old_root);
    defer pool.unpin(old_root, false);
    try std.testing.expectEqual(PageType.overflow, old_page.header.page_type);

    const new_page = try pool.pin(new_root);
    defer pool.unpin(new_root, false);
    try std.testing.expectEqual(PageType.overflow, new_page.header.page_type);
}

test "replayCommittedOverflowLifecycle skips aborted tx after overflow unlink enqueue and reclaim record" {
    const disk_mod = @import("../simulator/disk.zig");

    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    const overflow_root: u64 = 23_000;
    var allocator = try OverflowPageIdAllocator.initWithBounds(overflow_root, 8);

    {
        const page = try pool.pin(overflow_root);
        overflow_mod.OverflowPage.init(page);
        try overflow_mod.OverflowPage.writeChunk(page, "livechain", overflow_mod.OverflowPage.null_page_id);
        pool.unpin(overflow_root, true);
    }

    const tx_id: u64 = 93;
    _ = try wal.beginTx(tx_id);
    var unlink_payload: [16]u8 = undefined;
    encodeOverflowChainRecordMetaForTest(
        unlink_payload[0..],
        .{
            .first_page_id = overflow_root,
            .page_count = 0,
            .payload_bytes = 0,
        },
    );
    _ = try wal.append(tx_id, .overflow_chain_unlink, overflow_root, unlink_payload[0..]);

    var reclaim_payload: [16]u8 = undefined;
    encodeOverflowChainRecordMetaForTest(
        reclaim_payload[0..],
        .{
            .first_page_id = overflow_root,
            .page_count = 1,
            .payload_bytes = 0,
        },
    );
    _ = try wal.append(tx_id, .overflow_chain_reclaim, overflow_root, reclaim_payload[0..]);
    _ = try wal.abortTx(tx_id);
    try wal.flush();

    var records: [64]Record = undefined;
    var payload: [16 * 1024]u8 = undefined;
    const stats = try replayCommittedOverflowLifecycle(
        &pool,
        &wal,
        &allocator,
        records[0..],
        payload[0..],
    );
    try std.testing.expectEqual(@as(usize, 0), stats.overflow_reclaim_records_seen);
    try std.testing.expectEqual(@as(usize, 0), stats.overflow_reclaim_applied);
    try std.testing.expectEqual(@as(usize, 0), stats.overflow_reclaim_idempotent_skips);

    const root_page = try pool.pin(overflow_root);
    defer pool.unpin(overflow_root, false);
    try std.testing.expectEqual(PageType.overflow, root_page.header.page_type);
}

test "replayCommittedOverflowLifecycle fails closed when tx markers are missing for mutation records" {
    const disk_mod = @import("../simulator/disk.zig");

    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 8);
    defer pool.deinit();
    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    const overflow_root: u64 = 24_000;
    var allocator = try OverflowPageIdAllocator.initWithBounds(overflow_root, 8);

    var reclaim_payload: [16]u8 = undefined;
    encodeOverflowChainRecordMetaForTest(
        reclaim_payload[0..],
        .{
            .first_page_id = overflow_root,
            .page_count = 1,
            .payload_bytes = 4,
        },
    );
    _ = try wal.append(101, .overflow_chain_reclaim, overflow_root, reclaim_payload[0..]);
    try wal.flush();

    var records: [64]Record = undefined;
    var payload: [16 * 1024]u8 = undefined;
    try std.testing.expectError(
        error.Corruption,
        replayCommittedOverflowLifecycle(
            &pool,
            &wal,
            &allocator,
            records[0..],
            payload[0..],
        ),
    );
}
