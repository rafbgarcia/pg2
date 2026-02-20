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
};

pub const RecoveryError = error{
    Corruption,
} || buffer_pool_mod.BufferPoolError || wal_mod.WalError;

const OverflowChainRecordMeta = struct {
    first_page_id: u64,
    page_count: u32,
    payload_bytes: u32,
};

/// Replays committed overflow lifecycle WAL records into page state.
///
/// Caller supplies bounded record/payload buffers to keep recovery deterministic
/// and allocation-free in core paths.
pub fn replayCommittedOverflowLifecycle(
    pool: *BufferPool,
    wal: *Wal,
    overflow_allocator: *const OverflowPageIdAllocator,
    records_buf: []Record,
    payload_buf: []u8,
) RecoveryError!ReplayStats {
    const decoded = try wal.readFromInto(1, records_buf, payload_buf);
    const records = records_buf[0..decoded.records_len];
    var stats: ReplayStats = .{
        .total_records = records.len,
    };

    for (records) |rec| {
        if (!isTxReplayable(records, rec.tx_id)) continue;
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
            else => {},
        }
    }

    return stats;
}

fn isTxReplayable(
    records: []const Record,
    tx_id: u64,
) bool {
    var saw_tx_state = false;
    var committed = false;
    var saw_mutation = false;
    for (records) |rec| {
        if (rec.tx_id != tx_id) continue;
        switch (rec.record_type) {
            .tx_commit => {
                saw_tx_state = true;
                committed = true;
            },
            .tx_abort => {
                saw_tx_state = true;
                committed = false;
            },
            .tx_begin => {},
            else => saw_mutation = true,
        }
    }
    if (saw_tx_state) return committed;
    return saw_mutation;
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
    const long_len = 1200;
    var long_text: [long_len]u8 = undefined;
    @memset(long_text[0..], 'r');
    const schema_text =
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
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

    const delete_src = "User |> where(id = 1) |> delete";
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
    try wal.flush();

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
