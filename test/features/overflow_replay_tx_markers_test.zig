//! E2E coverage for strict tx-marker policy in overflow lifecycle replay.
//!
//! Responsibilities in this file:
//! - Exercises server session path writes that emit overflow lifecycle WAL.
//! - Verifies recovery replay fails closed on legacy overflow WAL without tx markers.
const std = @import("std");
const pg2 = @import("pg2");
const buffer_pool_mod = pg2.storage.buffer_pool;
const overflow_mod = pg2.storage.overflow;
const recovery_mod = pg2.storage.recovery;
const wal_mod = pg2.storage.wal;
const e2e = @import("test_env_test.zig");

const Record = wal_mod.Record;

const OverflowChainRecordMeta = struct {
    first_page_id: u64,
    page_count: u32,
    payload_bytes: u32,
};

fn decodeOverflowChainRecordMeta(payload: []const u8) !OverflowChainRecordMeta {
    if (payload.len != 16) return error.Corruption;
    return .{
        .first_page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, payload[0..8]).*),
        .page_count = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, payload[8..12]).*),
        .payload_bytes = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, payload[12..16]).*),
    };
}

fn encodeOverflowChainRecordMeta(out: []u8, meta: OverflowChainRecordMeta) void {
    std.debug.assert(out.len >= 16);
    @memcpy(out[0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, meta.first_page_id)));
    @memcpy(out[8..12], std.mem.asBytes(&std.mem.nativeToLittle(u32, meta.page_count)));
    @memcpy(out[12..16], std.mem.asBytes(&std.mem.nativeToLittle(u32, meta.payload_bytes)));
}

test "e2e overflow replay fails closed for lifecycle record without tx markers" {
    var env: e2e.E2EEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(14_000, 8);

    var long_name: [1200]u8 = undefined;
    @memset(long_name[0..], 'z');
    var insert_req_buf: [1500]u8 = undefined;
    const insert_req = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "User |> insert(id = 1, name = \"{s}\") {{}}",
        .{long_name[0..]},
    );
    const result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );
    try env.runtime.pool.flushAll();

    var wal_records: [128]Record = undefined;
    var wal_payloads: [64 * 1024]u8 = undefined;
    const decoded = try env.runtime.wal.readFromInto(1, &wal_records, &wal_payloads);

    var overflow_root: u64 = 0;
    for (wal_records[0..decoded.records_len]) |rec| {
        if (rec.record_type != .overflow_chain_create) continue;
        const meta = try decodeOverflowChainRecordMeta(rec.payload);
        overflow_root = meta.first_page_id;
        break;
    }
    try std.testing.expect(overflow_root != 0);

    var legacy_payload: [16]u8 = undefined;
    encodeOverflowChainRecordMeta(
        legacy_payload[0..],
        .{
            .first_page_id = overflow_root,
            .page_count = 1,
            .payload_bytes = 1200,
        },
    );
    _ = try env.runtime.wal.append(9_999, .overflow_chain_reclaim, overflow_root, legacy_payload[0..]);
    try env.runtime.wal.flush();

    var recovered_pool = try buffer_pool_mod.BufferPool.init(
        std.testing.allocator,
        env.disk.storage(),
        8,
    );
    defer recovered_pool.deinit();

    var recovered_wal = wal_mod.Wal.init(std.testing.allocator, env.disk.storage());
    defer recovered_wal.deinit();
    try recovered_wal.recover();

    var replay_records: [128]Record = undefined;
    var replay_payload: [64 * 1024]u8 = undefined;
    try std.testing.expectError(
        error.Corruption,
        recovery_mod.replayCommittedOverflowLifecycle(
            &recovered_pool,
            &recovered_wal,
            &env.catalog.overflow_page_allocator,
            replay_records[0..],
            replay_payload[0..],
        ),
    );
}
