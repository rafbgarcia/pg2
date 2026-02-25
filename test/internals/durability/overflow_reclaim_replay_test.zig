//! Internal durability checks for overflow reclaim WAL replay semantics.
//!
//! Responsibilities in this file:
//! - Verifies replay reclaims committed overflow chains after crash.
//! - Verifies replay is idempotent on already-applied reclaim records.
const std = @import("std");
const pg2 = @import("pg2");
const buffer_pool_mod = pg2.storage.buffer_pool;
const overflow_mod = pg2.storage.overflow;
const page_mod = pg2.storage.page;
const recovery_mod = pg2.storage.recovery;
const wal_mod = pg2.storage.wal;
const internal = @import("../../features/test_env_test.zig");

test "internal overflow reclaim WAL replay restores page state after crash and is idempotent" {
    var env: internal.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(14_000, 12);

    var long_name: [1200]u8 = undefined;
    @memset(long_name[0..], 'z');
    var insert_req_buf: [1500]u8 = undefined;
    const insert_req = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "User |> insert(id = 1, name = \"{s}\") {{}}",
        .{long_name[0..]},
    );
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    // Persist overflow chain pages before delete so replay must reclaim them.
    try env.runtime.pool.flushAll();

    result = try executor.run("User |> where(id == 1) |> delete {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=1\n",
        result,
    );
    try env.runtime.wal.flush();

    env.disk.crash();

    var recovered_pool = try buffer_pool_mod.BufferPool.init(
        std.testing.allocator,
        env.disk.storage(),
        16,
    );
    defer recovered_pool.deinit();

    var recovered_wal = wal_mod.Wal.init(std.testing.allocator, env.disk.storage());
    defer recovered_wal.deinit();
    try recovered_wal.recover();

    var wal_records: [128]wal_mod.Record = undefined;
    var wal_payloads: [64 * 1024]u8 = undefined;
    const decoded = try recovered_wal.readFromInto(1, &wal_records, &wal_payloads);
    var overflow_root: u64 = 0;
    for (wal_records[0..decoded.records_len]) |rec| {
        if (rec.record_type != .overflow_chain_create) continue;
        overflow_root = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, rec.payload[0..8]).*);
        break;
    }
    try std.testing.expect(overflow_root != 0);

    {
        const page = try recovered_pool.pin(overflow_root);
        defer recovered_pool.unpin(overflow_root, false);
        try std.testing.expectEqual(page_mod.PageType.overflow, page.header.page_type);
    }

    var replay_records_a: [128]wal_mod.Record = undefined;
    var replay_payload_a: [64 * 1024]u8 = undefined;
    const first_replay = try recovery_mod.replayCommittedOverflowLifecycle(
        &recovered_pool,
        &recovered_wal,
        &env.catalog.overflow_page_allocator,
        replay_records_a[0..],
        replay_payload_a[0..],
    );
    try std.testing.expectEqual(
        @as(usize, 1),
        first_replay.overflow_reclaim_applied + first_replay.overflow_reclaim_idempotent_skips,
    );
    try recovered_pool.flushAll();

    {
        const page = try recovered_pool.pin(overflow_root);
        defer recovered_pool.unpin(overflow_root, false);
        try std.testing.expectEqual(page_mod.PageType.free, page.header.page_type);
    }

    var replay_records_b: [128]wal_mod.Record = undefined;
    var replay_payload_b: [64 * 1024]u8 = undefined;
    const second_replay = try recovery_mod.replayCommittedOverflowLifecycle(
        &recovered_pool,
        &recovered_wal,
        &env.catalog.overflow_page_allocator,
        replay_records_b[0..],
        replay_payload_b[0..],
    );
    try std.testing.expectEqual(first_replay.total_records, second_replay.total_records);
    try std.testing.expectEqual(
        first_replay.overflow_reclaim_records_seen,
        second_replay.overflow_reclaim_records_seen,
    );
    try std.testing.expectEqual(@as(usize, 0), second_replay.overflow_reclaim_applied);
    try std.testing.expectEqual(second_replay.overflow_reclaim_records_seen, second_replay.overflow_reclaim_idempotent_skips);
}
