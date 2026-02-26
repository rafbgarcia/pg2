//! Crash/restart correctness checks for index reclaim WAL paths.
const std = @import("std");
const pg2 = @import("pg2");
const internal = @import("../../harness/internal_env.zig");

const buffer_pool_mod = pg2.storage.buffer_pool;
const recovery_mod = pg2.storage.recovery;
const wal_mod = pg2.storage.wal;
const mutation_mod = pg2.executor.mutation;
const index_maintenance_mod = pg2.executor.index_maintenance;
const Value = pg2.storage.row.Value;

fn runDispatch(executor: *internal.TestExecutor, request: []const u8) ![]const u8 {
    const len = try executor.session.dispatchRequest(
        &executor.pool,
        request,
        executor.response_buf[0..],
    );
    return executor.response_buf[0..len];
}

test "internal replay matrix asserts post-restart index delete correctness" {
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

    var result = try runDispatch(executor, "User |> insert(id = 1, name = \"a\") {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    const pinned_tx = try env.runtime.tx_manager.begin();

    result = try runDispatch(executor, "User |> where(id == 1) |> delete {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=1\n",
        result,
    );

    try env.runtime.tx_manager.commit(pinned_tx);
    const oldest_active = env.runtime.tx_manager.getOldestActive();
    env.runtime.undo_log.truncate(oldest_active);
    env.runtime.tx_manager.cleanupBefore(oldest_active);

    const maintenance_tx = try env.runtime.tx_manager.begin();
    _ = try env.runtime.wal.beginTx(maintenance_tx);
    try mutation_mod.commitSlotReclaimEntriesForTx(
        &env.catalog,
        &env.runtime.pool,
        &env.runtime.wal,
        maintenance_tx,
        std.math.maxInt(u64),
        std.math.maxInt(usize),
    );
    _ = try env.runtime.wal.commitTx(maintenance_tx);
    try env.runtime.wal.forceFlush();
    try env.runtime.tx_manager.commit(maintenance_tx);

    // Persist post-delete page state so crash restart observes reclaimed index state.
    try env.runtime.wal.forceFlush();
    try env.runtime.pool.flushAll();

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

    var replay_records: [256]wal_mod.Record = undefined;
    var replay_payload: [128 * 1024]u8 = undefined;
    const replay_stats = try recovery_mod.replayCommittedOverflowLifecycle(
        &recovered_pool,
        &recovered_wal,
        &env.catalog.overflow_page_allocator,
        replay_records[0..],
        replay_payload[0..],
    );
    try std.testing.expect(replay_stats.index_reclaim_delete_records_seen >= 1);

    var pk_btree = index_maintenance_mod.openPrimaryKeyIndex(
        &env.catalog,
        &recovered_pool,
        &recovered_wal,
        0,
    );
    try std.testing.expect(pk_btree != null);

    const exists = try index_maintenance_mod.primaryKeyExists(
        &pk_btree.?,
        Value{ .i64 = 1 },
    );
    try std.testing.expect(!exists);
}
