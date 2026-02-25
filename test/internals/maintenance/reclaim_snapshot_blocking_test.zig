//! Snapshot-pinning reclaim behavior checks.
const std = @import("std");
const pg2 = @import("pg2");
const internal = @import("../../harness/internal_env.zig");

test "internal long-lived snapshot blocks slot reclaim until snapshot closes" {
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

    var result = try executor.run("User |> insert(id = 1, name = \"a\") {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    const pinned_tx = try env.runtime.tx_manager.begin();
    const delete_tx_id = env.runtime.tx_manager.getNextTxId();

    result = try executor.run("User |> where(id == 1) |> delete {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=1\n",
        result,
    );

    // While snapshot is pinned, reclaim must remain queued.
    result = try executor.run("User |> insert(id = 2, name = \"b\") {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );
    result = try executor.run("User |> inspect {}");
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "INSPECT heap_reclaim queue_depth=1 pinned_by_snapshot=0 reclaim_enqueued_total=1 reclaim_dequeued_total=0 reclaimed_slots_total=0 reclaim_failures_total=0\n",
        ) != null,
    );

    try env.runtime.tx_manager.commit(pinned_tx);
    var tx_id: u64 = 1;
    while (tx_id < env.runtime.tx_manager.getNextTxId()) : (tx_id += 1) {
        if (env.runtime.tx_manager.getState(tx_id) == .active) {
            try env.runtime.tx_manager.commit(tx_id);
        }
    }
    env.runtime.undo_log.truncate(env.runtime.tx_manager.getOldestActive());
    env.runtime.tx_manager.cleanupBefore(env.runtime.tx_manager.getOldestActive());

    // With snapshot closed, explicit reclaim maintenance now drains.
    try pg2.executor.mutation.commitSlotReclaimEntriesForTx(
        &env.catalog,
        &env.runtime.pool,
        &env.runtime.wal,
        delete_tx_id,
        std.math.maxInt(u64),
        16,
    );
    result = try executor.run("User |> inspect {}");
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "INSPECT heap_reclaim queue_depth=0 pinned_by_snapshot=0 reclaim_enqueued_total=1 reclaim_dequeued_total=1 reclaimed_slots_total=1 reclaim_failures_total=0\n",
        ) != null,
    );
}
