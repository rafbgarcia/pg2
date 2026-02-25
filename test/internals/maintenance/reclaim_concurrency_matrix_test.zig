//! Mixed reclaim workload coverage under pinned snapshots and interleaved reads.
const std = @import("std");
const pg2 = @import("pg2");
const internal = @import("../../features/test_env_test.zig");

const mutation_mod = pg2.executor.mutation;

fn runDispatch(executor: *internal.TestExecutor, request: []const u8) ![]const u8 {
    const len = try executor.session.dispatchRequest(
        &executor.pool,
        request,
        executor.response_buf[0..],
    );
    return executor.response_buf[0..len];
}

test "internal mixed reclaim matrix blocks under pinned snapshot and drains after release" {
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

    var req_buf: [128]u8 = undefined;
    var result: []const u8 = undefined;

    var id: i64 = 1;
    while (id <= 8) : (id += 1) {
        const insert_seed = try std.fmt.bufPrint(
            req_buf[0..],
            "User |> insert(id = {d}, name = \"seed{d}\") {{}}",
            .{ id, id },
        );
        result = try runDispatch(executor, insert_seed);
        try std.testing.expectEqualStrings(
            "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
            result,
        );
    }

    const pinned_tx = try env.runtime.tx_manager.begin();
    defer {
        if (env.runtime.tx_manager.getState(pinned_tx) == .active) {
            env.runtime.tx_manager.commit(pinned_tx) catch {};
        }
    }

    const rounds: i64 = 24;
    var i: i64 = 0;
    while (i < rounds) : (i += 1) {
        const key = @mod(i, 8) + 1;
        const reader_key = @mod(i + 3, 8) + 1;

        const delete_req = try std.fmt.bufPrint(
            req_buf[0..],
            "User |> where(id == {d}) |> delete {{}}",
            .{key},
        );
        result = try runDispatch(executor, delete_req);
        try std.testing.expectEqualStrings(
            "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=1\n",
            result,
        );

        const read_req = try std.fmt.bufPrint(
            req_buf[0..],
            "User |> where(id == {d}) {{ id name }}",
            .{reader_key},
        );
        result = try runDispatch(executor, read_req);
        try std.testing.expect(std.mem.startsWith(
            u8,
            result,
            "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        ));

        const insert_req = try std.fmt.bufPrint(
            req_buf[0..],
            "User |> insert(id = {d}, name = \"loop{d}\") {{}}",
            .{ key, i },
        );
        result = try runDispatch(executor, insert_req);
        try std.testing.expectEqualStrings(
            "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
            result,
        );
    }

    result = try runDispatch(executor, "User |> inspect {}");
    try std.testing.expect(std.mem.indexOf(u8, result, "INSPECT heap_reclaim queue_depth=24 ") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "INSPECT index_reclaim queue_depth=24 ") != null);

    try env.runtime.tx_manager.commit(pinned_tx);
    const oldest_active = env.runtime.tx_manager.getOldestActive();
    env.runtime.undo_log.truncate(oldest_active);
    env.runtime.tx_manager.cleanupBefore(oldest_active);

    var made_progress = false;
    var attempts: usize = 0;
    while (attempts < 64) : (attempts += 1) {
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
        env.runtime.undo_log.truncate(env.runtime.tx_manager.getOldestActive());
        env.runtime.tx_manager.cleanupBefore(env.runtime.tx_manager.getOldestActive());

        result = try runDispatch(executor, "User |> inspect {}");
        const heap_progress = std.mem.indexOf(u8, result, "INSPECT heap_reclaim queue_depth=24 ") == null;
        const index_progress = std.mem.indexOf(u8, result, "INSPECT index_reclaim queue_depth=24 ") == null;
        if (heap_progress and index_progress) {
            made_progress = true;
            break;
        }
    }
    try std.testing.expect(made_progress);

    id = 1;
    while (id <= 8) : (id += 1) {
        const verify_req = try std.fmt.bufPrint(
            req_buf[0..],
            "User |> where(id == {d}) {{ id name }}",
            .{id},
        );
        result = try runDispatch(executor, verify_req);
        try std.testing.expect(std.mem.startsWith(
            u8,
            result,
            "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        ));
    }
}
