//! Internal checks for reclaim-time index cleanup wiring.
const std = @import("std");
const internal = @import("../../features/test_env_test.zig");

test "internal delete enqueues index reclaim metadata and reports inspect counters" {
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

    result = try executor.run("User |> where(id == 1) |> delete {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=1\n",
        result,
    );

    // A follow-up write commit boundary drains reclaim queues.
    result = try executor.run("User |> insert(id = 2, name = \"x\") {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );
    result = try executor.run("User |> insert(id = 3, name = \"y\") {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> inspect {}");
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            result,
            "INSPECT index_reclaim queue_depth=1 pinned_by_snapshot=0 reclaim_enqueued_total=1 reclaim_dequeued_total=0 reclaimed_entries_total=0 reclaim_failures_total=0\n",
        ) != null,
    );

    // Reinserting the same key must succeed without stale index blockers.
    result = try executor.run("User |> insert(id = 1, name = \"b\") {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "internal uniqueness path opportunistically cleans stale index keys under pinned reclaim backlog" {
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

    const pinned_tx = try env.runtime.tx_manager.begin();
    defer {
        if (env.runtime.tx_manager.getState(pinned_tx) == .active) {
            env.runtime.tx_manager.commit(pinned_tx) catch {};
        }
    }

    var result = try executor.run("User |> insert(id = 1, name = \"seed\") {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );
    result = try executor.run("User |> where(id == 1) |> delete {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=1\n",
        result,
    );

    // Keep a small hot-key working set and churn many rounds while reclaim is
    // pinned. Inserts must keep succeeding, proving stale key blockers are
    // opportunistically cleaned in the write path.
    const key_space: i64 = 4;
    const rounds: i64 = 40;
    var i: i64 = 0;
    var req_buf: [128]u8 = undefined;
    while (i < rounds) : (i += 1) {
        const id = @mod(i, key_space) + 1;
        const insert_req = try std.fmt.bufPrint(
            req_buf[0..],
            "User |> insert(id = {d}, name = \"n{d}\") {{}}",
            .{ id, i },
        );
        result = try executor.run(insert_req);
        try std.testing.expectEqualStrings(
            "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
            result,
        );

        const delete_req = try std.fmt.bufPrint(
            req_buf[0..],
            "User |> where(id == {d}) |> delete {{}}",
            .{id},
        );
        result = try executor.run(delete_req);
        try std.testing.expectEqualStrings(
            "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=1\n",
            result,
        );
    }

    result = try executor.run("User |> inspect {}");
    try std.testing.expect(std.mem.indexOf(u8, result, "INSPECT index_reclaim queue_depth=41 ") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "reclaim_enqueued_total=41 ") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "reclaim_dequeued_total=0 ") != null);

    var key: i64 = 1;
    while (key <= key_space) : (key += 1) {
        const insert_req = try std.fmt.bufPrint(
            req_buf[0..],
            "User |> insert(id = {d}, name = \"final\") {{}}",
            .{key},
        );
        result = try executor.run(insert_req);
        try std.testing.expectEqualStrings(
            "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
            result,
        );
    }
}
