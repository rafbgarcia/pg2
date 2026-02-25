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
