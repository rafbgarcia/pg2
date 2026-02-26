//! Feature coverage for user-visible planner inspect contracts.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature inspect exposes planner metadata and deterministic checkpoint chronology" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(active, bool, notNull)
        \\}
    );

    _ = try executor.run("User |> insert(id = 1, active = true) {}");
    _ = try executor.run("User |> insert(id = 2, active = false) {}");
    _ = try executor.run("User |> insert(id = 3, active = true) {}");

    const result = try executor.run(
        "User |> where(active == true) |> sort(id desc) |> inspect { id }",
    );
    try std.testing.expect(std.mem.indexOf(u8, result, "INSPECT plan ") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "planner_policy_version=2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "planner_snapshot_fingerprint=") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "planner_decision_fingerprint=") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "streaming_mode=disabled") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "parallel_mode=sequential") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "parallel_reason=PARALLEL_DISABLED_FEATURE_GATE") != null);

    const pre_scan = std.mem.indexOf(u8, result, "INSPECT checkpoint name=pre_scan ");
    const post_filter = std.mem.indexOf(u8, result, "INSPECT checkpoint name=post_filter ");
    const post_group = std.mem.indexOf(u8, result, "INSPECT checkpoint name=post_group ");
    const pre_join = std.mem.indexOf(u8, result, "INSPECT checkpoint name=pre_join ");
    try std.testing.expect(pre_scan != null);
    try std.testing.expect(post_filter != null);
    try std.testing.expect(post_group != null);
    try std.testing.expect(pre_join != null);
    try std.testing.expect(pre_scan.? < post_filter.?);
    try std.testing.expect(post_filter.? < post_group.?);
    try std.testing.expect(post_group.? < pre_join.?);
}
