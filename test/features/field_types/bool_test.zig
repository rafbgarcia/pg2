//! Feature coverage for bool field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature bool fields support true/false lifecycle" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\FeatureFlag {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(enabled, bool, notNull)
        \\}
    );

    _ = try executor.run("FeatureFlag |> insert(id = 1, enabled = true) {}");

    var result = try executor.run("FeatureFlag |> where(id == 1) { id enabled }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n",
        result,
    );

    result = try executor.run(
        "FeatureFlag |> where(id == 1) |> update(enabled = false) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("FeatureFlag |> where(id == 1) { id enabled }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,false\n",
        result,
    );
}
