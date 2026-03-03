//! Feature coverage for less-than operator behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature less-than supports timestamp keyword comparisons" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CurrentTsSemantics {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    _ = try executor.run("CurrentTsSemantics |> insert(id = 1) {}");
    _ = try executor.run("CurrentTsSemantics |> insert(id = 2) {}");
    _ = try executor.run("CurrentTsSemantics |> insert(id = 3) {}");

    const result = try executor.run(
        "CurrentTsSemantics |> where(CurrentTimestamp < CurrentTimestamp) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );
}
