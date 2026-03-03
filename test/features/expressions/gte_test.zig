//! Feature coverage for greater-than-or-equal operator behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature greater-than-or-equal supports keyword field names" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\KeywordFields {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(offset, i64, notNull)
        \\}
    );

    _ = try executor.run("KeywordFields |> insert(id = 1, offset = 20) {}");
    _ = try executor.run("KeywordFields |> insert(id = 2, offset = 10) {}");

    const result = try executor.run(
        "KeywordFields |> where(offset >= 10) |> sort(offset asc) { id offset }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n2,10\n1,20\n",
        result,
    );
}
