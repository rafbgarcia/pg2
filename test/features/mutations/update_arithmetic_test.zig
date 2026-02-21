//! Feature coverage for arithmetic update behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature update supports integer arithmetic on u16 fields" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Counter {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, u16, notNull)
        \\}
    );

    _ = try executor.run("Counter |> insert(id = 1, value = 41) {}");

    var result = try executor.run(
        "Counter |> where(id = 1) |> update(value = value + 1) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("Counter |> where(id = 1) { id value }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,42\n",
        result,
    );
}
