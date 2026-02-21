//! Feature coverage for f64 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature f64 fields preserve decimal values across insert and update" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Metric {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, f64, notNull)
        \\}
    );

    _ = try executor.run("Metric |> insert(id = 1, value = 3.14159) {}");

    var result = try executor.run("Metric |> where(id == 1) { id value }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,3.14159\n",
        result,
    );

    result = try executor.run("Metric |> where(id == 1) |> update(value = -0.125) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("Metric |> where(id == 1) { id value }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,-0.125\n",
        result,
    );
}
