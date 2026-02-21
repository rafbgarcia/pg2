//! Feature coverage for i8 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature i8 fields preserve 8-bit signed values" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\DeviceState {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(level, i8, notNull)
        \\}
    );

    _ = try executor.run("DeviceState |> insert(id = 1, level = 0) {}");
    _ = try executor.run("DeviceState |> insert(id = 2, level = 127) {}");

    const result = try executor.run("DeviceState |> sort(level asc) { id level }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,0\n2,127\n",
        result,
    );
}
