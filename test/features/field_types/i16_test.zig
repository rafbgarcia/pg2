//! Feature coverage for i16 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature i16 fields preserve 16-bit signed values" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\SensorSample {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(delta, i16, notNull)
        \\}
    );

    _ = try executor.run("SensorSample |> insert(id = 1, delta = 0) {}");
    _ = try executor.run("SensorSample |> insert(id = 2, delta = 32767) {}");

    const result = try executor.run("SensorSample |> sort(delta asc) { id delta }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,0\n2,32767\n",
        result,
    );
}
