//! Feature coverage for i32 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature i32 fields preserve 32-bit signed values" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\TelemetryCounter {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(delta, i32, notNull)
        \\}
    );

    _ = try executor.run("TelemetryCounter |> insert(id = 1, delta = 0) {}");
    _ = try executor.run("TelemetryCounter |> insert(id = 2, delta = 2147483647) {}");

    const result = try executor.run(
        "TelemetryCounter |> sort(delta asc) { id delta }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,0\n2,2147483647\n",
        result,
    );
}
