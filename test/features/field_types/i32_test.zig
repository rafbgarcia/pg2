//! Feature coverage for i32 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature i32 fields preserve 32-bit signed values across insert and update" {
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

    _ = try executor.run("TelemetryCounter |> insert(id = 1, delta = -2147483648) {}");
    _ = try executor.run("TelemetryCounter |> insert(id = 2, delta = 2147483647) {}");

    var result = try executor.run("TelemetryCounter |> sort(id asc) { id delta }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,-2147483648\n2,2147483647\n",
        result,
    );

    result = try executor.run("TelemetryCounter |> where(id = 1) |> update(delta = 100) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("TelemetryCounter |> where(id = 1) { id delta }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,100\n",
        result,
    );
}

test "feature i32 fields fail closed when insert value is out of range" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\TelemetryCounterValidation {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(delta, i32, notNull)
        \\}
    );

    const result = try executor.run(
        "TelemetryCounterValidation |> insert(id = 1, delta = 2147483648) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: phase=mutation code=IntegerOutOfRange path=insert.delta line=1 col=54 message=\"value is out of range (-2147483648 to 2147483647)\"\n",
        result,
    );
}
