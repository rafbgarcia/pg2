//! Feature coverage for i16 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature i16 fields preserve 16-bit signed values across insert and update" {
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

    _ = try executor.run("SensorSample |> insert(id = 1, delta = -32768) {}");
    _ = try executor.run("SensorSample |> insert(id = 2, delta = 32767) {}");

    var result = try executor.run("SensorSample |> sort(id asc) { id delta }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,-32768\n2,32767\n",
        result,
    );

    result = try executor.run("SensorSample |> where(id = 1) |> update(delta = 1234) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("SensorSample |> where(id = 1) { id delta }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,1234\n",
        result,
    );
}

test "feature i16 fields fail closed when insert value is out of range" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\SensorSampleValidation {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(delta, i16, notNull)
        \\}
    );

    const result = try executor.run(
        "SensorSampleValidation |> insert(id = 1, delta = -32769) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: phase=mutation code=IntegerOutOfRange path=insert.delta line=1 col=50 message=\"value is out of range (-32768 to 32767)\"\n",
        result,
    );
}
