//! Feature coverage for i8 field behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature i8 fields preserve 8-bit signed values across insert and update" {
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

    _ = try executor.run("DeviceState |> insert(id = 1, level = -128) {}");
    _ = try executor.run("DeviceState |> insert(id = 2, level = 127) {}");

    var result = try executor.run("DeviceState |> sort(id asc) { id level }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,-128\n2,127\n",
        result,
    );

    result = try executor.run("DeviceState |> where(id == 1) |> update(level = 42) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("DeviceState |> where(id == 1) { id level }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,42\n",
        result,
    );
}

test "feature i8 fields fail closed when insert value is out of range" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\DeviceStateValidation {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(level, i8, notNull)
        \\}
    );

    const result = try executor.run(
        "DeviceStateValidation |> insert(id = 1, level = -129) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"value is out of range (-128 to 127)\" phase=mutation code=IntegerOutOfRange path=insert.level line=1 col=49\n",
        result,
    );
}
