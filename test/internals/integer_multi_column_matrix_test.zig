//! Internal coverage for multi-column integer assignment in a single mutation.
const std = @import("std");
const feature = @import("../features/test_env_test.zig");

test "internal integer matrix insert accepts mixed boundary values in one statement" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\IntegerMatrix {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v_i8, i8, notNull)
        \\  field(v_i16, i16, notNull)
        \\  field(v_i32, i32, notNull)
        \\  field(v_i64, i64, notNull)
        \\  field(v_u8, u8, notNull)
        \\  field(v_u16, u16, notNull)
        \\  field(v_u32, u32, notNull)
        \\  field(v_u64, u64, notNull)
        \\}
    );

    _ = try executor.run(
        "IntegerMatrix |> insert(id = 1, v_i8 = -128, v_i16 = 32767, v_i32 = -2147483648, v_i64 = 9223372036854775807, v_u8 = 255, v_u16 = 0, v_u32 = 4294967295, v_u64 = 18446744073709551615) {}",
    );

    const result = try executor.run(
        "IntegerMatrix |> where(id == 1) { id v_i8 v_i16 v_i32 v_i64 v_u8 v_u16 v_u32 v_u64 }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n" ++
            "1,-128,32767,-2147483648,9223372036854775807,255,0,4294967295,18446744073709551615\n",
        result,
    );
}

test "internal integer matrix insert reports failing column path for mixed assignments" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\IntegerMatrixValidation {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v_i8, i8, notNull)
        \\  field(v_u16, u16, notNull)
        \\}
    );

    const result = try executor.run(
        "IntegerMatrixValidation |> insert(id = 1, v_i8 = 10, v_u16 = 65536) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: phase=mutation code=IntegerOutOfRange path=insert.v_u16 line=1 col=62 message=\"value is out of range (0 to 65535)\"\n",
        result,
    );
}
