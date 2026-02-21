//! End-to-end coverage for integer literal boundaries and range failures.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature signed integer boundaries include signed minima" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\SignedBoundaries {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v_i8, i8, notNull)
        \\  field(v_i16, i16, notNull)
        \\  field(v_i32, i32, notNull)
        \\  field(v_i64, i64, notNull)
        \\}
    );

    _ = try executor.run(
        "SignedBoundaries |> insert(id = 1, v_i8 = -128, v_i16 = -32768, v_i32 = -2147483648, v_i64 = -9223372036854775808) {}",
    );
    _ = try executor.run(
        "SignedBoundaries |> insert(id = 2, v_i8 = 127, v_i16 = 32767, v_i32 = 2147483647, v_i64 = 9223372036854775807) {}",
    );

    const result = try executor.run(
        "SignedBoundaries |> sort(id asc) { id v_i8 v_i16 v_i32 v_i64 }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n" ++
            "1,-128,-32768,-2147483648,-9223372036854775808\n" ++
            "2,127,32767,2147483647,9223372036854775807\n",
        result,
    );
}

test "feature unsigned integer boundaries preserve full ranges" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\UnsignedBoundaries {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v_u8, u8, notNull)
        \\  field(v_u16, u16, notNull)
        \\  field(v_u32, u32, notNull)
        \\  field(v_u64, u64, notNull)
        \\}
    );

    _ = try executor.run(
        "UnsignedBoundaries |> insert(id = 1, v_u8 = 0, v_u16 = 0, v_u32 = 0, v_u64 = 0) {}",
    );
    _ = try executor.run(
        "UnsignedBoundaries |> insert(id = 2, v_u8 = 255, v_u16 = 65535, v_u32 = 4294967295, v_u64 = 18446744073709551615) {}",
    );

    const result = try executor.run(
        "UnsignedBoundaries |> sort(id asc) { id v_u8 v_u16 v_u32 v_u64 }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n" ++
            "1,0,0,0,0\n" ++
            "2,255,65535,4294967295,18446744073709551615\n",
        result,
    );
}

test "feature integer boundary violations fail closed" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\BoundaryViolations {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(small_i8, i8, notNull)
        \\  field(big_i64, i64, notNull)
        \\  field(big_u64, u64, notNull)
        \\}
    );

    var result = try executor.run(
        "BoundaryViolations |> insert(id = 1, small_i8 = -129, big_i64 = 0, big_u64 = 0) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=TypeMismatch\n",
        result,
    );

    result = try executor.run(
        "BoundaryViolations |> insert(id = 2, small_i8 = 0, big_i64 = -9223372036854775809, big_u64 = 0) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=NumericOverflow\n",
        result,
    );

    result = try executor.run(
        "BoundaryViolations |> insert(id = 3, small_i8 = 0, big_i64 = 0, big_u64 = -1) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=TypeMismatch\n",
        result,
    );
}
