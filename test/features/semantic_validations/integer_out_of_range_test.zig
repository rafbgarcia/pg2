//! Feature coverage for integer range validation diagnostics.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature integer boundary violations fail closed with path and location" {
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
        "ERR query: phase=mutation code=IntegerOutOfRange path=insert.small_i8 line=1 col=49 message=\"value is out of range (-128 to 127)\"\n",
        result,
    );

    result = try executor.run(
        "BoundaryViolations |> insert(id = 2, small_i8 = 0, big_i64 = -9223372036854775809, big_u64 = 0) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: phase=mutation code=IntegerOutOfRange path=insert.big_i64 line=1 col=62 message=\"value is out of range (-9223372036854775808 to 9223372036854775807)\"\n",
        result,
    );

    result = try executor.run(
        "BoundaryViolations |> insert(id = 3, small_i8 = 0, big_i64 = 0, big_u64 = -1) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: phase=mutation code=IntegerOutOfRange path=insert.big_u64 line=1 col=75 message=\"value is out of range (0 to 18446744073709551615)\"\n",
        result,
    );
}
