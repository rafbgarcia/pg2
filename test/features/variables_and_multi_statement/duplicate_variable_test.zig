//! Feature coverage for duplicate let variable names.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature duplicate let variable names fail closed with statement index" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    const result = try executor.run(
        \\let a = 1
        \\let a = 2
        \\a
    );
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR query: statement_index=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "duplicate let variable name") != null);
}
