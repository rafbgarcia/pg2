//! Feature coverage for invalid variable type usage diagnostics.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature scalar variable used as in-list fails closed" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\VarTypeUser {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    _ = try executor.run("VarTypeUser |> insert(id = 1) {}");

    const result = try executor.run(
        \\let ids = 1
        \\VarTypeUser |> where(in(id, ids)) { id }
    );
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR query: statement_index=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "variable type mismatch in where expression") != null);
}
