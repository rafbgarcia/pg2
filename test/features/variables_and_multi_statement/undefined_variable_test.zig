//! Feature coverage for undefined variable diagnostics.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature undefined variable in predicate fails closed with statement index" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\UndefinedVarUser {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    _ = try executor.run("UndefinedVarUser |> insert(id = 1) {}");

    const result = try executor.run(
        \\UndefinedVarUser |> where(id == 1) { id }
        \\UndefinedVarUser |> where(in(id, ids)) { id }
    );
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR query: statement_index=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "undefined variable in where expression") != null);
}
