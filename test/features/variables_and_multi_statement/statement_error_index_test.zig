//! Feature coverage for deterministic statement-indexed errors.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature statement index points to failing statement in multi-statement chain" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\IndexUser {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    _ = try executor.run("IndexUser |> insert(id = 1) {}");

    const result = try executor.run(
        \\IndexUser |> where(id == 1) { id }
        \\IndexUser |> where(in(id, missing_ids)) { id }
    );
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR query: statement_index=1") != null);
}
