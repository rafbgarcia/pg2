//! Feature coverage for where expression parity and fail-closed predicate behavior.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature where fails closed for non-boolean predicate outputs" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\WhereFailClosed {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(base, i64, notNull)
        \\  field(bonus, i64, nullable)
        \\}
    );

    _ = try executor.run("WhereFailClosed |> insert(id = 1, base = 2, bonus = 1) {}");
    _ = try executor.run("WhereFailClosed |> insert(id = 2, base = 7, bonus = null) {}");

    const result = try executor.run(
        "WhereFailClosed |> where(base + bonus) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"where expression must evaluate to boolean\" phase=execution code=QueryExecutionError path=query line=1 col=1\n",
        result,
    );
}
