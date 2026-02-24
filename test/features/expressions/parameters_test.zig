//! Feature coverage for expression parameter binding and undefined-parameter failures.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature parameters fail closed for undefined where binding" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ParamWhere {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );

    _ = try executor.run("ParamWhere |> insert(id = 1, name = \"a\") {}");

    const result = try executor.run(
        "ParamWhere |> where(id == $target_id) { id }",
    );
    try std.testing.expectEqualStrings(
        "ERR query: undefined parameter in where expression\n",
        result,
    );
}

test "feature parameters fail closed for undefined mutation assignment binding" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ParamMutation {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );

    const result = try executor.run(
        "ParamMutation |> insert(id = 1, name = $missing) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=UndefinedParameter\n",
        result,
    );
}
