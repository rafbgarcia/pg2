//! Feature coverage for boolean logic composition and short-circuit behavior.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature precedence applies && before || and honors explicit grouping" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\PrecedenceBooleanWhere {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(active, bool, nullable)
        \\  field(score, i64, notNull)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("PrecedenceBooleanWhere |> insert(id = 1, active = false, score = 12, status = \"open\") {}");
    _ = try executor.run("PrecedenceBooleanWhere |> insert(id = 2, active = true, score = 1, status = \"blocked\") {}");
    _ = try executor.run("PrecedenceBooleanWhere |> insert(id = 3, active = false, score = 12, status = \"blocked\") {}");
    _ = try executor.run("PrecedenceBooleanWhere |> insert(id = 4, active = false, score = 9, status = \"open\") {}");
    _ = try executor.run("PrecedenceBooleanWhere |> insert(id = 5, active = null, score = 12, status = \"open\") {}");

    var result = try executor.run(
        "PrecedenceBooleanWhere |> where(active == true || score >= 10 && !in(status, [\"blocked\"])) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n2\n5\n",
        result,
    );

    result = try executor.run(
        "PrecedenceBooleanWhere |> where((active == true || score >= 10) && !in(status, [\"blocked\"])) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n5\n",
        result,
    );
}

test "feature boolean OR short-circuits rhs evaluation" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ShortCircuitOr {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    _ = try executor.run("ShortCircuitOr |> insert(id = 1) {}");

    const result = try executor.run(
        "ShortCircuitOr |> where(true || id == $missing) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n",
        result,
    );
}

test "feature boolean AND short-circuits rhs evaluation" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ShortCircuitAnd {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    _ = try executor.run("ShortCircuitAnd |> insert(id = 1) {}");

    const result = try executor.run(
        "ShortCircuitAnd |> where(false && id == $missing) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );
}
