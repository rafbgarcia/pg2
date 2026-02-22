//! Feature coverage for operator precedence and explicit parentheses grouping.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature precedence applies arithmetic before comparison in where predicates" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\PrecedenceArithmeticWhere {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(a, i64, notNull)
        \\  field(b, i64, notNull)
        \\  field(c, i64, notNull)
        \\}
    );

    _ = try executor.run("PrecedenceArithmeticWhere |> insert(id = 1, a = 2, b = 3, c = 4) {}");
    _ = try executor.run("PrecedenceArithmeticWhere |> insert(id = 2, a = 2, b = 3, c = 3) {}");
    _ = try executor.run("PrecedenceArithmeticWhere |> insert(id = 3, a = 5, b = 2, c = 4) {}");
    _ = try executor.run("PrecedenceArithmeticWhere |> insert(id = 4, a = 1, b = 5, c = 3) {}");

    var result = try executor.run(
        "PrecedenceArithmeticWhere |> where(a + b * c >= 14) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n4\n",
        result,
    );

    result = try executor.run(
        "PrecedenceArithmeticWhere |> where((a + b) * c >= 14) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=4 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n2\n3\n4\n",
        result,
    );
}

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

test "feature precedence controls sort expression key grouping" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\PrecedenceSort {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(a, i64, notNull)
        \\  field(b, i64, notNull)
        \\  field(c, i64, notNull)
        \\}
    );

    _ = try executor.run("PrecedenceSort |> insert(id = 1, a = 2, b = 3, c = 4) {}");
    _ = try executor.run("PrecedenceSort |> insert(id = 2, a = 2, b = 3, c = 3) {}");
    _ = try executor.run("PrecedenceSort |> insert(id = 3, a = 5, b = 2, c = 4) {}");
    _ = try executor.run("PrecedenceSort |> insert(id = 4, a = 1, b = 5, c = 3) {}");

    var result = try executor.run(
        "PrecedenceSort |> sort(a + b * c desc, id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=4 inserted_rows=0 updated_rows=0 deleted_rows=0\n4\n1\n3\n2\n",
        result,
    );

    result = try executor.run(
        "PrecedenceSort |> sort((a + b) * c desc, id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=4 inserted_rows=0 updated_rows=0 deleted_rows=0\n3\n1\n4\n2\n",
        result,
    );
}

test "feature computed select exposes grouped and ungrouped precedence outcomes" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\PrecedenceComputedSelect {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(a, i64, notNull)
        \\  field(b, i64, notNull)
        \\  field(c, i64, notNull)
        \\}
    );

    _ = try executor.run("PrecedenceComputedSelect |> insert(id = 1, a = 2, b = 3, c = 4) {}");
    _ = try executor.run("PrecedenceComputedSelect |> insert(id = 2, a = 2, b = 3, c = 3) {}");

    const result = try executor.run(
        "PrecedenceComputedSelect |> sort(id asc) { id default_group: a + b * c grouped: (a + b) * c }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,14,20\n2,11,15\n",
        result,
    );
}
