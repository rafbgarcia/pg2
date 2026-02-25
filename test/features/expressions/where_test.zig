//! Feature coverage for where expression parity and fail-closed predicate behavior.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature where supports mixed arithmetic, boolean, and membership predicates" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\WhereParity {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(base, i64, notNull)
        \\  field(bonus, i64, nullable)
        \\  field(active, bool, nullable)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("WhereParity |> insert(id = 1, base = 7, bonus = 3, active = false, status = \"open\") {}");
    _ = try executor.run("WhereParity |> insert(id = 2, base = 2, bonus = 1, active = true, status = \"archived\") {}");
    _ = try executor.run("WhereParity |> insert(id = 3, base = 5, bonus = null, active = false, status = \"open\") {}");
    _ = try executor.run("WhereParity |> insert(id = 4, base = 9, bonus = 2, active = false, status = \"archived\") {}");
    _ = try executor.run("WhereParity |> insert(id = 5, base = 1, bonus = 9, active = null, status = \"open\") {}");
    _ = try executor.run("WhereParity |> insert(id = 6, base = 3, bonus = 3, active = null, status = null) {}");

    const result = try executor.run(
        "WhereParity |> where(active == true || base + bonus >= 10 && !in(status, [\"archived\"])) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n2\n5\n",
        result,
    );
}

test "feature where respects parentheses over default boolean precedence" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\WhereGrouping {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(base, i64, notNull)
        \\  field(bonus, i64, nullable)
        \\  field(active, bool, nullable)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("WhereGrouping |> insert(id = 1, base = 7, bonus = 3, active = false, status = \"open\") {}");
    _ = try executor.run("WhereGrouping |> insert(id = 2, base = 2, bonus = 1, active = true, status = \"archived\") {}");
    _ = try executor.run("WhereGrouping |> insert(id = 3, base = 5, bonus = null, active = false, status = \"open\") {}");
    _ = try executor.run("WhereGrouping |> insert(id = 4, base = 9, bonus = 2, active = false, status = \"archived\") {}");
    _ = try executor.run("WhereGrouping |> insert(id = 5, base = 1, bonus = 9, active = null, status = \"open\") {}");

    const result = try executor.run(
        "WhereGrouping |> where((active == true || base + bonus >= 10) && !in(status, [\"archived\"])) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n5\n",
        result,
    );
}

test "feature where preserves current null equality and inequality semantics" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\WhereNullSemantics {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("WhereNullSemantics |> insert(id = 1, status = \"open\") {}");
    _ = try executor.run("WhereNullSemantics |> insert(id = 2, status = \"closed\") {}");
    _ = try executor.run("WhereNullSemantics |> insert(id = 3, status = null) {}");

    const result = try executor.run(
        "WhereNullSemantics |> where(status == null || status != null) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n2\n3\n",
        result,
    );
}

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
