//! Feature coverage for deterministic expression diagnostics.
const std = @import("std");
const feature = @import("../../test_env_test.zig");
const assertions = @import("../../assertions.zig");

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

test "feature update where predicate fails closed for non-boolean outputs" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\UpdateWhereTypeMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(base, i64, notNull)
        \\  field(bonus, i64, notNull)
        \\  field(flag, bool, nullable)
        \\}
    );

    _ = try executor.run("UpdateWhereTypeMismatch |> insert(id = 1, base = 2, bonus = 1, flag = null) {}");

    var result = try executor.run(
        "UpdateWhereTypeMismatch |> where(base + bonus) |> update(flag = true) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"where expression must evaluate to boolean\" phase=execution code=QueryExecutionError path=query line=1 col=1\n",
        result,
    );

    result = try executor.run("UpdateWhereTypeMismatch |> where(id == 1) { id flag }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,null\n",
        result,
    );
}

test "feature having fails closed for non-boolean predicate outputs" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\HavingTypeMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, notNull)
        \\  field(points, i64, notNull)
        \\}
    );

    _ = try executor.run("HavingTypeMismatch |> insert(id = 1, status = \"open\", points = 5) {}");
    _ = try executor.run("HavingTypeMismatch |> insert(id = 2, status = \"open\", points = 7) {}");

    const result = try executor.run(
        "HavingTypeMismatch |> group(status) |> having(sum(points)) { status }",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"having expression must evaluate to boolean\" phase=execution code=QueryExecutionError path=query line=1 col=1\n",
        result,
    );
}

test "feature nested child where fails closed for non-boolean predicate outputs" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  reference(posts, id, Post.user_id, withoutReferentialIntegrity)
        \\}
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\  field(title, string, notNull)
        \\}
    );

    _ = try executor.run("User |> insert(id = 1, name = \"Alice\") {}");
    _ = try executor.run("Post |> insert(id = 10, user_id = 1, title = \"A10\") {}");

    const result = try executor.run(
        "User |> sort(id asc) { name posts |> where(id + user_id) { id } }",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"where expression must evaluate to boolean\" phase=execution code=QueryExecutionError path=query line=1 col=1\n",
        result,
    );
}

test "feature nested child having fails closed for non-boolean predicate outputs" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  reference(posts, id, Post.user_id, withoutReferentialIntegrity)
        \\}
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\  field(title, string, notNull)
        \\}
    );

    _ = try executor.run("User |> insert(id = 1, name = \"Alice\") {}");
    _ = try executor.run("Post |> insert(id = 10, user_id = 1, title = \"A10\") {}");

    const result = try executor.run(
        "User |> sort(id asc) { name posts |> having(id + user_id) { id } }",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"having expression must evaluate to boolean\" phase=execution code=QueryExecutionError path=query line=1 col=1\n",
        result,
    );
}

test "feature update assignment fails closed on null arithmetic operand" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\UpdateAssignmentNullArithmetic {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(base, i64, notNull)
        \\  field(bonus, i64, nullable)
        \\  field(flag, bool, nullable)
        \\}
    );

    _ = try executor.run("UpdateAssignmentNullArithmetic |> insert(id = 1, base = 9, bonus = null, flag = null) {}");

    const result = try executor.run(
        "UpdateAssignmentNullArithmetic |> update(flag = base + bonus >= 10) {}",
    );
    try assertions.expectContains(result, "phase=mutation code=NullArithmeticOperand");
    try assertions.expectContains(result, "path=update.flag");
}

test "feature update assignment fails closed on incompatible comparison types" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\UpdateAssignmentTypeMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, notNull)
        \\  field(flag, bool, nullable)
        \\}
    );

    _ = try executor.run("UpdateAssignmentTypeMismatch |> insert(id = 1, status = \"1\", flag = null) {}");

    const result = try executor.run(
        "UpdateAssignmentTypeMismatch |> update(flag = status == 1) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );
}

test "feature computed select fails closed on incompatible comparison types" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ComputedSelectTypeMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, notNull)
        \\}
    );

    _ = try executor.run("ComputedSelectTypeMismatch |> insert(id = 1, status = \"1\") {}");

    const result = try executor.run(
        "ComputedSelectTypeMismatch |> sort(id asc) { id bad: status == 1 }",
    );
    try assertions.expectContains(result, "message=\"select computed expression evaluation failed\"");
}

test "feature having fails closed on invalid aggregate operand type" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\BadAggregate {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, notNull)
        \\}
    );

    _ = try executor.run("BadAggregate |> insert(id = 1, status = \"open\") {}");

    const result = try executor.run(
        "BadAggregate |> group(status) |> having(sum(status) > 0) { status }",
    );
    try assertions.expectContains(result, "message=\"aggregate evaluation failed\"");
}
