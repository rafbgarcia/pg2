//! Feature coverage for update behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature update supports row growth via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    var result = try executor.run(
        "User |> insert(id = 1, name = \"Alice\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> where(id == 1) |> update(name = \"Alicia\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> where(id == 1) { id name active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,Alicia,true\n",
        result,
    );
}

test "feature update returns selected fields via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    var result = try executor.run(
        "User |> insert(id = 1, name = \"Alice\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> where(id == 1) |> update(name = \"Alicia\") { id name active }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=1 deleted_rows=0\n1,Alicia,true\n",
        result,
    );
}

test "feature update assignment mirrors where predicate outcomes for composed expressions" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\UpdateAssignmentParity {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(base, i64, notNull)
        \\  field(bonus, i64, nullable)
        \\  field(active, bool, nullable)
        \\  field(status, string, nullable)
        \\  field(in_scope, bool, nullable)
        \\}
    );

    _ = try executor.run("UpdateAssignmentParity |> insert(id = 1, base = 7, bonus = 3, active = false, status = \"open\", in_scope = null) {}");
    _ = try executor.run("UpdateAssignmentParity |> insert(id = 2, base = 2, bonus = 1, active = true, status = \"archived\", in_scope = null) {}");
    _ = try executor.run("UpdateAssignmentParity |> insert(id = 3, base = 5, bonus = 0, active = false, status = \"open\", in_scope = null) {}");
    _ = try executor.run("UpdateAssignmentParity |> insert(id = 4, base = 9, bonus = 2, active = false, status = \"archived\", in_scope = null) {}");

    var result = try executor.run(
        "UpdateAssignmentParity |> where(active == true || base + bonus >= 10 && !in(status, [\"archived\"])) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n2\n",
        result,
    );

    result = try executor.run(
        "UpdateAssignmentParity |> update(in_scope = active == true || base + bonus >= 10 && !in(status, [\"archived\"])) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=4 deleted_rows=0\n",
        result,
    );

    result = try executor.run("UpdateAssignmentParity |> sort(id asc) { id in_scope }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=4 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n2,true\n3,false\n4,false\n",
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
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR query: phase=mutation code=NullArithmeticOperand") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "path=update.flag") != null);
}

test "feature update assignment preserves null equality and inequality semantics" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\UpdateAssignmentNullEq {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\  field(flag, bool, nullable)
        \\}
    );

    _ = try executor.run("UpdateAssignmentNullEq |> insert(id = 1, status = \"open\", flag = null) {}");
    _ = try executor.run("UpdateAssignmentNullEq |> insert(id = 2, status = \"closed\", flag = null) {}");
    _ = try executor.run("UpdateAssignmentNullEq |> insert(id = 3, status = null, flag = null) {}");

    var result = try executor.run(
        "UpdateAssignmentNullEq |> update(flag = status == null || status != null) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=3 deleted_rows=0\n",
        result,
    );

    result = try executor.run("UpdateAssignmentNullEq |> sort(id asc) { id flag }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n2,true\n3,true\n",
        result,
    );
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
        "ERR query: update failed; class=fatal; code=TypeMismatch\n",
        result,
    );
}
