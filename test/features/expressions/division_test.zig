//! Feature coverage for division operator behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}

test "feature update supports division on representative numeric types" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Counter {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v_i64, i64, notNull)
        \\  field(v_u64, u64, notNull)
        \\  field(v_f64, f64, notNull)
        \\}
    );

    _ = try executor.run("Counter |> insert(id = 1, v_i64 = 42, v_u64 = 9, v_f64 = 3.5) {}");

    var result = try executor.run(
        "Counter |> where(id == 1) |> update(v_i64 = v_i64 / 2, v_u64 = v_u64 / 2, v_f64 = v_f64 / 0.5) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("Counter |> where(id == 1) { id v_i64 v_u64 v_f64 }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,21,4,7\n",
        result,
    );
}

test "feature update division fails closed on type mismatch" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CounterMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, i64, notNull)
        \\}
    );

    _ = try executor.run("CounterMismatch |> insert(id = 1, value = 42) {}");
    const result = try executor.run(
        "CounterMismatch |> where(id == 1) |> update(value = value / \"2\") {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: update failed; class=fatal; code=TypeMismatch\n",
        result,
    );
}

test "feature update division fails closed on divide by zero" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CounterZero {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, i64, notNull)
        \\}
    );

    _ = try executor.run("CounterZero |> insert(id = 1, value = 42) {}");
    const result = try executor.run(
        "CounterZero |> where(id == 1) |> update(value = value / 0) {}",
    );

    try expectContains(result, "ERR query: update failed; class=fatal; code=DivisionByZero");
}

test "feature update division fails closed on null arithmetic operand" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CounterNull {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, i64, nullable)
        \\}
    );

    _ = try executor.run("CounterNull |> insert(id = 1, value = null) {}");
    const result = try executor.run(
        "CounterNull |> where(id == 1) |> update(value = value / 2) {}",
    );

    try expectContains(result, "ERR query: phase=mutation code=NullArithmeticOperand");
    try expectContains(result, "path=update.value");
    try expectContains(result, "message=\"arithmetic operand cannot be null\"");
}

test "feature division supports where and sort expression contexts" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Score {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(base, i64, notNull)
        \\  field(extra, i64, notNull)
        \\}
    );

    _ = try executor.run("Score |> insert(id = 1, base = 15, extra = 3) {}");
    _ = try executor.run("Score |> insert(id = 2, base = 8, extra = 2) {}");
    _ = try executor.run("Score |> insert(id = 3, base = 7, extra = 3) {}");

    var result = try executor.run(
        "Score |> where(base / extra >= 4) |> sort(base / extra desc, id asc) { id base extra }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,15,3\n2,8,2\n",
        result,
    );

    result = try executor.run(
        "Score |> sort(base / extra asc, id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n3\n2\n1\n",
        result,
    );
}

test "feature update division supports mixed numeric coercion into f64 target" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\MixedDivision {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(i_value, i64, notNull)
        \\  field(f_value, f64, notNull)
        \\  field(total, f64, notNull)
        \\}
    );

    _ = try executor.run("MixedDivision |> insert(id = 1, i_value = 9, f_value = 2.0, total = 0.0) {}");
    const result = try executor.run(
        "MixedDivision |> where(id == 1) |> update(total = i_value / f_value) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    const verify = try executor.run("MixedDivision |> where(id == 1) { id total }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,4.5\n",
        verify,
    );
}
