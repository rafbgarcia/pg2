//! Feature coverage for subtraction operator behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}

test "feature update supports subtraction on representative numeric types" {
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

    _ = try executor.run("Counter |> insert(id = 1, v_i64 = 41, v_u64 = 9, v_f64 = 3.5) {}");

    var result = try executor.run(
        "Counter |> where(id == 1) |> update(v_i64 = v_i64 - 1, v_u64 = v_u64 - 2, v_f64 = v_f64 - 0.25) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("Counter |> where(id == 1) { id v_i64 v_u64 v_f64 }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,40,7,3.25\n",
        result,
    );
}

test "feature update subtraction fails closed on type mismatch" {
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

    _ = try executor.run("CounterMismatch |> insert(id = 1, value = 41) {}");
    const result = try executor.run(
        "CounterMismatch |> where(id == 1) |> update(value = value - \"1\") {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );
}

test "feature update subtraction fails closed on underflow for constrained integer target" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CounterUnderflow {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, u8, notNull)
        \\}
    );

    _ = try executor.run("CounterUnderflow |> insert(id = 1, value = 0) {}");
    const result = try executor.run(
        "CounterUnderflow |> where(id == 1) |> update(value = value - 1) {}",
    );

    try expectContains(result, "phase=mutation code=IntegerOutOfRange");
    try expectContains(result, "path=update.value");
    try expectContains(result, "message=\"value is out of range (0 to 255)\"");
}

test "feature update subtraction fails closed on null arithmetic operand" {
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
        "CounterNull |> where(id == 1) |> update(value = value - 1) {}",
    );

    try expectContains(result, "phase=mutation code=NullArithmeticOperand");
    try expectContains(result, "path=update.value");
    try expectContains(result, "message=\"arithmetic operand cannot be null\"");
}

test "feature subtraction supports where and sort expression contexts" {
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

    _ = try executor.run("Score |> insert(id = 1, base = 12, extra = 2) {}");
    _ = try executor.run("Score |> insert(id = 2, base = 6, extra = 4) {}");
    _ = try executor.run("Score |> insert(id = 3, base = 9, extra = 1) {}");

    var result = try executor.run(
        "Score |> where(base - extra >= 8) |> sort(base - extra desc, id asc) { id base extra }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,12,2\n3,9,1\n",
        result,
    );

    result = try executor.run(
        "Score |> sort(base - extra asc, id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n2\n3\n1\n",
        result,
    );
}

test "feature update subtraction supports mixed numeric coercion into f64 target" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\MixedSubtraction {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(i_value, i64, notNull)
        \\  field(f_value, f64, notNull)
        \\  field(total, f64, notNull)
        \\}
    );

    _ = try executor.run("MixedSubtraction |> insert(id = 1, i_value = 9, f_value = 0.25, total = 0.0) {}");
    const result = try executor.run(
        "MixedSubtraction |> where(id == 1) |> update(total = i_value - f_value) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    const verify = try executor.run("MixedSubtraction |> where(id == 1) { id total }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,8.75\n",
        verify,
    );
}
