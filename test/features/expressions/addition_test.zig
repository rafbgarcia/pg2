//! Feature coverage for addition operator behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}

test "feature update supports addition on representative numeric types" {
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
        "Counter |> where(id == 1) |> update(v_i64 = v_i64 + 1, v_u64 = v_u64 + 2, v_f64 = v_f64 + 0.25) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("Counter |> where(id == 1) { id v_i64 v_u64 v_f64 }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,42,11,3.75\n",
        result,
    );
}

test "feature update addition fails closed on type mismatch" {
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
        "CounterMismatch |> where(id == 1) |> update(value = value + \"1\") {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: update failed; class=fatal; code=TypeMismatch\n",
        result,
    );
}

test "feature update addition fails closed on overflow for constrained integer target" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CounterOverflow {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, u8, notNull)
        \\}
    );

    _ = try executor.run("CounterOverflow |> insert(id = 1, value = 255) {}");
    const result = try executor.run(
        "CounterOverflow |> where(id == 1) |> update(value = value + 1) {}",
    );

    try expectContains(result, "ERR query: phase=mutation code=IntegerOutOfRange");
    try expectContains(result, "path=update.value");
    try expectContains(result, "message=\"value is out of range (0 to 255)\"");
}

test "feature update addition fails closed on null arithmetic operand" {
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
        "CounterNull |> where(id == 1) |> update(value = value + 1) {}",
    );

    try expectContains(result, "ERR query: phase=mutation code=NullArithmeticOperand");
    try expectContains(result, "path=update.value");
    try expectContains(result, "message=\"arithmetic operand cannot be null\"");
}
