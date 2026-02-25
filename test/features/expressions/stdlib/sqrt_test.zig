//! Feature coverage for sqrt() builtin semantics.
const std = @import("std");
const feature = @import("../../test_env_test.zig");

test "feature sqrt supports integer coercion and floating result output" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\SqrtValues {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v_i64, i64, notNull)
        \\  field(v_f64, f64, notNull)
        \\  field(out_i64, f64, nullable)
        \\  field(out_f64, f64, nullable)
        \\}
    );

    _ = try executor.run("SqrtValues |> insert(id = 1, v_i64 = 16, v_f64 = 9.0, out_i64 = null, out_f64 = null) {}");

    var result = try executor.run(
        "SqrtValues |> where(id == 1) |> update(out_i64 = sqrt(v_i64), out_f64 = sqrt(v_f64)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("SqrtValues |> where(id == 1) { id out_i64 out_f64 }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,4,3\n",
        result,
    );

    _ = try executor.run("SqrtValues |> insert(id = 2, v_i64 = 4, v_f64 = 1.0, out_i64 = null, out_f64 = null) {}");
    _ = try executor.run("SqrtValues |> insert(id = 3, v_i64 = 1, v_f64 = 16.0, out_i64 = null, out_f64 = null) {}");

    result = try executor.run(
        "SqrtValues |> where(sqrt(v_i64) >= 2) |> sort(sqrt(v_i64) desc, id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n2\n",
        result,
    );
}

test "feature sqrt fails closed on invalid arity and type" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\SqrtMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, f64, notNull)
        \\}
    );

    _ = try executor.run("SqrtMismatch |> insert(id = 1, value = 4.0) {}");

    var result = try executor.run(
        "SqrtMismatch |> where(id == 1) |> update(value = sqrt(value, 1)) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );

    result = try executor.run(
        "SqrtMismatch |> where(id == 1) |> update(value = sqrt(\"x\")) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );
}

test "feature sqrt fails closed on negative numeric input" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\SqrtNegative {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, f64, notNull)
        \\}
    );

    _ = try executor.run("SqrtNegative |> insert(id = 1, value = -1.0) {}");
    const result = try executor.run(
        "SqrtNegative |> where(id == 1) |> update(value = sqrt(value)) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=NumericDomain path=query line=1 col=1\n",
        result,
    );
}
