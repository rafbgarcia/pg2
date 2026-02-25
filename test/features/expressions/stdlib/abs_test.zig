//! Feature coverage for abs() builtin semantics.
const std = @import("std");
const feature = @import("../../test_env_test.zig");

test "feature abs supports numeric inputs and null propagation" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\AbsValues {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v_i64, i64, nullable)
        \\  field(v_u64, u64, nullable)
        \\  field(v_f64, f64, nullable)
        \\  field(out_i64, i64, nullable)
        \\  field(out_u64, u64, nullable)
        \\  field(out_f64, f64, nullable)
        \\}
    );

    _ = try executor.run("AbsValues |> insert(id = 1, v_i64 = -7, v_u64 = 8, v_f64 = -2.5, out_i64 = null, out_u64 = null, out_f64 = null) {}");

    var result = try executor.run(
        "AbsValues |> where(id == 1) |> update(out_i64 = abs(v_i64), out_u64 = abs(v_u64), out_f64 = abs(v_f64)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("AbsValues |> where(id == 1) { id out_i64 out_u64 out_f64 }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,7,8,2.5\n",
        result,
    );

    _ = try executor.run("AbsValues |> insert(id = 2, v_i64 = 2, v_u64 = 2, v_f64 = 2.0, out_i64 = null, out_u64 = null, out_f64 = null) {}");
    _ = try executor.run("AbsValues |> insert(id = 3, v_i64 = -1, v_u64 = 1, v_f64 = -1.0, out_i64 = null, out_u64 = null, out_f64 = null) {}");

    result = try executor.run(
        "AbsValues |> where(abs(v_i64) >= 2) |> sort(abs(v_i64) asc, id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n2\n1\n",
        result,
    );
}

test "feature abs fails closed on invalid arity and type" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\AbsMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, i64, notNull)
        \\}
    );

    _ = try executor.run("AbsMismatch |> insert(id = 1, value = 5) {}");

    var result = try executor.run(
        "AbsMismatch |> where(id == 1) |> update(value = abs(value, 1)) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );

    result = try executor.run(
        "AbsMismatch |> where(id == 1) |> update(value = abs(\"x\")) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );
}
