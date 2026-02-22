//! Feature coverage for round() builtin semantics.
const std = @import("std");
const feature = @import("../../test_env_test.zig");

test "feature round supports numeric inputs with f64 result" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\RoundValues {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v_f64, f64, notNull)
        \\  field(v_i64, i64, notNull)
        \\  field(out_f64, f64, nullable)
        \\  field(out_i64, f64, nullable)
        \\}
    );

    _ = try executor.run("RoundValues |> insert(id = 1, v_f64 = 2.4, v_i64 = 5, out_f64 = null, out_i64 = null) {}");

    var result = try executor.run(
        "RoundValues |> where(id == 1) |> update(out_f64 = round(v_f64), out_i64 = round(v_i64)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("RoundValues |> where(id == 1) { id out_f64 out_i64 }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,2,5\n",
        result,
    );

    _ = try executor.run("RoundValues |> insert(id = 2, v_f64 = 2.6, v_i64 = 6, out_f64 = null, out_i64 = null) {}");
    _ = try executor.run("RoundValues |> insert(id = 3, v_f64 = -1.2, v_i64 = -1, out_f64 = null, out_i64 = null) {}");
    result = try executor.run(
        "RoundValues |> where(round(v_f64) >= 2) |> sort(round(v_f64) desc, id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n2\n1\n",
        result,
    );
}

test "feature round fails closed on invalid arity and type" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\RoundMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, f64, notNull)
        \\}
    );

    _ = try executor.run("RoundMismatch |> insert(id = 1, value = 1.25) {}");

    var result = try executor.run(
        "RoundMismatch |> where(id == 1) |> update(value = round(value, 1)) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: update failed; class=fatal; code=TypeMismatch\n",
        result,
    );

    result = try executor.run(
        "RoundMismatch |> where(id == 1) |> update(value = round(\"x\")) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: update failed; class=fatal; code=TypeMismatch\n",
        result,
    );
}

test "feature round uses nearest-even tie policy for f64" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\RoundTie {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v, f64, notNull)
        \\  field(out_v, f64, nullable)
        \\}
    );

    _ = try executor.run("RoundTie |> insert(id = 1, v = 2.5, out_v = null) {}");
    _ = try executor.run("RoundTie |> insert(id = 2, v = 3.5, out_v = null) {}");
    _ = try executor.run("RoundTie |> insert(id = 3, v = -2.5, out_v = null) {}");
    _ = try executor.run("RoundTie |> insert(id = 4, v = -3.5, out_v = null) {}");

    var result = try executor.run(
        "RoundTie |> update(out_v = round(v)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=4 deleted_rows=0\n",
        result,
    );

    result = try executor.run("RoundTie |> sort(id asc) { id out_v }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=4 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,2\n2,4\n3,-2\n4,-4\n",
        result,
    );
}
