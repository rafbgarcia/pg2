//! Feature coverage for length() builtin semantics.
const std = @import("std");
const feature = @import("../../test_env_test.zig");

test "feature length returns byte length and propagates null" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\LengthValues {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(raw, string, nullable)
        \\  field(out_len, i64, nullable)
        \\}
    );

    _ = try executor.run("LengthValues |> insert(id = 1, raw = \"hello\", out_len = null) {}");
    _ = try executor.run("LengthValues |> insert(id = 2, raw = null, out_len = null) {}");

    var result = try executor.run(
        "LengthValues |> where(id == 1) |> update(out_len = length(raw)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "LengthValues |> where(id == 2) |> update(out_len = length(raw)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("LengthValues |> sort(id asc) { id out_len }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,5\n2,null\n",
        result,
    );

    _ = try executor.run("LengthValues |> insert(id = 3, raw = \"abc\", out_len = null) {}");
    _ = try executor.run("LengthValues |> insert(id = 4, raw = \"\xC3\x84\", out_len = null) {}");
    result = try executor.run(
        "LengthValues |> where(length(raw) >= 2) |> sort(length(raw) asc, id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n4\n3\n1\n",
        result,
    );

    result = try executor.run("LengthValues |> where(id == 4) |> update(out_len = length(raw)) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );
    result = try executor.run("LengthValues |> where(id == 4) { id out_len }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n4,2\n",
        result,
    );
}

test "feature length fails closed on invalid arity and type" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\LengthMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(raw, string, notNull)
        \\}
    );

    _ = try executor.run("LengthMismatch |> insert(id = 1, raw = \"abc\") {}");

    var result = try executor.run(
        "LengthMismatch |> where(id == 1) |> update(raw = length(raw, \"x\")) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: update failed; class=fatal; code=TypeMismatch\n",
        result,
    );

    result = try executor.run(
        "LengthMismatch |> where(id == 1) |> update(raw = length(1)) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: update failed; class=fatal; code=TypeMismatch\n",
        result,
    );
}
