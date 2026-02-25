//! Feature coverage for SQLite-style trim() semantics.
const std = @import("std");
const feature = @import("../../test_env_test.zig");

test "feature trim removes ASCII spaces from both ends" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\TrimProbe {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(raw, string, notNull)
        \\  field(cleaned, string, notNull)
        \\}
    );

    _ = try executor.run(
        "TrimProbe |> insert(id = 1, raw = \"  hello\t \", cleaned = \"\") {}",
    );

    var result = try executor.run(
        "TrimProbe |> where(id == 1) |> update(cleaned = trim(raw)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("TrimProbe |> where(id == 1) { id cleaned }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,hello\t\n",
        result,
    );

    result = try executor.run(
        "TrimProbe |> where(trim(raw) == \"hello\") { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );

    _ = try executor.run(
        "TrimProbe |> insert(id = 2, raw = \"  z\", cleaned = \"\") {}",
    );
    _ = try executor.run(
        "TrimProbe |> insert(id = 3, raw = \" a\", cleaned = \"\") {}",
    );
    result = try executor.run(
        "TrimProbe |> sort(trim(raw) asc, id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n3\n1\n2\n",
        result,
    );
}

test "feature trim fails closed on non-string input" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\TrimMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v, string, notNull)
        \\}
    );

    _ = try executor.run("TrimMismatch |> insert(id = 1, v = \"x\") {}");
    const result = try executor.run(
        "TrimMismatch |> where(id == 1) |> update(v = trim(1)) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );
}

test "feature trim fails closed on invalid arity" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\TrimArity {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v, string, notNull)
        \\}
    );

    _ = try executor.run("TrimArity |> insert(id = 1, v = \"x\") {}");
    const result = try executor.run(
        "TrimArity |> where(id == 1) |> update(v = trim(v, \"y\")) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );
}
