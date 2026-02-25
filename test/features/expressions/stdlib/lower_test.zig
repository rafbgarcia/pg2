//! Feature coverage for SQLite-style lower() semantics.
const std = @import("std");
const feature = @import("../../test_env_test.zig");

test "feature lower applies ASCII-only case folding and leaves non-ASCII unchanged" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Label {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(raw, string, notNull)
        \\  field(lowered, string, notNull)
        \\}
    );

    _ = try executor.run(
        "Label |> insert(id = 1, raw = \"HELLO \xC3\x84\xC3\x96\xC3\x9C\", lowered = \"\") {}",
    );

    var result = try executor.run(
        "Label |> where(id == 1) |> update(lowered = lower(raw)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("Label |> where(id == 1) { id lowered }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,hello \xC3\x84\xC3\x96\xC3\x9C\n",
        result,
    );

    result = try executor.run(
        "Label |> where(lower(raw) == \"hello \xC3\x84\xC3\x96\xC3\x9C\") { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n",
        result,
    );

    _ = try executor.run(
        "Label |> insert(id = 2, raw = \"beta\", lowered = \"\") {}",
    );
    _ = try executor.run(
        "Label |> insert(id = 3, raw = \"Alpha\", lowered = \"\") {}",
    );
    result = try executor.run(
        "Label |> sort(lower(raw) asc, id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n3\n2\n1\n",
        result,
    );
}

test "feature lower fails closed on non-string input" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\LowerMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v, string, notNull)
        \\}
    );

    _ = try executor.run("LowerMismatch |> insert(id = 1, v = \"x\") {}");
    const result = try executor.run(
        "LowerMismatch |> where(id == 1) |> update(v = lower(1)) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );
}

test "feature lower fails closed on invalid arity" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\LowerArity {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v, string, notNull)
        \\}
    );

    _ = try executor.run("LowerArity |> insert(id = 1, v = \"x\") {}");
    const result = try executor.run(
        "LowerArity |> where(id == 1) |> update(v = lower(v, \"y\")) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );
}
