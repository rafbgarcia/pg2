//! Feature coverage for SQLite-style upper() semantics.
const std = @import("std");
const feature = @import("../../test_env_test.zig");

test "feature upper applies ASCII-only case folding and leaves non-ASCII unchanged" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\LabelUpper {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(raw, string, notNull)
        \\  field(uppered, string, notNull)
        \\}
    );

    _ = try executor.run(
        "LabelUpper |> insert(id = 1, raw = \"hello \xC3\xA4\xC3\xB6\xC3\xBC\", uppered = \"\") {}",
    );

    var result = try executor.run(
        "LabelUpper |> where(id == 1) |> update(uppered = upper(raw)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("LabelUpper |> where(id == 1) { id uppered }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,HELLO \xC3\xA4\xC3\xB6\xC3\xBC\n",
        result,
    );

    result = try executor.run(
        "LabelUpper |> where(upper(raw) == \"HELLO \xC3\xA4\xC3\xB6\xC3\xBC\") { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n",
        result,
    );

    _ = try executor.run(
        "LabelUpper |> insert(id = 2, raw = \"beta\", uppered = \"\") {}",
    );
    _ = try executor.run(
        "LabelUpper |> insert(id = 3, raw = \"Alpha\", uppered = \"\") {}",
    );
    result = try executor.run(
        "LabelUpper |> sort(upper(raw) asc, id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n3\n2\n1\n",
        result,
    );
}

test "feature upper fails closed on non-string input" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\UpperMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v, string, notNull)
        \\}
    );

    _ = try executor.run("UpperMismatch |> insert(id = 1, v = \"x\") {}");
    const result = try executor.run(
        "UpperMismatch |> where(id == 1) |> update(v = upper(1)) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );
}

test "feature upper fails closed on invalid arity" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\UpperArity {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(v, string, notNull)
        \\}
    );

    _ = try executor.run("UpperArity |> insert(id = 1, v = \"x\") {}");
    const result = try executor.run(
        "UpperArity |> where(id == 1) |> update(v = upper(v, \"y\")) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );
}
