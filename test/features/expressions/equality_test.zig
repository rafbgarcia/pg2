//! Feature coverage for equality operator behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature equality preserves null semantics in where context" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\WhereNullSemantics {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("WhereNullSemantics |> insert(id = 1, status = \"open\") {}");
    _ = try executor.run("WhereNullSemantics |> insert(id = 2, status = \"closed\") {}");
    _ = try executor.run("WhereNullSemantics |> insert(id = 3, status = null) {}");

    const result = try executor.run(
        "WhereNullSemantics |> where(status == null || status != null) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n2\n3\n",
        result,
    );
}
