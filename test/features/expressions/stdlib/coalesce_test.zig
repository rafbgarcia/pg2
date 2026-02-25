//! Feature coverage for coalesce() builtin semantics.
const std = @import("std");
const feature = @import("../../test_env_test.zig");

test "feature coalesce returns first non-null argument" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CoalesceValues {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(a, i64, nullable)
        \\  field(b, i64, nullable)
        \\  field(out_value, i64, nullable)
        \\}
    );

    _ = try executor.run("CoalesceValues |> insert(id = 1, a = null, b = 7, out_value = null) {}");
    _ = try executor.run("CoalesceValues |> insert(id = 2, a = 5, b = 7, out_value = null) {}");
    _ = try executor.run("CoalesceValues |> insert(id = 3, a = null, b = null, out_value = null) {}");

    var result = try executor.run(
        "CoalesceValues |> where(id == 1) |> update(out_value = coalesce(a, b, 99)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "CoalesceValues |> where(id == 2) |> update(out_value = coalesce(a, b, 99)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "CoalesceValues |> where(id == 3) |> update(out_value = coalesce(a, b, 99)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("CoalesceValues |> sort(id asc) { id out_value }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,7\n2,5\n3,99\n",
        result,
    );

    result = try executor.run(
        "CoalesceValues |> where(coalesce(a, b, 0) >= 6) |> sort(coalesce(a, b, 0) asc, id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n",
        result,
    );
}

test "feature coalesce returns null when all arguments are null" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CoalesceNulls {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(a, i64, nullable)
        \\  field(b, i64, nullable)
        \\  field(out_value, i64, nullable)
        \\}
    );

    _ = try executor.run("CoalesceNulls |> insert(id = 1, a = null, b = null, out_value = 1) {}");
    const result = try executor.run(
        "CoalesceNulls |> where(id == 1) |> update(out_value = coalesce(a, b)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    const verify = try executor.run("CoalesceNulls |> where(id == 1) { id out_value }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,null\n",
        verify,
    );
}

test "feature coalesce fails closed on empty argument list" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CoalesceMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(value, i64, nullable)
        \\}
    );

    _ = try executor.run("CoalesceMismatch |> insert(id = 1, value = null) {}");
    const result = try executor.run(
        "CoalesceMismatch |> where(id == 1) |> update(value = coalesce()) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"update failed\" phase=execution code=TypeMismatch path=query line=1 col=1\n",
        result,
    );
}
