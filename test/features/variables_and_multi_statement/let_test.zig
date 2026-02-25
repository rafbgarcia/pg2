//! Feature coverage for let scalar and list bindings.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature let scalar binding can be returned by final expression statement" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\LetUser {
        \\  field(id, i64, notNull, primaryKey)
        \\}
    );

    _ = try executor.run("LetUser |> insert(id = 1) {}");
    _ = try executor.run("LetUser |> insert(id = 2) {}");

    const result = try executor.run(
        \\let total = LetUser |> sort(id asc) |> limit(1) { id }
        \\total
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n",
        result,
    );
}

test "feature let list from query can drive in predicate in a later statement" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ListUser {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(active, bool, notNull)
        \\}
    );

    _ = try executor.run("ListUser |> insert(id = 1, active = true) {}");
    _ = try executor.run("ListUser |> insert(id = 2, active = false) {}");
    _ = try executor.run("ListUser |> insert(id = 3, active = true) {}");

    const result = try executor.run(
        \\let ids = ListUser |> where(active == true) |> sort(id asc) { id }
        \\ListUser |> where(in(id, ids)) |> sort(id asc) { id active }
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n3,true\n",
        result,
    );
}
