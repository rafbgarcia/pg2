//! Feature coverage for computed select expression parity.
const std = @import("std");
const feature = @import("../test_env_test.zig");

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}

test "feature computed select mirrors where predicate outcomes for composed expressions" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ComputedSelectParity {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(base, i64, notNull)
        \\  field(bonus, i64, notNull)
        \\  field(active, bool, nullable)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("ComputedSelectParity |> insert(id = 1, base = 7, bonus = 3, active = false, status = \"open\") {}");
    _ = try executor.run("ComputedSelectParity |> insert(id = 2, base = 2, bonus = 1, active = true, status = \"archived\") {}");
    _ = try executor.run("ComputedSelectParity |> insert(id = 3, base = 5, bonus = 0, active = false, status = \"open\") {}");
    _ = try executor.run("ComputedSelectParity |> insert(id = 4, base = 9, bonus = 2, active = false, status = \"archived\") {}");

    var result = try executor.run(
        "ComputedSelectParity |> where(active == true || base + bonus >= 10 && !in(status, [\"archived\"])) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n2\n",
        result,
    );

    result = try executor.run(
        "ComputedSelectParity |> sort(id asc) { id in_scope: active == true || base + bonus >= 10 && !in(status, [\"archived\"]) }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=4 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n2,true\n3,false\n4,false\n",
        result,
    );
}

test "feature computed select preserves null equality and inequality semantics" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ComputedSelectNullEq {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("ComputedSelectNullEq |> insert(id = 1, status = \"open\") {}");
    _ = try executor.run("ComputedSelectNullEq |> insert(id = 2, status = \"closed\") {}");
    _ = try executor.run("ComputedSelectNullEq |> insert(id = 3, status = null) {}");

    const result = try executor.run(
        "ComputedSelectNullEq |> sort(id asc) { id probe: status == null || status != null }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n2,true\n3,true\n",
        result,
    );
}

test "feature computed select fails closed on incompatible comparison types" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ComputedSelectTypeMismatch {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, notNull)
        \\}
    );

    _ = try executor.run("ComputedSelectTypeMismatch |> insert(id = 1, status = \"1\") {}");

    const result = try executor.run(
        "ComputedSelectTypeMismatch |> sort(id asc) { id bad: status == 1 }",
    );
    try expectContains(result, "ERR query: select computed expression evaluation failed");
}
