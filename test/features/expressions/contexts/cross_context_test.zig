//! Feature coverage for cross-context expression parity.
const std = @import("std");
const feature = @import("../../test_env_test.zig");

test "feature update assignment mirrors where predicate outcomes for composed expressions" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\UpdateAssignmentParity {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(base, i64, notNull)
        \\  field(bonus, i64, nullable)
        \\  field(active, bool, nullable)
        \\  field(status, string, nullable)
        \\  field(in_scope, bool, nullable)
        \\}
    );

    _ = try executor.run("UpdateAssignmentParity |> insert(id = 1, base = 7, bonus = 3, active = false, status = \"open\", in_scope = null) {}");
    _ = try executor.run("UpdateAssignmentParity |> insert(id = 2, base = 2, bonus = 1, active = true, status = \"archived\", in_scope = null) {}");
    _ = try executor.run("UpdateAssignmentParity |> insert(id = 3, base = 5, bonus = 0, active = false, status = \"open\", in_scope = null) {}");
    _ = try executor.run("UpdateAssignmentParity |> insert(id = 4, base = 9, bonus = 2, active = false, status = \"archived\", in_scope = null) {}");

    var result = try executor.run(
        "UpdateAssignmentParity |> where(active == true || base + bonus >= 10 && !in(status, [\"archived\"])) |> sort(id asc) { id }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n2\n",
        result,
    );

    result = try executor.run(
        "UpdateAssignmentParity |> update(in_scope = active == true || base + bonus >= 10 && !in(status, [\"archived\"])) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=4 deleted_rows=0\n",
        result,
    );

    result = try executor.run("UpdateAssignmentParity |> sort(id asc) { id in_scope }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=4 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n2,true\n3,false\n4,false\n",
        result,
    );
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
