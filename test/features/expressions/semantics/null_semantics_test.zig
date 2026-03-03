//! Feature coverage for null semantics across expression contexts.
const std = @import("std");
const feature = @import("../../test_env_test.zig");

test "feature null equality semantics hold in where context" {
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

test "feature null equality semantics hold in computed select context" {
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

test "feature null equality semantics hold in update assignment context" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\UpdateAssignmentNullEq {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\  field(flag, bool, nullable)
        \\}
    );

    _ = try executor.run("UpdateAssignmentNullEq |> insert(id = 1, status = \"open\", flag = null) {}");
    _ = try executor.run("UpdateAssignmentNullEq |> insert(id = 2, status = \"closed\", flag = null) {}");
    _ = try executor.run("UpdateAssignmentNullEq |> insert(id = 3, status = null, flag = null) {}");

    var result = try executor.run(
        "UpdateAssignmentNullEq |> update(flag = status == null || status != null) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=3 deleted_rows=0\n",
        result,
    );

    result = try executor.run("UpdateAssignmentNullEq |> sort(id asc) { id flag }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n2,true\n3,true\n",
        result,
    );
}

test "feature null equality semantics hold in having context" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Probe {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("Probe |> insert(id = 1, status = \"open\") {}");
    _ = try executor.run("Probe |> insert(id = 2, status = \"closed\") {}");
    _ = try executor.run("Probe |> insert(id = 3, status = null) {}");

    const result = try executor.run(
        "Probe |> group(status) |> having(count(*) > 0 && (status == null || status != null)) |> sort(status asc) { status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=0 updated_rows=0 deleted_rows=0\nclosed\nopen\nnull\n",
        result,
    );
}
