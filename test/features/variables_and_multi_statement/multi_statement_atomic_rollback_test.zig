//! Feature coverage for request-atomic rollback across multiple statements.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature multi-statement mutation failure rolls back prior mutations" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\RollbackUser {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    _ = try executor.run("RollbackUser |> insert(id = 1, name = \"Alice\", active = true) {}");

    const failed = try executor.run(
        \\RollbackUser |> where(id == 1) |> update(active = false) {}
        \\RollbackUser |> update(active = name == 1) {}
        \\RollbackUser |> where(id == 1) { id active }
    );
    try std.testing.expect(std.mem.indexOf(u8, failed, "ERR query:") != null);

    const after = try executor.run("RollbackUser |> where(id == 1) { id active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n",
        after,
    );
}
