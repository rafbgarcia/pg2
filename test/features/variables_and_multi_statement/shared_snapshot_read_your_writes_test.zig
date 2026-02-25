//! Feature coverage for request snapshot and read-your-own-writes semantics.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature multi-statement request reads its own writes deterministically" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\SnapshotUser {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(active, bool, notNull)
        \\}
    );

    const result = try executor.run(
        \\SnapshotUser |> insert(id = 1, active = true) {}
        \\let target = 1
        \\SnapshotUser |> where(id == target) |> update(active = false) {}
        \\SnapshotUser |> where(id == 1) { id active }
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=1 updated_rows=1 deleted_rows=0\n1,false\n",
        result,
    );
}
