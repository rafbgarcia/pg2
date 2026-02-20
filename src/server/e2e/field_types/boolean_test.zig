//! E2E coverage for boolean field behavior through server session path.
const std = @import("std");
const e2e = @import("../test_env_test.zig");

test "e2e boolean fields support operational toggle workflows" {
    var env: e2e.E2EEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\FeatureFlag {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(enabled, boolean, notNull)
        \\}
    );

    var result = try executor.run(
        "FeatureFlag |> insert(id = 1, name = \"checkout\", enabled = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "FeatureFlag |> where(id = 1) |> update(enabled = false) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("FeatureFlag |> where(id = 1) { id name enabled }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,checkout,false\n",
        result,
    );
}
