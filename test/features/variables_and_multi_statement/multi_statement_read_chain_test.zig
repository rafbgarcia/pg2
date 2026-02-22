//! Feature coverage for read-only multi-statement requests.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature multi-statement read chain returns only final statement rows" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ReadChainUser {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    _ = try executor.run("ReadChainUser |> insert(id = 1, name = \"Alice\", active = true) {}");
    _ = try executor.run("ReadChainUser |> insert(id = 2, name = \"Bob\", active = false) {}");
    _ = try executor.run("ReadChainUser |> insert(id = 3, name = \"Carol\", active = true) {}");

    const result = try executor.run(
        \\ReadChainUser |> where(active == true) |> sort(id asc) { id }
        \\ReadChainUser |> where(active == false) |> sort(id asc) { id name active }
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n2,Bob,false\n",
        result,
    );
}
