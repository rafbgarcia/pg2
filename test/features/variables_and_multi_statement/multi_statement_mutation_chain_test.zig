//! Feature coverage for mutation + read multi-statement requests.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature multi-statement mutation chain accumulates mutation stats and returns final rows" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\MutationChainUser {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    _ = try executor.run("MutationChainUser |> insert(id = 1, name = \"Alice\", active = true) {}");
    _ = try executor.run("MutationChainUser |> insert(id = 2, name = \"Bob\", active = true) {}");

    const result = try executor.run(
        \\MutationChainUser |> where(id == 1) |> update(active = false) {}
        \\MutationChainUser |> where(active == false) |> sort(id asc) { id active }
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=1 deleted_rows=0\n1,false\n",
        result,
    );
}
