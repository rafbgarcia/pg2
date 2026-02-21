//! Feature coverage for not-null insert constraint handling.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature insert fails closed on not-null violation from missing required field" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );

    const result = try executor.run(
        "User |> insert(id = 1) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=NullNotAllowed\n",
        result,
    );
}
