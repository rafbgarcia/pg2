//! Feature coverage for insert type-validation handling.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature insert fails closed on type mismatch" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(active, bool, notNull)
        \\}
    );

    const result = try executor.run(
        "User |> insert(id = 1, active = \"yes\") {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=TypeMismatch\n",
        result,
    );
}
