//! Feature coverage for unknown-column insert assignment handling.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature insert fails closed on unknown field assignment" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );

    const result = try executor.run(
        "User |> insert(id = 1, nickname = \"ali\") {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=ColumnNotFound\n",
        result,
    );
}
