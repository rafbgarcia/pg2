//! Feature coverage for duplicate-key insert constraint handling.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature insert fails closed on duplicate primary key" {
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

    var result = try executor.run(
        "User |> insert(id = 1, name = \"Alice\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> insert(id = 1, name = \"Bob\") {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: message=\"insert failed\" phase=execution code=DuplicateKey path=query line=1 col=1\n",
        result,
    );
}
