//! Feature coverage for referential-integrity insert constraint handling.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature insert fails closed on foreign-key violation" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\}
        \\Post {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(user_id, bigint, notNull)
        \\  reference(author, user_id, User.id, withReferentialIntegrity(onDeleteRestrict, onUpdateCascade))
        \\}
    );

    const result = try executor.run(
        "Post |> insert(id = 10, user_id = 999) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=ReferentialIntegrityViolation\n",
        result,
    );
}
