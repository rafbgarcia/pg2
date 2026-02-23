//! Feature coverage for `reference(...)` schema-definition behavior.
const std = @import("std");
const pg2 = @import("pg2");
const feature = @import("../test_env_test.zig");

const catalog_mod = pg2.catalog.meta;

test "feature schema reference registers association without implicit index" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  reference(posts, id, Post.user_id, withoutReferentialIntegrity)
        \\}
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\  field(title, string, notNull)
        \\}
    );

    const user_id = env.catalog.findModel("User").?;
    const post_id = env.catalog.findModel("Post").?;
    const user = env.catalog.models[user_id];
    const post = env.catalog.models[post_id];

    try std.testing.expectEqual(@as(u16, 1), user.association_count);
    // index_count is 1: the auto-created PK B+ tree index.
    try std.testing.expectEqual(@as(u16, 1), user.index_count);
    try std.testing.expectEqual(@as(u16, 1), post.index_count);

    const posts_assoc = user.associations[0];
    try std.testing.expectEqual(catalog_mod.AssociationKind.has_many, posts_assoc.kind);
    try std.testing.expectEqual(
        catalog_mod.ReferentialIntegrityMode.without_referential_integrity,
        posts_assoc.referential_integrity_mode,
    );
    try std.testing.expectEqual(@as(u16, 0), posts_assoc.local_column_id);
    try std.testing.expectEqual(@as(u16, 1), posts_assoc.foreign_key_column_id);
}
