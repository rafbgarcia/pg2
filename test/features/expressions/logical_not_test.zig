//! Feature coverage for logical-not operator behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature logical-not supports membership negation in where" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\}
    );

    _ = try executor.run("Post |> insert(id = 1, status = \"draft\") {}");
    _ = try executor.run("Post |> insert(id = 2, status = \"published\") {}");
    _ = try executor.run("Post |> insert(id = 3, status = \"archived\") {}");
    _ = try executor.run("Post |> insert(id = 4, status = null) {}");

    const result = try executor.run(
        "Post |> where(!in(status, [\"draft\", \"published\"])) |> sort(id asc) { id status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n3,archived\n",
        result,
    );
}
