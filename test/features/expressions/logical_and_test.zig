//! Feature coverage for logical-and operator behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

test "feature logical-and supports composed aggregate predicates with membership" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Ticket {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(status, string, nullable)
        \\  field(points, i64, notNull)
        \\}
    );

    _ = try executor.run("Ticket |> insert(id = 1, status = \"open\", points = 5) {}");
    _ = try executor.run("Ticket |> insert(id = 2, status = \"open\", points = 6) {}");
    _ = try executor.run("Ticket |> insert(id = 3, status = \"closed\", points = 4) {}");
    _ = try executor.run("Ticket |> insert(id = 4, status = \"closed\", points = 3) {}");
    _ = try executor.run("Ticket |> insert(id = 5, status = \"closed\", points = 2) {}");
    _ = try executor.run("Ticket |> insert(id = 6, status = null, points = 9) {}");

    const result = try executor.run(
        "Ticket |> group(status) |> having(count(*) >= 2 && sum(points) >= 9 && !in(status, [\"closed\"])) |> sort(status asc) { status }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\nopen\n",
        result,
    );
}
