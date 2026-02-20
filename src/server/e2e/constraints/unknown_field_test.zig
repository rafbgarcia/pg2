//! E2E coverage for unknown-column insert assignment handling.
const std = @import("std");
const e2e = @import("../test_env_test.zig");

test "e2e insert fails closed on unknown field assignment" {
    var env: e2e.E2EEnv = undefined;
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
