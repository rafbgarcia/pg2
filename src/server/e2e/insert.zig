//! E2E coverage for insert behavior through server session path.
const std = @import("std");
const e2e = @import("test_env.zig");

test "e2e insert returns success via session path" {
    var env: e2e.E2EEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, boolean, notNull)
        \\}
    );

    const result = try executor.run(
        "User |> insert(id = 1, name = \"Alice\", active = true)",
    );
    try std.testing.expectEqualStrings("OK rows=0\n", result);
}
