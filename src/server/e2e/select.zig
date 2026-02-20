//! E2E coverage for select/query behavior through server session path.
const std = @import("std");
const e2e = @import("test_env.zig");

test "e2e query returns deterministic rows via session path" {
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

    var result = try executor.run(
        "User |> insert(id = 1, name = \"Charlie\", active = true)",
    );
    try std.testing.expectEqualStrings("OK rows=0\n", result);

    result = try executor.run(
        "User |> insert(id = 2, name = \"Alice\", active = true)",
    );
    try std.testing.expectEqualStrings("OK rows=0\n", result);

    result = try executor.run(
        "User |> insert(id = 3, name = \"Bob\", active = false)",
    );
    try std.testing.expectEqualStrings("OK rows=0\n", result);

    result = try executor.run("User |> where(active = true) |> sort(name asc)");
    try std.testing.expectEqualStrings(
        "OK rows=2\n2,Alice,true\n1,Charlie,true\n",
        result,
    );
}
