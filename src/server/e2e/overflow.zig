const std = @import("std");
const overflow_mod = @import("../../storage/overflow.zig");
const e2e = @import("test_env.zig");

test "e2e overflow insert update and read via session path" {
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
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(11_000, 12);

    var long_name_a: [1200]u8 = undefined;
    @memset(long_name_a[0..], 'a');
    var insert_req_buf: [1500]u8 = undefined;
    const insert_req = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "User |> insert(id = 1, name = \"{s}\")",
        .{long_name_a[0..]},
    );
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings("OK rows=0\n", result);

    result = try executor.run("User |> where(id = 1)");
    var expected_select_a: [1300]u8 = undefined;
    const expected_a = try std.fmt.bufPrint(
        expected_select_a[0..],
        "OK rows=1\n1,{s}\n",
        .{long_name_a[0..]},
    );
    try std.testing.expectEqualStrings(expected_a, result);

    var long_name_b: [1200]u8 = undefined;
    @memset(long_name_b[0..], 'b');
    var update_req_buf: [1700]u8 = undefined;
    const update_req = try std.fmt.bufPrint(
        update_req_buf[0..],
        "User |> where(id = 1) |> update(name = \"{s}\")",
        .{long_name_b[0..]},
    );
    result = try executor.run(update_req);
    try std.testing.expectEqualStrings("OK rows=0\n", result);

    result = try executor.run("User |> where(id = 1)");
    var expected_select_b: [1300]u8 = undefined;
    const expected_b = try std.fmt.bufPrint(
        expected_select_b[0..],
        "OK rows=1\n1,{s}\n",
        .{long_name_b[0..]},
    );
    try std.testing.expectEqualStrings(expected_b, result);
}

test "e2e overflow delete drains reclaim queue deterministically" {
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
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(12_000, 8);

    var long_name: [1200]u8 = undefined;
    @memset(long_name[0..], 'x');
    var insert_req_buf: [1500]u8 = undefined;
    const insert_req = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "User |> insert(id = 1, name = \"{s}\")",
        .{long_name[0..]},
    );
    _ = try executor.run(insert_req);

    var result = try executor.run("User |> where(id = 1) |> delete");
    try std.testing.expectEqualStrings("OK rows=0\n", result);
    result = try executor.run("User |> where(id = 1)");
    try std.testing.expectEqualStrings("OK rows=0\n", result);
    try std.testing.expect(env.catalog.overflow_reclaim_queue.isEmpty());
}
