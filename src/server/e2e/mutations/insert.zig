//! E2E coverage for insert behavior through server session path.
const std = @import("std");
const overflow_mod = @import("../../../storage/overflow.zig");
const e2e = @import("../test_env.zig");

test "e2e insert returns explicit insert count via session path" {
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
        "User |> insert(id = 1, name = \"Alice\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "e2e insert high-volume sequential requests remain queryable via session path" {
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

    var insert_req_buf: [256]u8 = undefined;
    var row_index: usize = 0;
    while (row_index < 512) : (row_index += 1) {
        const id = row_index + 1;
        const insert_req = try std.fmt.bufPrint(
            insert_req_buf[0..],
            "User |> insert(id = {d}, name = \"user-{d}\", active = true) {{}}",
            .{ id, id },
        );
        const result = try executor.run(insert_req);
        try std.testing.expectEqualStrings(
            "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
            result,
        );
    }

    var result = try executor.run("User |> where(id = 1) { id name active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,user-1,true\n",
        result,
    );

    result = try executor.run("User |> where(id = 256) { id name active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n256,user-256,true\n",
        result,
    );

    result = try executor.run("User |> where(id = 512) { id name active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n512,user-512,true\n",
        result,
    );
}

test "e2e insert large-row payloads remain readable via session path" {
    var env: e2e.E2EEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Document {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(title, string, notNull)
        \\  field(payload, string, notNull)
        \\}
    );
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(15_000, 1024);

    var payload_a: [1400]u8 = undefined;
    @memset(payload_a[0..], 'a');
    var payload_b: [1400]u8 = undefined;
    @memset(payload_b[0..], 'b');
    var payload_c: [1400]u8 = undefined;
    @memset(payload_c[0..], 'c');

    var insert_req_buf: [1800]u8 = undefined;
    const insert_a = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "Document |> insert(id = 1, title = \"alpha\", payload = \"{s}\") {{}}",
        .{payload_a[0..]},
    );
    var result = try executor.run(insert_a);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    const insert_b = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "Document |> insert(id = 2, title = \"bravo\", payload = \"{s}\") {{}}",
        .{payload_b[0..]},
    );
    result = try executor.run(insert_b);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    const insert_c = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "Document |> insert(id = 3, title = \"charlie\", payload = \"{s}\") {{}}",
        .{payload_c[0..]},
    );
    result = try executor.run(insert_c);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("Document |> where(id = 1) { id title payload }");
    var expected_row_a: [1500]u8 = undefined;
    const expected_a = try std.fmt.bufPrint(
        expected_row_a[0..],
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,alpha,{s}\n",
        .{payload_a[0..]},
    );
    try std.testing.expectEqualStrings(expected_a, result);

    result = try executor.run("Document |> where(id = 2) { id title payload }");
    var expected_row_b: [1500]u8 = undefined;
    const expected_b = try std.fmt.bufPrint(
        expected_row_b[0..],
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n2,bravo,{s}\n",
        .{payload_b[0..]},
    );
    try std.testing.expectEqualStrings(expected_b, result);

    result = try executor.run("Document |> where(id = 3) { id title payload }");
    var expected_row_c: [1500]u8 = undefined;
    const expected_c = try std.fmt.bufPrint(
        expected_row_c[0..],
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n3,charlie,{s}\n",
        .{payload_c[0..]},
    );
    try std.testing.expectEqualStrings(expected_c, result);
}
