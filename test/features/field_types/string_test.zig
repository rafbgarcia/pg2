//! Feature coverage for string field behavior through server session path.
const std = @import("std");
const pg2 = @import("pg2");
const overflow_mod = pg2.storage.overflow;
const feature = @import("../test_env_test.zig");

test "feature string fields preserve user-facing text values across insert and update" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\CustomerProfile {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(display_name, string, notNull)
        \\}
    );

    _ = try executor.run(
        "CustomerProfile |> insert(id = 1, display_name = \"Ada Lovelace\") {}",
    );

    var result = try executor.run(
        "CustomerProfile |> where(id == 1) { id display_name }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,Ada Lovelace\n",
        result,
    );

    result = try executor.run(
        "CustomerProfile |> where(id == 1) |> update(display_name = \"Grace Hopper\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "CustomerProfile |> where(id == 1) { id display_name }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,Grace Hopper\n",
        result,
    );
}

test "feature string fields support overflow-backed large values end-to-end" {
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
    env.catalog.overflow_page_allocator = try overflow_mod.PageIdAllocator.initWithBounds(11_000, 12);

    var long_name_a: [1200]u8 = undefined;
    @memset(long_name_a[0..], 'a');
    var insert_req_buf: [1500]u8 = undefined;
    const insert_req = try std.fmt.bufPrint(
        insert_req_buf[0..],
        "User |> insert(id = 1, name = \"{s}\") {{}}",
        .{long_name_a[0..]},
    );
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> where(id == 1) { id name }");
    var expected_select_a: [1300]u8 = undefined;
    const expected_a = try std.fmt.bufPrint(
        expected_select_a[0..],
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,{s}\n",
        .{long_name_a[0..]},
    );
    try std.testing.expectEqualStrings(expected_a, result);

    var long_name_b: [1200]u8 = undefined;
    @memset(long_name_b[0..], 'b');
    var update_req_buf: [1700]u8 = undefined;
    const update_req = try std.fmt.bufPrint(
        update_req_buf[0..],
        "User |> where(id == 1) |> update(name = \"{s}\") {{}}",
        .{long_name_b[0..]},
    );
    result = try executor.run(update_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=1 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> where(id == 1) { id name }");
    var expected_select_b: [1300]u8 = undefined;
    const expected_b = try std.fmt.bufPrint(
        expected_select_b[0..],
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,{s}\n",
        .{long_name_b[0..]},
    );
    try std.testing.expectEqualStrings(expected_b, result);
}
