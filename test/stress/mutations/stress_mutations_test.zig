//! Stress mutation scenarios that are too heavy for default feature runs.
const std = @import("std");
const feature = @import("../test_env_test.zig");

fn applyUserSchema(executor: *feature.TestExecutor) !void {
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );
}

fn insertUsers(executor: *feature.TestExecutor, count: usize) !void {
    var insert_req_buf: [256]u8 = undefined;
    var row_index: usize = 0;
    while (row_index < count) : (row_index += 1) {
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
}

test "stress insert high-volume sequential requests remain queryable via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try applyUserSchema(executor);
    try insertUsers(executor, 512);

    var result = try executor.run("User |> where(id == 1) { id name active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,user-1,true\n",
        result,
    );

    result = try executor.run("User |> where(id == 256) { id name active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n256,user-256,true\n",
        result,
    );

    result = try executor.run("User |> where(id == 512) { id name active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n512,user-512,true\n",
        result,
    );
}

test "stress update returning delivery failure aborts mutation via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.initWithConfig(.{
        .max_query_slots = 1,
        .wal_buffer_capacity_bytes = 4 * 1024 * 1024,
        .undo_max_entries = 8192,
        .undo_max_data_bytes = 4 * 1024 * 1024,
    });
    defer env.deinit();

    const executor = &env.executor;
    try applyUserSchema(executor);
    try insertUsers(executor, 4097);

    const overflow_result = try executor.run(
        "User |> where(active == true) |> update(active = false) { id }",
    );
    try std.testing.expect(std.mem.startsWith(u8, overflow_result, "ERR query: "));
    try std.testing.expect(
        std.mem.indexOf(u8, overflow_result, "code=ReturningBufferExhausted") != null,
    );

    var result = try executor.run("User |> where(id == 1) { id active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n",
        result,
    );

    result = try executor.run("User |> where(id == 4096) { id active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n4096,true\n",
        result,
    );
}

test "stress delete returning delivery failure aborts mutation via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.initWithConfig(.{
        .max_query_slots = 1,
        .wal_buffer_capacity_bytes = 4 * 1024 * 1024,
        .undo_max_entries = 8192,
        .undo_max_data_bytes = 4 * 1024 * 1024,
    });
    defer env.deinit();

    const executor = &env.executor;
    try applyUserSchema(executor);
    try insertUsers(executor, 4097);

    const overflow_result = try executor.run(
        "User |> where(active == true) |> delete { id }",
    );
    try std.testing.expect(std.mem.startsWith(u8, overflow_result, "ERR query: "));
    try std.testing.expect(
        std.mem.indexOf(u8, overflow_result, "code=ReturningBufferExhausted") != null,
    );

    var result = try executor.run("User |> where(id == 1) { id active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n",
        result,
    );

    result = try executor.run("User |> where(id == 4096) { id active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n4096,true\n",
        result,
    );
}
