//! Stress mutation scenarios that are too heavy for default feature runs.
const std = @import("std");
const feature = @import("../test_env_test.zig");
const seed_batch_size: usize = 256;

fn applyUserSchema(executor: *feature.TestExecutor) !void {
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );
}

fn insertUsersBatched(
    executor: *feature.TestExecutor,
    count: usize,
    batch_size: usize,
) !void {
    var start_id: usize = 1;
    while (start_id <= count) {
        const remaining = count - start_id + 1;
        const batch = @min(batch_size, remaining);

        var req_buf: [16 * 1024]u8 = undefined;
        var stream = std.io.fixedBufferStream(req_buf[0..]);
        const writer = stream.writer();

        try writer.writeAll("User |> insert(");
        var i: usize = 0;
        while (i < batch) : (i += 1) {
            if (i > 0) try writer.writeAll(", ");
            const id = start_id + i;
            try writer.print(
                "(id = {d}, name = \"user-{d}\", active = true)",
                .{ id, id },
            );
        }
        try writer.writeAll(") {}");

        const result = try executor.runSeed(stream.getWritten());
        var expected_buf: [96]u8 = undefined;
        const expected = try std.fmt.bufPrint(
            expected_buf[0..],
            "OK returned_rows=0 inserted_rows={d} updated_rows=0 deleted_rows=0\n",
            .{batch},
        );
        try std.testing.expectEqualStrings(expected, result);

        start_id += batch;
    }
}

test "stress insert high-volume batched requests remain queryable via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try applyUserSchema(executor);
    try insertUsersBatched(executor, 512, seed_batch_size);

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
    try insertUsersBatched(executor, 4097, seed_batch_size);

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
    try insertUsersBatched(executor, 4097, seed_batch_size);

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
