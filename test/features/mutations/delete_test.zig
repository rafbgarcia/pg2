//! Feature coverage for delete behavior through server session path.
const std = @import("std");
const feature = @import("../test_env_test.zig");

// const returning_overflow_inserted_row_count: usize = 4097;

test "feature delete removes row via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    var result = try executor.run(
        "User |> insert(id = 1, name = \"Alice\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> where(id = 1) |> delete {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=1\n",
        result,
    );

    result = try executor.run("User {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "feature delete with no matching rows reports zero deletions via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    var result = try executor.run(
        "User |> insert(id = 1, name = \"Alice\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> where(id = 999) |> delete {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> where(id = 1) { id name active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,Alice,true\n",
        result,
    );
}

test "feature delete returns pre-delete selected fields via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    var result = try executor.run(
        "User |> insert(id = 1, name = \"Alice\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> where(id = 1) |> delete { id name active }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=1\n1,Alice,true\n",
        result,
    );
}

test "feature delete failure abort keeps previously deleted row visible via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.initWithConfig(.{
        .max_query_slots = 1,
        .undo_max_entries = 1,
        .undo_max_data_bytes = 1024,
    });
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    var result = try executor.run(
        "User |> insert(id = 1, name = \"A\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> insert(id = 2, name = \"B\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> where(active = true) |> delete {}");
    try std.testing.expect(std.mem.startsWith(u8, result, "ERR query: "));
    try std.testing.expect(std.mem.indexOf(u8, result, "code=UndoLogFull") != null);

    result = try executor.run("User |> where(id = 1) { id name active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,A,true\n",
        result,
    );

    result = try executor.run("User |> where(id = 2) { id name active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n2,B,true\n",
        result,
    );

    result = try executor.run("User |> sort(id asc) { id name active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,A,true\n2,B,true\n",
        result,
    );
}

test "feature delete returning delivery failure aborts mutation via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.initWithConfig(.{
        .max_query_slots = 1,
        .wal_buffer_capacity_bytes = 4 * 1024 * 1024,
        .undo_max_entries = 8192,
        .undo_max_data_bytes = 4 * 1024 * 1024,
    });
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    );

    var insert_req_buf: [256]u8 = undefined;
    var row_index: usize = 0;
    while (row_index <= 4096) : (row_index += 1) {
        const id = row_index + 1;
        const insert_req = try std.fmt.bufPrint(
            insert_req_buf[0..],
            "User |> insert(id = {d}, name = \"user-{d}\", active = true) {{}}",
            .{ id, id },
        );
        const insert_result = try executor.run(insert_req);
        try std.testing.expectEqualStrings(
            "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
            insert_result,
        );
    }

    const overflow_result = try executor.run(
        "User |> where(active = true) |> delete { id }",
    );
    try std.testing.expect(std.mem.startsWith(u8, overflow_result, "ERR query: "));
    try std.testing.expect(
        std.mem.indexOf(u8, overflow_result, "code=ReturningBufferExhausted") != null,
    );

    var result = try executor.run("User |> where(id = 1) { id active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,true\n",
        result,
    );

    result = try executor.run("User |> where(id = 4096) { id active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n4096,true\n",
        result,
    );
}
