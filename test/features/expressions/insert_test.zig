//! Feature coverage for insert behavior through server session path.
const std = @import("std");
const pg2 = @import("pg2");
const overflow_mod = pg2.storage.overflow;
const feature = @import("../test_env_test.zig");

const wide_field_count: usize = 127;

fn appendWideFieldName(writer: anytype, field_index: usize) !void {
    try writer.print("f{d:0>3}", .{field_index});
}

fn appendWideFieldDefinition(writer: anytype, field_index: usize) !void {
    try writer.writeAll("  field(");
    try appendWideFieldName(writer, field_index);
    switch (field_index % 3) {
        1 => try writer.writeAll(", i64, notNull)\n"),
        2 => try writer.writeAll(", string, notNull)\n"),
        else => try writer.writeAll(", bool, notNull)\n"),
    }
}

fn appendWideFieldInsertAssignment(writer: anytype, field_index: usize) !void {
    try writer.writeAll(", ");
    try appendWideFieldName(writer, field_index);
    try writer.writeAll(" = ");
    switch (field_index % 3) {
        1 => try writer.print("{d}", .{1000 + field_index}),
        2 => try writer.print("\"v{d:0>3}\"", .{field_index}),
        else => {
            if ((field_index % 2) == 0) {
                try writer.writeAll("false");
            } else {
                try writer.writeAll("true");
            }
        },
    }
}

fn buildWideInsertSchema(buf: []u8, field_count: usize) ![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();
    try writer.writeAll("WideUser {\n");
    try writer.writeAll("  field(id, i64, notNull, primaryKey)\n");
    var field_index: usize = 1;
    while (field_index <= field_count) : (field_index += 1) {
        try appendWideFieldDefinition(writer, field_index);
    }
    try writer.writeAll("}\n");
    return stream.getWritten();
}

fn buildWideInsertRequest(buf: []u8, id: usize) ![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();
    try writer.print("WideUser |> insert(id = {d}", .{id});
    var field_index: usize = 1;
    while (field_index <= wide_field_count) : (field_index += 1) {
        try appendWideFieldInsertAssignment(writer, field_index);
    }
    try writer.writeAll(") {}");
    return stream.getWritten();
}

fn buildBulkUserInsertRequest(buf: []u8, start_id: usize, row_count: usize) ![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();
    try writer.writeAll("User |> insert(");
    var row_index: usize = 0;
    while (row_index < row_count) : (row_index += 1) {
        if (row_index > 0) try writer.writeAll(", ");
        const id = start_id + row_index;
        try writer.print(
            "(id = {d}, name = \"user-{d}\", active = true)",
            .{ id, id },
        );
    }
    try writer.writeAll(") {}");
    return stream.getWritten();
}

fn buildBulkUserWithEmailInsertRequest(buf: []u8, start_id: usize, row_count: usize) ![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();
    try writer.writeAll("User |> insert(");
    var row_index: usize = 0;
    while (row_index < row_count) : (row_index += 1) {
        if (row_index > 0) try writer.writeAll(", ");
        const id = start_id + row_index;
        try writer.print(
            "(id = {d}, email = \"user-{d}@test.com\", name = \"User {d}\")",
            .{ id, id, id },
        );
    }
    try writer.writeAll(") {}");
    return stream.getWritten();
}

test "feature insert returns explicit insert count via session path" {
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

    const result = try executor.run(
        "User |> insert(id = 1, name = \"Alice\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "feature insert returns selected fields via session path" {
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

    const result = try executor.run(
        "User |> insert(id = 1, name = \"Alice\", active = true) { id name }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=1 updated_rows=0 deleted_rows=0\n1,Alice\n",
        result,
    );
}

test "feature insert large-row payloads remain readable via session path" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\Document {
        \\  field(id, i64, notNull, primaryKey)
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

    result = try executor.run("Document |> where(id == 1) { id title payload }");
    var expected_row_a: [1500]u8 = undefined;
    const expected_a = try std.fmt.bufPrint(
        expected_row_a[0..],
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,alpha,{s}\n",
        .{payload_a[0..]},
    );
    try std.testing.expectEqualStrings(expected_a, result);

    result = try executor.run("Document |> where(id == 2) { id title payload }");
    var expected_row_b: [1500]u8 = undefined;
    const expected_b = try std.fmt.bufPrint(
        expected_row_b[0..],
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n2,bravo,{s}\n",
        .{payload_b[0..]},
    );
    try std.testing.expectEqualStrings(expected_b, result);

    result = try executor.run("Document |> where(id == 3) { id title payload }");
    var expected_row_c: [1500]u8 = undefined;
    const expected_c = try std.fmt.bufPrint(
        expected_row_c[0..],
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n3,charlie,{s}\n",
        .{payload_c[0..]},
    );
    try std.testing.expectEqualStrings(expected_c, result);
}

test "feature insert supports 128 total fields with deterministic readback" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;

    var schema_buf: [8 * 1024]u8 = undefined;
    const schema = try buildWideInsertSchema(schema_buf[0..], wide_field_count);
    try executor.applyDefinitions(schema);

    var insert_req_buf: [16 * 1024]u8 = undefined;
    const insert_req = try buildWideInsertRequest(insert_req_buf[0..], 1);
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("WideUser |> where(id == 1) { id f002 f003 f126 f127 }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,v002,true,false,1127\n",
        result,
    );
}

test "feature insert model creation fails closed above 128 total fields" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;

    // Catalog currently allows at most 128 total columns per model.
    var schema_buf: [8 * 1024]u8 = undefined;
    const too_wide_schema = try buildWideInsertSchema(schema_buf[0..], 128);
    try std.testing.expectError(error.TooManyColumns, executor.applyDefinitions(too_wide_schema));
}

test "feature insert duplicate key fails closed late in high-volume workload" {
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

    var insert_req_buf: [256]u8 = undefined;
    var row_index: usize = 0;
    while (row_index < 300) : (row_index += 1) {
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

    var result = try executor.run(
        "User |> insert(id = 299, name = \"duplicate\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=DuplicateKey\n",
        result,
    );

    result = try executor.run("User |> where(id == 299) { id name active }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n299,user-299,true\n",
        result,
    );

    result = try executor.run(
        "User |> insert(id = 301, name = \"user-301\", active = true) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "feature multi-row insert returns explicit insert count via session path" {
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

    const result = try executor.run(
        "User |> insert((id = 1, name = \"Alice\", active = true), (id = 2, name = \"Bob\", active = false), (id = 3, name = \"Carol\", active = true)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=3 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "feature multi-row insert returning yields all inserted rows" {
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

    const result = try executor.run(
        "User |> insert((id = 1, name = \"Alice\", active = true), (id = 2, name = \"Bob\", active = false), (id = 3, name = \"Carol\", active = true)) { id name }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=3 updated_rows=0 deleted_rows=0\n1,Alice\n2,Bob\n3,Carol\n",
        result,
    );
}

test "feature multi-row insert duplicate key fails closed and inserts nothing" {
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
        "User |> insert((id = 1, name = \"Alice\", active = true), (id = 1, name = \"Bob\", active = false)) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=DuplicateKey\n",
        result,
    );

    result = try executor.run("User { id name }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );
}

test "feature multi-row insert large batch keeps PK index correctness" {
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

    var insert_buf: [8 * 1024]u8 = undefined;
    const insert_req = try buildBulkUserInsertRequest(insert_buf[0..], 1, 100);
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=100 updated_rows=0 deleted_rows=0\n",
        result,
    );

    var where_buf: [128]u8 = undefined;
    var expected_buf: [128]u8 = undefined;
    var id: usize = 1;
    while (id <= 100) : (id += 1) {
        const query = try std.fmt.bufPrint(
            where_buf[0..],
            "User |> where(id == {d}) {{ id name }}",
            .{id},
        );
        result = try executor.run(query);
        const expected = try std.fmt.bufPrint(
            expected_buf[0..],
            "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n{d},user-{d}\n",
            .{ id, id },
        );
        try std.testing.expectEqualStrings(expected, result);
    }
}

test "feature multi-row insert large batch maintains non-PK unique index entries" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(email, string, notNull)
        \\  field(name, string, notNull)
        \\  index(idx_email, [email], unique)
        \\}
    );

    var insert_buf: [12 * 1024]u8 = undefined;
    const insert_req = try buildBulkUserWithEmailInsertRequest(insert_buf[0..], 1, 80);
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=80 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> insert(id = 999, email = \"user-40@test.com\", name = \"dup\") {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=DuplicateKey\n",
        result,
    );
}

test "feature multi-row insert token-bound batch preserves heap and unique index correctness" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(email, string, notNull)
        \\  field(name, string, notNull)
        \\  index(idx_email, [email], unique)
        \\}
    );

    var insert_buf: [128 * 1024]u8 = undefined;
    const insert_req = try buildBulkUserWithEmailInsertRequest(insert_buf[0..], 1, 170);
    var result = try executor.run(insert_req);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=170 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run("User |> where(id == 1) { id email }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,user-1@test.com\n",
        result,
    );
    result = try executor.run("User |> where(id == 85) { id email }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n85,user-85@test.com\n",
        result,
    );
    result = try executor.run("User |> where(id == 170) { id email }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n170,user-170@test.com\n",
        result,
    );

    // Duplicate email proves non-PK unique index entries were maintained.
    result = try executor.run(
        "User |> insert(id = 5001, email = \"user-85@test.com\", name = \"dup\") {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=DuplicateKey\n",
        result,
    );
}

test "feature multi-row insert rejects PK duplicate against existing row" {
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

    var result = try executor.run(
        "User |> insert(id = 1, name = \"existing\") {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "User |> insert((id = 1, name = \"dup\"), (id = 2, name = \"new\")) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=DuplicateKey\n",
        result,
    );

    result = try executor.run("User { id name }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,existing\n",
        result,
    );
}

test "feature multi-row insert enforces FK constraints for valid and invalid batches" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\}
        \\
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\  reference(author, user_id, User.id, withReferentialIntegrity(onDeleteRestrict, onUpdateCascade))
        \\}
    );

    var result = try executor.run("User |> insert((id = 1), (id = 2)) {}");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=2 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "Post |> insert((id = 10, user_id = 1), (id = 11, user_id = 2)) {}",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=2 updated_rows=0 deleted_rows=0\n",
        result,
    );

    result = try executor.run(
        "Post |> insert((id = 12, user_id = 1), (id = 13, user_id = 999)) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=ReferentialIntegrityViolation\n",
        result,
    );

    result = try executor.run("Post { id user_id }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n10,1\n11,2\n",
        result,
    );
}

test "feature multi-row insert applies defaults for omitted columns" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(tier, string, notNull, default, "free")
        \\  field(marketing_opt_in, bool, notNull, default, false)
        \\}
    );

    const result = try executor.run(
        "User |> insert((id = 1), (id = 2, tier = \"pro\"), (id = 3)) { id tier marketing_opt_in }",
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=3 inserted_rows=3 updated_rows=0 deleted_rows=0\n1,free,false\n2,pro,false\n3,free,false\n",
        result,
    );
}

test "feature multi-row insert non-PK unique in-batch duplicate fails closed" {
    var env: feature.FeatureEnv = undefined;
    try env.init();
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(email, string, notNull)
        \\  field(name, string, notNull)
        \\  index(idx_email, [email], unique)
        \\}
    );

    var result = try executor.run(
        "User |> insert((id = 1, email = \"dup@test.com\", name = \"A\"), (id = 2, email = \"dup@test.com\", name = \"B\")) {}",
    );
    try std.testing.expectEqualStrings(
        "ERR query: insert failed; class=fatal; code=DuplicateKey\n",
        result,
    );

    result = try executor.run("User { id email }");
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=0 updated_rows=0 deleted_rows=0\n",
        result,
    );
}
