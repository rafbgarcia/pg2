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

test "feature insert high-volume sequential requests remain queryable via session path" {
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

    result = try executor.run("WideUser |> where(id = 1) { id f002 f003 f126 f127 }");
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

    result = try executor.run("User |> where(id = 299) { id name active }");
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
