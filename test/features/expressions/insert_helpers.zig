//! Feature coverage for insert behavior through server session path.
const std = @import("std");
const pg2 = @import("pg2");
const shared = @import("test_shared");
const overflow_mod = pg2.storage.overflow;
pub const feature = @import("../test_env_test.zig");

pub const wide_field_count: usize = 127;

pub fn appendWideFieldName(writer: anytype, field_index: usize) !void {
    try writer.print("f{d:0>3}", .{field_index});
}

pub fn appendWideFieldDefinition(writer: anytype, field_index: usize) !void {
    try writer.writeAll("  field(");
    try appendWideFieldName(writer, field_index);
    switch (field_index % 3) {
        1 => try writer.writeAll(", i64, notNull)\n"),
        2 => try writer.writeAll(", string, notNull)\n"),
        else => try writer.writeAll(", bool, notNull)\n"),
    }
}

pub fn appendWideFieldInsertAssignment(writer: anytype, field_index: usize) !void {
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

pub fn buildWideInsertSchema(buf: []u8, field_count: usize) ![]const u8 {
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

pub fn buildWideInsertRequest(buf: []u8, id: usize) ![]const u8 {
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

pub fn buildBulkUserInsertRequest(buf: []u8, start_id: usize, row_count: usize) ![]const u8 {
    return shared.insert.buildBulkUserInsertRequest(buf, start_id, row_count);
}

pub fn buildBulkUserWithEmailInsertRequest(buf: []u8, start_id: usize, row_count: usize) ![]const u8 {
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

pub fn buildBulkUserNullableEmailInsertRequest(
    buf: []u8,
    start_id: usize,
    row_count: usize,
    duplicate_non_null_in_batch: bool,
) ![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();
    try writer.writeAll("User |> insert(");
    var row_index: usize = 0;
    while (row_index < row_count) : (row_index += 1) {
        if (row_index > 0) try writer.writeAll(", ");
        const id = start_id + row_index;
        try writer.print("(id = {d}, email = ", .{id});
        if ((row_index % 3) == 0) {
            try writer.writeAll("null");
        } else if (duplicate_non_null_in_batch and row_index == row_count - 1) {
            try writer.print("\"user-{d}@test.com\"", .{start_id + 1});
        } else {
            try writer.print("\"user-{d}@test.com\"", .{id});
        }
        try writer.print(", name = \"User {d}\")", .{id});
    }
    try writer.writeAll(") {}");
    return stream.getWritten();
}
