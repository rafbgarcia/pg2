const std = @import("std");

/// Column data types supported by pg2.
pub const ColumnType = enum(u8) {
    bigint = 1, // i64, 8 bytes
    int = 2, // i32, 4 bytes
    float = 3, // f64, 8 bytes
    boolean = 4, // bool, 1 byte
    string = 5, // variable length, u16-length-prefixed
    timestamp = 6, // i64, 8 bytes (microseconds since epoch)
};

/// A typed value. One variant per column type plus a null sentinel.
pub const Value = union(enum) {
    bigint: i64,
    int: i32,
    float: f64,
    boolean: bool,
    string: []const u8,
    timestamp: i64,
    null_value: void,

    /// Returns the column type, or null for null_value.
    pub fn columnType(self: Value) ?ColumnType {
        return switch (self) {
            .bigint => .bigint,
            .int => .int,
            .float => .float,
            .boolean => .boolean,
            .string => .string,
            .timestamp => .timestamp,
            .null_value => null,
        };
    }
};

/// Comparison result for Values. Null sorts last.
pub fn compareValues(a: Value, b: Value) std.math.Order {
    // Null handling: null sorts last.
    const a_null = (a == .null_value);
    const b_null = (b == .null_value);
    if (a_null and b_null) return .eq;
    if (a_null) return .gt;
    if (b_null) return .lt;

    return switch (a) {
        .bigint => |av| switch (b) {
            .bigint => |bv| std.math.order(av, bv),
            else => .lt,
        },
        .int => |av| switch (b) {
            .int => |bv| std.math.order(av, bv),
            else => .lt,
        },
        .float => |av| switch (b) {
            .float => |bv| std.math.order(av, bv),
            else => .lt,
        },
        .boolean => |av| switch (b) {
            .boolean => |bv| blk: {
                const ai: u1 = @intFromBool(av);
                const bi: u1 = @intFromBool(bv);
                break :blk std.math.order(ai, bi);
            },
            else => .lt,
        },
        .string => |av| switch (b) {
            .string => |bv| std.mem.order(u8, av, bv),
            else => .lt,
        },
        .timestamp => |av| switch (b) {
            .timestamp => |bv| std.math.order(av, bv),
            else => .lt,
        },
        .null_value => @panic("compareValues null arm reached unexpectedly"),
    };
}

/// Definition of a single column in a row schema.
pub const ColumnDef = struct {
    name_offset: u16,
    name_len: u16,
    column_type: ColumnType,
    nullable: bool,
};

/// Maximum columns per row.
pub const max_columns = 128;

/// Schema describing the layout of rows. Fixed after creation.
pub const RowSchema = struct {
    columns: [max_columns]ColumnDef = undefined,
    column_count: u16 = 0,
    /// Total bytes for fixed-size columns (not including null bitmap).
    fixed_size_bytes: u16 = 0,
    /// Bytes needed for the null bitmap (1 bit per column, rounded up).
    null_bitmap_bytes: u16 = 0,

    /// Name buffer shared across all columns.
    name_buffer: [4096]u8 = undefined,
    name_buffer_len: u16 = 0,

    pub fn addColumn(
        self: *RowSchema,
        name: []const u8,
        col_type: ColumnType,
        nullable: bool,
    ) error{ TooManyColumns, NameBufferFull }!u16 {
        if (self.column_count >= max_columns) return error.TooManyColumns;
        if (name.len > std.math.maxInt(u16)) return error.NameBufferFull;
        const remaining = self.name_buffer.len - self.name_buffer_len;
        if (name.len > remaining) return error.NameBufferFull;
        const idx = self.column_count;

        // Copy name into buffer.
        const name_offset = self.name_buffer_len;
        const name_u16: u16 = @intCast(name.len);
        @memcpy(self.name_buffer[name_offset..][0..name.len], name);
        self.name_buffer_len += name_u16;

        self.columns[idx] = .{
            .name_offset = name_offset,
            .name_len = name_u16,
            .column_type = col_type,
            .nullable = nullable,
        };
        self.column_count += 1;

        // Recalculate sizes.
        self.null_bitmap_bytes = (self.column_count + 7) / 8;
        self.fixed_size_bytes = 0;
        for (0..self.column_count) |i| {
            self.fixed_size_bytes += fixedSize(self.columns[i].column_type);
        }

        return idx;
    }

    pub fn getColumnName(self: *const RowSchema, idx: u16) []const u8 {
        const col = self.columns[idx];
        return self.name_buffer[col.name_offset..][0..col.name_len];
    }

    pub fn findColumn(self: *const RowSchema, name: []const u8) ?u16 {
        for (0..self.column_count) |i| {
            const col_name = self.getColumnName(@intCast(i));
            if (std.mem.eql(u8, col_name, name)) return @intCast(i);
        }
        return null;
    }
};

/// Returns the fixed byte size for a column type. Strings are variable (0).
pub fn fixedSize(col_type: ColumnType) u16 {
    return switch (col_type) {
        .bigint => 8,
        .int => 4,
        .float => 8,
        .boolean => 1,
        .string => 0,
        .timestamp => 8,
    };
}

/// Encoding errors.
pub const EncodeError = error{
    BufferTooSmall,
    TypeMismatch,
    NullNotAllowed,
};

pub const DecodeError = error{
    Corruption,
    InvalidRowFormat,
    UnsupportedRowVersion,
};

pub const row_format_magic: u16 = 0x5232; // "R2"
pub const row_format_version: u8 = 2;
pub const row_format_version_legacy: u8 = 1;
const row_header_size: usize = 3; // magic:u16 + version:u8
const string_fixed_slot_size_legacy: usize = 2;
const string_fixed_slot_size: usize = 10;
const string_slot_inline_tag: u8 = 1;
const string_slot_overflow_tag: u8 = 2;

pub const DecodedColumn = union(enum) {
    value: Value,
    string_overflow_page_id: u64,
};

/// Encode a row of values into a byte buffer.
///
/// Layout:
///   [magic:u16][version:u8][null_bitmap: N bytes]
///   [fixed columns in order] [variable-length data]
///
/// Variable-length strings are stored as u16 length prefix + data, appended
/// after all fixed columns. Fixed-column slots for strings store a u16 offset
/// (relative to start of row) pointing to the string's length prefix.
///
/// Returns the number of bytes written.
pub fn encodeRow(
    schema: *const RowSchema,
    values: []const Value,
    buf: []u8,
) EncodeError!u16 {
    return encodeRowInternal(schema, values, null, buf);
}

/// Encode row values with optional per-column overflow page pointers.
///
/// `string_overflow_page_ids` must have `schema.column_count` entries.
/// For each non-null string column:
/// - `0` means inline string payload in row bytes.
/// - non-zero means store an overflow pointer to that first page id.
pub fn encodeRowWithOverflow(
    schema: *const RowSchema,
    values: []const Value,
    string_overflow_page_ids: []const u64,
    buf: []u8,
) EncodeError!u16 {
    if (string_overflow_page_ids.len < schema.column_count) return error.BufferTooSmall;
    return encodeRowInternal(schema, values, string_overflow_page_ids, buf);
}

fn encodeRowInternal(
    schema: *const RowSchema,
    values: []const Value,
    string_overflow_page_ids: ?[]const u64,
    buf: []u8,
) EncodeError!u16 {
    std.debug.assert(values.len == schema.column_count);

    var total: usize = row_header_size + schema.null_bitmap_bytes;
    for (0..schema.column_count) |i| {
        const col = schema.columns[i];
        if (values[i] == .null_value) {
            if (!col.nullable) return error.NullNotAllowed;
            total += if (col.column_type == .string) string_fixed_slot_size else fixedSize(col.column_type);
            continue;
        }
        if (values[i].columnType()) |vt| {
            if (vt != col.column_type) return error.TypeMismatch;
        }
        if (col.column_type == .string) {
            total += string_fixed_slot_size;
            const overflow_page_id = if (string_overflow_page_ids) |ids|
                ids[i]
            else
                0;
            if (overflow_page_id == 0) {
                if (values[i].string.len > std.math.maxInt(u16)) return error.BufferTooSmall;
                total += 2 + values[i].string.len;
            }
        } else {
            total += fixedSize(col.column_type);
        }
    }

    if (total > buf.len) return error.BufferTooSmall;
    if (total > std.math.maxInt(u16)) return error.BufferTooSmall;

    @memset(buf[0..total], 0);
    @memcpy(
        buf[0..2],
        std.mem.asBytes(&std.mem.nativeToLittle(u16, row_format_magic)),
    );
    buf[2] = row_format_version;

    for (0..schema.column_count) |i| {
        if (values[i] == .null_value) {
            buf[row_header_size + i / 8] |= @as(u8, 1) << @intCast(i % 8);
        }
    }

    var fixed_offset: usize = row_header_size + schema.null_bitmap_bytes;
    var var_data_start: usize = row_header_size + schema.null_bitmap_bytes;
    for (0..schema.column_count) |i| {
        const col = schema.columns[i];
        var_data_start += if (col.column_type == .string)
            string_fixed_slot_size
        else
            fixedSize(col.column_type);
    }
    var var_offset: usize = var_data_start;

    for (0..schema.column_count) |i| {
        const col = schema.columns[i];
        if (values[i] == .null_value) {
            fixed_offset += if (col.column_type == .string)
                string_fixed_slot_size
            else
                fixedSize(col.column_type);
            continue;
        }

        switch (col.column_type) {
            .bigint => {
                const v = values[i].bigint;
                @memcpy(buf[fixed_offset..][0..8], std.mem.asBytes(
                    &std.mem.nativeToLittle(i64, v),
                ));
                fixed_offset += 8;
            },
            .int => {
                const v = values[i].int;
                @memcpy(buf[fixed_offset..][0..4], std.mem.asBytes(
                    &std.mem.nativeToLittle(i32, v),
                ));
                fixed_offset += 4;
            },
            .float => {
                const v = values[i].float;
                const bits = @as(u64, @bitCast(v));
                @memcpy(buf[fixed_offset..][0..8], std.mem.asBytes(
                    &std.mem.nativeToLittle(u64, bits),
                ));
                fixed_offset += 8;
            },
            .boolean => {
                buf[fixed_offset] = if (values[i].boolean) 1 else 0;
                fixed_offset += 1;
            },
            .string => {
                const str = values[i].string;
                const overflow_page_id = if (string_overflow_page_ids) |ids|
                    ids[i]
                else
                    0;
                if (overflow_page_id == 0) {
                    const str_len: u16 = @intCast(str.len);
                    buf[fixed_offset] = string_slot_inline_tag;
                    const off_u16: u16 = @intCast(var_offset);
                    @memcpy(buf[fixed_offset + 2 ..][0..2], std.mem.asBytes(
                        &std.mem.nativeToLittle(u16, off_u16),
                    ));
                    @memcpy(buf[var_offset..][0..2], std.mem.asBytes(
                        &std.mem.nativeToLittle(u16, str_len),
                    ));
                    var_offset += 2;
                    @memcpy(buf[var_offset..][0..str.len], str);
                    var_offset += str.len;
                } else {
                    buf[fixed_offset] = string_slot_overflow_tag;
                    @memcpy(buf[fixed_offset + 2 ..][0..8], std.mem.asBytes(
                        &std.mem.nativeToLittle(u64, overflow_page_id),
                    ));
                }
                fixed_offset += string_fixed_slot_size;
            },
            .timestamp => {
                const v = values[i].timestamp;
                @memcpy(buf[fixed_offset..][0..8], std.mem.asBytes(
                    &std.mem.nativeToLittle(i64, v),
                ));
                fixed_offset += 8;
            },
        }
    }

    return @intCast(total);
}

/// Decode a single column value from encoded row data.
pub fn decodeColumnStorageChecked(
    schema: *const RowSchema,
    row_data: []const u8,
    col_index: u16,
) DecodeError!DecodedColumn {
    if (col_index >= schema.column_count) return error.Corruption;
    const row_version = try validateRowHeader(row_data);

    const byte_idx = row_header_size + col_index / 8;
    try requireRange(row_data, byte_idx, 1);
    const bit_idx: u3 = @intCast(col_index % 8);
    if (row_data[byte_idx] & (@as(u8, 1) << bit_idx) != 0) {
        return .{ .value = .{ .null_value = {} } };
    }

    var offset: usize = row_header_size + schema.null_bitmap_bytes;
    for (0..col_index) |i| {
        const col = schema.columns[i];
        const slot_size: usize = if (col.column_type == .string)
            stringSlotSizeForVersion(row_version)
        else
            fixedSize(col.column_type);
        offset = std.math.add(usize, offset, slot_size) catch
            return error.Corruption;
    }

    const col = schema.columns[col_index];
    return switch (col.column_type) {
        .bigint => blk: {
            try requireRange(row_data, offset, 8);
            break :blk .{
                .value = .{
                    .bigint = std.mem.littleToNative(
                        i64,
                        std.mem.bytesAsValue(i64, row_data[offset..][0..8]).*,
                    ),
                },
            };
        },
        .int => blk: {
            try requireRange(row_data, offset, 4);
            break :blk .{
                .value = .{
                    .int = std.mem.littleToNative(
                        i32,
                        std.mem.bytesAsValue(i32, row_data[offset..][0..4]).*,
                    ),
                },
            };
        },
        .float => blk: {
            try requireRange(row_data, offset, 8);
            break :blk .{
                .value = .{
                    .float = @bitCast(std.mem.littleToNative(
                        u64,
                        std.mem.bytesAsValue(u64, row_data[offset..][0..8]).*,
                    )),
                },
            };
        },
        .boolean => blk: {
            try requireRange(row_data, offset, 1);
            break :blk .{ .value = .{ .boolean = row_data[offset] != 0 } };
        },
        .string => blk: {
            if (row_version == row_format_version_legacy) {
                try requireRange(row_data, offset, 2);
                const str_offset = std.mem.littleToNative(
                    u16,
                    std.mem.bytesAsValue(u16, row_data[offset..][0..2]).*,
                );
                const str_offset_usize: usize = str_offset;
                try requireRange(row_data, str_offset_usize, 2);
                const str_len = std.mem.littleToNative(
                    u16,
                    std.mem.bytesAsValue(u16, row_data[str_offset_usize..][0..2]).*,
                );
                const str_data_start = std.math.add(usize, str_offset_usize, 2) catch
                    return error.Corruption;
                try requireRange(row_data, str_data_start, str_len);
                break :blk .{
                    .value = .{ .string = row_data[str_data_start..][0..str_len] },
                };
            }

            try requireRange(row_data, offset, string_fixed_slot_size);
            const tag = row_data[offset];
            if (tag == string_slot_inline_tag) {
                const str_offset = std.mem.littleToNative(
                    u16,
                    std.mem.bytesAsValue(u16, row_data[offset + 2 ..][0..2]).*,
                );
                const str_offset_usize: usize = str_offset;
                try requireRange(row_data, str_offset_usize, 2);
                const str_len = std.mem.littleToNative(
                    u16,
                    std.mem.bytesAsValue(u16, row_data[str_offset_usize..][0..2]).*,
                );
                const str_data_start = std.math.add(usize, str_offset_usize, 2) catch
                    return error.Corruption;
                try requireRange(row_data, str_data_start, str_len);
                break :blk .{
                    .value = .{ .string = row_data[str_data_start..][0..str_len] },
                };
            }
            if (tag == string_slot_overflow_tag) {
                const overflow_page_id = std.mem.littleToNative(
                    u64,
                    std.mem.bytesAsValue(u64, row_data[offset + 2 ..][0..8]).*,
                );
                if (overflow_page_id == 0) return error.Corruption;
                break :blk .{ .string_overflow_page_id = overflow_page_id };
            }
            return error.Corruption;
        },
        .timestamp => blk: {
            try requireRange(row_data, offset, 8);
            break :blk .{
                .value = .{
                    .timestamp = std.mem.littleToNative(
                        i64,
                        std.mem.bytesAsValue(i64, row_data[offset..][0..8]).*,
                    ),
                },
            };
        },
    };
}

/// Decode a single column value from encoded row data.
pub fn decodeColumnChecked(
    schema: *const RowSchema,
    row_data: []const u8,
    col_index: u16,
) DecodeError!Value {
    const decoded = try decodeColumnStorageChecked(schema, row_data, col_index);
    return switch (decoded) {
        .value => |v| v,
        .string_overflow_page_id => error.Corruption,
    };
}

/// Decode all columns from encoded row data into a caller-provided array.
pub fn decodeRowChecked(
    schema: *const RowSchema,
    row_data: []const u8,
    out: []Value,
) DecodeError!void {
    std.debug.assert(out.len >= schema.column_count);
    for (0..schema.column_count) |i| {
        out[i] = try decodeColumnChecked(schema, row_data, @intCast(i));
    }
}

/// Backwards-compatible wrapper for callers that expect infallible decode.
pub fn decodeColumn(schema: *const RowSchema, row_data: []const u8, col_index: u16) Value {
    return decodeColumnChecked(schema, row_data, col_index) catch
        @panic("row decode corruption");
}

/// Backwards-compatible wrapper for callers that expect infallible decode.
pub fn decodeRow(schema: *const RowSchema, row_data: []const u8, out: []Value) void {
    decodeRowChecked(schema, row_data, out) catch
        @panic("row decode corruption");
}

fn requireRange(row_data: []const u8, start: usize, len: usize) DecodeError!void {
    const end = std.math.add(usize, start, len) catch return error.Corruption;
    if (end > row_data.len) return error.Corruption;
}

fn validateRowHeader(row_data: []const u8) DecodeError!u8 {
    try requireRange(row_data, 0, row_header_size);
    const magic = std.mem.littleToNative(
        u16,
        std.mem.bytesAsValue(u16, row_data[0..2]).*,
    );
    if (magic != row_format_magic) return error.InvalidRowFormat;
    const version = row_data[2];
    if (version != row_format_version and version != row_format_version_legacy) {
        return error.UnsupportedRowVersion;
    }
    return version;
}

fn stringSlotSizeForVersion(version: u8) usize {
    return if (version == row_format_version_legacy)
        string_fixed_slot_size_legacy
    else
        string_fixed_slot_size;
}

// --- Tests ---

const testing = std.testing;

test "roundtrip bigint column" {
    var schema = RowSchema{};
    _ = try schema.addColumn("id", .bigint, false);

    const values = [_]Value{.{ .bigint = 42 }};
    var buf: [256]u8 = undefined;
    const written = try encodeRow(&schema, &values, &buf);

    const decoded = decodeColumn(&schema, buf[0..written], 0);
    try testing.expectEqual(@as(i64, 42), decoded.bigint);
}

test "row encode/decode matches golden vector" {
    var schema = RowSchema{};
    _ = try schema.addColumn("id", .bigint, false);
    _ = try schema.addColumn("name", .string, false);
    _ = try schema.addColumn("active", .boolean, false);

    const values = [_]Value{
        .{ .bigint = 42 },
        .{ .string = "Bob" },
        .{ .boolean = true },
    };
    const expected = [_]u8{
        0x32, 0x52, 0x02, 0x00, 0x2A, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x17, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x03,
        0x00, 0x42, 0x6F, 0x62,
    };

    var buf: [256]u8 = undefined;
    const written = try encodeRow(&schema, &values, &buf);
    try testing.expectEqual(@as(u16, expected.len), written);
    try testing.expectEqualSlices(u8, &expected, buf[0..written]);

    var decoded: [3]Value = undefined;
    try decodeRowChecked(&schema, &expected, &decoded);
    try testing.expectEqual(@as(i64, 42), decoded[0].bigint);
    try testing.expectEqualSlices(u8, "Bob", decoded[1].string);
    try testing.expect(decoded[2].boolean);
}

test "roundtrip int column" {
    var schema = RowSchema{};
    _ = try schema.addColumn("count", .int, false);

    const values = [_]Value{.{ .int = -100 }};
    var buf: [256]u8 = undefined;
    const written = try encodeRow(&schema, &values, &buf);

    const decoded = decodeColumn(&schema, buf[0..written], 0);
    try testing.expectEqual(@as(i32, -100), decoded.int);
}

test "roundtrip float column" {
    var schema = RowSchema{};
    _ = try schema.addColumn("score", .float, false);

    const values = [_]Value{.{ .float = 3.14 }};
    var buf: [256]u8 = undefined;
    const written = try encodeRow(&schema, &values, &buf);

    const decoded = decodeColumn(&schema, buf[0..written], 0);
    try testing.expectEqual(@as(f64, 3.14), decoded.float);
}

test "roundtrip boolean column" {
    var schema = RowSchema{};
    _ = try schema.addColumn("active", .boolean, false);

    const values_true = [_]Value{.{ .boolean = true }};
    var buf: [256]u8 = undefined;
    const w1 = try encodeRow(&schema, &values_true, &buf);
    try testing.expect(decodeColumn(&schema, buf[0..w1], 0).boolean);

    const values_false = [_]Value{.{ .boolean = false }};
    const w2 = try encodeRow(&schema, &values_false, &buf);
    try testing.expect(!decodeColumn(&schema, buf[0..w2], 0).boolean);
}

test "roundtrip string column" {
    var schema = RowSchema{};
    _ = try schema.addColumn("name", .string, false);

    const values = [_]Value{.{ .string = "hello" }};
    var buf: [256]u8 = undefined;
    const written = try encodeRow(&schema, &values, &buf);

    const decoded = decodeColumn(&schema, buf[0..written], 0);
    try testing.expectEqualSlices(u8, "hello", decoded.string);
}

test "roundtrip timestamp column" {
    var schema = RowSchema{};
    _ = try schema.addColumn("created_at", .timestamp, false);

    const values = [_]Value{.{ .timestamp = 1700000000 }};
    var buf: [256]u8 = undefined;
    const written = try encodeRow(&schema, &values, &buf);

    const decoded = decodeColumn(&schema, buf[0..written], 0);
    try testing.expectEqual(@as(i64, 1700000000), decoded.timestamp);
}

test "null handling" {
    var schema = RowSchema{};
    _ = try schema.addColumn("id", .bigint, false);
    _ = try schema.addColumn("name", .string, true);
    _ = try schema.addColumn("score", .float, true);

    const values = [_]Value{
        .{ .bigint = 1 },
        .{ .null_value = {} },
        .{ .float = 9.5 },
    };
    var buf: [256]u8 = undefined;
    const written = try encodeRow(&schema, &values, &buf);

    const v0 = decodeColumn(&schema, buf[0..written], 0);
    try testing.expectEqual(@as(i64, 1), v0.bigint);
    const v1 = decodeColumn(&schema, buf[0..written], 1);
    try testing.expect(v1 == .null_value);
    const v2 = decodeColumn(&schema, buf[0..written], 2);
    try testing.expectEqual(@as(f64, 9.5), v2.float);
}

test "null not allowed returns error" {
    var schema = RowSchema{};
    _ = try schema.addColumn("id", .bigint, false);

    const values = [_]Value{.{ .null_value = {} }};
    var buf: [256]u8 = undefined;
    try testing.expectError(error.NullNotAllowed, encodeRow(&schema, &values, &buf));
}

test "type mismatch returns error" {
    var schema = RowSchema{};
    _ = try schema.addColumn("id", .bigint, false);

    const values = [_]Value{.{ .int = 5 }};
    var buf: [256]u8 = undefined;
    try testing.expectError(error.TypeMismatch, encodeRow(&schema, &values, &buf));
}

test "mixed schema roundtrip" {
    var schema = RowSchema{};
    _ = try schema.addColumn("id", .bigint, false);
    _ = try schema.addColumn("name", .string, false);
    _ = try schema.addColumn("active", .boolean, false);
    _ = try schema.addColumn("email", .string, true);
    _ = try schema.addColumn("score", .float, true);

    const values = [_]Value{
        .{ .bigint = 42 },
        .{ .string = "Alice" },
        .{ .boolean = true },
        .{ .string = "alice@example.com" },
        .{ .float = 99.5 },
    };
    var buf: [512]u8 = undefined;
    const written = try encodeRow(&schema, &values, &buf);

    try testing.expectEqual(@as(i64, 42), decodeColumn(&schema, buf[0..written], 0).bigint);
    try testing.expectEqualSlices(u8, "Alice", decodeColumn(&schema, buf[0..written], 1).string);
    try testing.expect(decodeColumn(&schema, buf[0..written], 2).boolean);
    try testing.expectEqualSlices(u8, "alice@example.com", decodeColumn(&schema, buf[0..written], 3).string);
    try testing.expectEqual(@as(f64, 99.5), decodeColumn(&schema, buf[0..written], 4).float);
}

test "compareValues ordering" {
    // Same type comparisons.
    try testing.expectEqual(std.math.Order.lt, compareValues(
        .{ .bigint = 1 },
        .{ .bigint = 2 },
    ));
    try testing.expectEqual(std.math.Order.eq, compareValues(
        .{ .bigint = 5 },
        .{ .bigint = 5 },
    ));
    try testing.expectEqual(std.math.Order.gt, compareValues(
        .{ .bigint = 10 },
        .{ .bigint = 3 },
    ));

    // String comparison.
    try testing.expectEqual(std.math.Order.lt, compareValues(
        .{ .string = "apple" },
        .{ .string = "banana" },
    ));

    // Null sorts last.
    try testing.expectEqual(std.math.Order.gt, compareValues(
        .{ .null_value = {} },
        .{ .bigint = 1 },
    ));
    try testing.expectEqual(std.math.Order.lt, compareValues(
        .{ .bigint = 1 },
        .{ .null_value = {} },
    ));
    try testing.expectEqual(std.math.Order.eq, compareValues(
        .{ .null_value = {} },
        .{ .null_value = {} },
    ));
}

test "decodeRow decodes all columns" {
    var schema = RowSchema{};
    _ = try schema.addColumn("a", .int, false);
    _ = try schema.addColumn("b", .boolean, false);

    const values = [_]Value{ .{ .int = 7 }, .{ .boolean = true } };
    var buf: [256]u8 = undefined;
    const written = try encodeRow(&schema, &values, &buf);

    var out: [2]Value = undefined;
    decodeRow(&schema, buf[0..written], &out);
    try testing.expectEqual(@as(i32, 7), out[0].int);
    try testing.expect(out[1].boolean);
}

test "findColumn looks up by name" {
    var schema = RowSchema{};
    _ = try schema.addColumn("id", .bigint, false);
    _ = try schema.addColumn("name", .string, false);

    try testing.expectEqual(@as(?u16, 0), schema.findColumn("id"));
    try testing.expectEqual(@as(?u16, 1), schema.findColumn("name"));
    try testing.expect(schema.findColumn("missing") == null);
}

test "empty string roundtrip" {
    var schema = RowSchema{};
    _ = try schema.addColumn("val", .string, false);

    const values = [_]Value{.{ .string = "" }};
    var buf: [256]u8 = undefined;
    const written = try encodeRow(&schema, &values, &buf);

    const decoded = decodeColumn(&schema, buf[0..written], 0);
    try testing.expectEqualSlices(u8, "", decoded.string);
}

test "addColumn returns NameBufferFull when name buffer is exhausted" {
    var schema = RowSchema{};
    var long_name = [_]u8{'x'} ** 4096;
    _ = try schema.addColumn(long_name[0..], .int, false);

    const result = schema.addColumn("y", .int, false);
    try testing.expectError(error.NameBufferFull, result);
}

test "decode rejects unsupported row format version" {
    var schema = RowSchema{};
    _ = try schema.addColumn("id", .bigint, false);

    const values = [_]Value{.{ .bigint = 42 }};
    var buf: [256]u8 = undefined;
    const written = try encodeRow(&schema, &values, &buf);
    buf[2] = row_format_version + 1;

    const result = decodeColumnChecked(&schema, buf[0..written], 0);
    try testing.expectError(error.UnsupportedRowVersion, result);
}

test "decode rejects invalid row format magic" {
    var schema = RowSchema{};
    _ = try schema.addColumn("id", .bigint, false);

    const values = [_]Value{.{ .bigint = 42 }};
    var buf: [256]u8 = undefined;
    const written = try encodeRow(&schema, &values, &buf);
    @memset(buf[0..2], 0);

    const result = decodeColumnChecked(&schema, buf[0..written], 0);
    try testing.expectError(error.InvalidRowFormat, result);
}

test "encode/decode string overflow pointer slot" {
    var schema = RowSchema{};
    _ = try schema.addColumn("id", .bigint, false);
    _ = try schema.addColumn("name", .string, false);

    const values = [_]Value{
        .{ .bigint = 1 },
        .{ .string = "large payload stored in overflow pages" },
    };
    const overflow_ids = [_]u64{ 0, 10_000_123 };

    var buf: [256]u8 = undefined;
    const written = try encodeRowWithOverflow(&schema, &values, &overflow_ids, &buf);
    const decoded_id = try decodeColumnStorageChecked(&schema, buf[0..written], 1);
    try testing.expect(decoded_id == .string_overflow_page_id);
    try testing.expectEqual(@as(u64, 10_000_123), decoded_id.string_overflow_page_id);
}

test "decode supports legacy v1 row format for inline string" {
    var schema = RowSchema{};
    _ = try schema.addColumn("name", .string, false);

    // [magic:u16][version:u8][null_bitmap:u8][offset:u16][len:u16]["ok"]
    const legacy_row = [_]u8{ 0x32, 0x52, 0x01, 0x00, 0x06, 0x00, 0x02, 0x00, 0x6F, 0x6B };
    const decoded = try decodeColumnChecked(&schema, &legacy_row, 0);
    try testing.expectEqualSlices(u8, "ok", decoded.string);
}
