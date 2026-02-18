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
        .null_value => unreachable, // handled above
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
    ) error{TooManyColumns}!u16 {
        if (self.column_count >= max_columns) return error.TooManyColumns;
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

/// Encode a row of values into a byte buffer.
///
/// Layout:
///   [null_bitmap: N bytes] [fixed columns in order] [variable-length data]
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
    std.debug.assert(values.len == schema.column_count);

    // First pass: compute total size needed.
    var total: usize = schema.null_bitmap_bytes;
    for (0..schema.column_count) |i| {
        const col = schema.columns[i];
        if (values[i] == .null_value) {
            if (!col.nullable) return error.NullNotAllowed;
            // Null columns still occupy their fixed slot (zeroed).
            total += if (col.column_type == .string) 2 else fixedSize(col.column_type);
        } else {
            if (values[i].columnType()) |vt| {
                if (vt != col.column_type) return error.TypeMismatch;
            }
            if (col.column_type == .string) {
                // Fixed slot: 2 bytes (offset). Var data: 2 + string len.
                total += 2; // offset in fixed area
                total += 2 + values[i].string.len; // length prefix + data
            } else {
                total += fixedSize(col.column_type);
            }
        }
    }

    if (total > buf.len) return error.BufferTooSmall;
    if (total > std.math.maxInt(u16)) return error.BufferTooSmall;

    // Zero the buffer region.
    @memset(buf[0..total], 0);

    // Write null bitmap.
    for (0..schema.column_count) |i| {
        if (values[i] == .null_value) {
            buf[i / 8] |= @as(u8, 1) << @intCast(i % 8);
        }
    }

    // Second pass: write fixed columns, track variable data offset.
    var fixed_offset: usize = schema.null_bitmap_bytes;
    // Variable data starts after all fixed columns.
    var var_data_start: usize = schema.null_bitmap_bytes;
    for (0..schema.column_count) |i| {
        const col = schema.columns[i];
        if (col.column_type == .string) {
            var_data_start += 2; // offset slot
        } else {
            var_data_start += fixedSize(col.column_type);
        }
    }
    var var_offset: usize = var_data_start;

    for (0..schema.column_count) |i| {
        const col = schema.columns[i];
        if (values[i] == .null_value) {
            // Leave zeroed.
            if (col.column_type == .string) {
                fixed_offset += 2;
            } else {
                fixed_offset += fixedSize(col.column_type);
            }
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
                const str_len: u16 = @intCast(str.len);
                // Write offset into fixed slot.
                const off_u16: u16 = @intCast(var_offset);
                @memcpy(buf[fixed_offset..][0..2], std.mem.asBytes(
                    &std.mem.nativeToLittle(u16, off_u16),
                ));
                fixed_offset += 2;
                // Write length-prefixed string data.
                @memcpy(buf[var_offset..][0..2], std.mem.asBytes(
                    &std.mem.nativeToLittle(u16, str_len),
                ));
                var_offset += 2;
                @memcpy(buf[var_offset..][0..str.len], str);
                var_offset += str.len;
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
pub fn decodeColumn(
    schema: *const RowSchema,
    row_data: []const u8,
    col_index: u16,
) Value {
    std.debug.assert(col_index < schema.column_count);

    // Check null bitmap.
    const byte_idx = col_index / 8;
    const bit_idx: u3 = @intCast(col_index % 8);
    if (row_data[byte_idx] & (@as(u8, 1) << bit_idx) != 0) {
        return .{ .null_value = {} };
    }

    // Compute offset to this column's fixed slot.
    var offset: usize = schema.null_bitmap_bytes;
    for (0..col_index) |i| {
        const col = schema.columns[i];
        if (col.column_type == .string) {
            offset += 2;
        } else {
            offset += fixedSize(col.column_type);
        }
    }

    const col = schema.columns[col_index];
    return switch (col.column_type) {
        .bigint => .{
            .bigint = std.mem.littleToNative(
                i64,
                std.mem.bytesAsValue(i64, row_data[offset..][0..8]).*,
            ),
        },
        .int => .{
            .int = std.mem.littleToNative(
                i32,
                std.mem.bytesAsValue(i32, row_data[offset..][0..4]).*,
            ),
        },
        .float => .{
            .float = @bitCast(std.mem.littleToNative(
                u64,
                std.mem.bytesAsValue(u64, row_data[offset..][0..8]).*,
            )),
        },
        .boolean => .{ .boolean = row_data[offset] != 0 },
        .string => blk: {
            const str_offset = std.mem.littleToNative(
                u16,
                std.mem.bytesAsValue(u16, row_data[offset..][0..2]).*,
            );
            const str_len = std.mem.littleToNative(
                u16,
                std.mem.bytesAsValue(u16, row_data[str_offset..][0..2]).*,
            );
            break :blk .{
                .string = row_data[str_offset + 2 ..][0..str_len],
            };
        },
        .timestamp => .{
            .timestamp = std.mem.littleToNative(
                i64,
                std.mem.bytesAsValue(i64, row_data[offset..][0..8]).*,
            ),
        },
    };
}

/// Decode all columns from encoded row data into a caller-provided array.
pub fn decodeRow(
    schema: *const RowSchema,
    row_data: []const u8,
    out: []Value,
) void {
    std.debug.assert(out.len >= schema.column_count);
    for (0..schema.column_count) |i| {
        out[i] = decodeColumn(schema, row_data, @intCast(i));
    }
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
