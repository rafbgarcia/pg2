//! Compact row serialization for temporary spill pages.
//!
//! Responsibilities in this file:
//! - Defines a self-describing, compact wire format for serializing `ResultRow`
//!   data into temp page payloads for query-scoped spill operations.
//! - Provides row-level encode/decode (`encodeSpillRow`, `decodeSpillRow`) and
//!   a pure size computation (`spillRowSize`) for byte-budget tracking.
//! - Provides page-level packing (`SpillPageWriter`) and sequential reading
//!   (`SpillPageReader`) to pack multiple rows per 8 KB temp page.
//!
//! Why this exists:
//! - The B-tree row format (`row.zig:encodeRow`) is designed for heap pages with
//!   random access (10-byte string slot indirection, per-row magic/version header,
//!   schema-dependent decode). Spill pages need a simpler format optimized for
//!   sequential bulk write and read-back.
//! - Self-describing type tags (1 byte per non-null column) make the format
//!   independent of schema context, which matters for projected/computed columns.
//!
//! How it works:
//! - Each row is serialized as: [row_data_len:u16][column_count:u8][null_bitmap]
//!   followed by [type_tag:u8][payload] pairs for each non-null column.
//! - Multiple serialized rows are packed into a temp page payload with a
//!   [row_count:u16] header. The temp page's own header (magic, version,
//!   next_page_id, payload_len, checksum) handles framing.
//! - Strings are stored inline (u16 length prefix + data). No overflow chains.
//!
//! Boundaries and non-responsibilities:
//! - This file does not perform I/O. Page read/write goes through `TempStorageManager`.
//! - This file does not decide when to spill. That is the `SpillingResultCollector`'s job.
//! - No WAL logging, no durability — temp pages are ephemeral.
//!
//! Contributor notes:
//! - The per-row layout is a format contract for the spill path. Changes require
//!   bumping the temp page format version.
//! - Encoding conventions match `row.zig`: little-endian, f64 via @bitCast to u64,
//!   CRC-32 ISCSI at the page level (handled by `Page.serialize()`).
//! - Null bitmap uses LSB-first ordering within each byte (bit=1 means null),
//!   matching the heap row format convention.
const std = @import("std");
const row_mod = @import("row.zig");
const temp_mod = @import("temp.zig");
const scan_mod = @import("../executor/scan.zig");
const heap_mod = @import("heap.zig");

const Value = row_mod.Value;
const ColumnType = row_mod.ColumnType;
const ResultRow = scan_mod.ResultRow;
const StringArena = scan_mod.StringArena;
const TempPage = temp_mod.TempPage;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Bytes used by the per-page row_count header.
const page_row_count_size: usize = 2;

/// Maximum temp page payload bytes available for row data.
pub const max_payload: usize = TempPage.max_payload_len();

/// Maximum columns per row (must match scan.zig).
const max_columns: usize = scan_mod.max_columns;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

pub const SpillError = error{
    /// Output buffer is too small for the serialized row.
    BufferTooSmall,
    /// A single serialized row exceeds the temp page payload capacity.
    RowTooLargeForSpill,
    /// Row has zero columns.
    InvalidColumnCount,
    /// Encountered an unrecognized type tag during decode.
    InvalidColumnType,
    /// Input data is truncated; expected more bytes.
    UnexpectedEndOfData,
    /// String arena is full during decode.
    OutOfMemory,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Number of bytes needed for the null bitmap given `column_count` columns.
fn nullBitmapBytes(column_count: u8) u8 {
    return @intCast((@as(u16, column_count) + 7) / 8);
}

/// Byte size of a value payload for a given column type tag.
/// Strings are variable-length and not covered by this function.
fn fixedPayloadSize(col_type: ColumnType) u8 {
    return switch (col_type) {
        .i8, .u8, .bool => 1,
        .i16, .u16 => 2,
        .i32, .u32 => 4,
        .i64, .u64, .f64, .timestamp => 8,
        .string => 0, // variable; handled separately
    };
}

/// Convert a Value's active tag to its ColumnType, or null for null_value.
fn valueColumnType(v: Value) ?ColumnType {
    return v.columnType();
}

// ---------------------------------------------------------------------------
// Size computation
// ---------------------------------------------------------------------------

/// Compute the serialized byte size of a row without writing anything.
///
/// The returned size includes the row_data_len prefix (2 bytes).
/// Returns `RowTooLargeForSpill` if the result would exceed temp page capacity.
/// Returns `InvalidColumnCount` if column_count is 0.
pub fn spillRowSize(row: *const ResultRow) SpillError!u16 {
    const col_count = row.column_count;
    if (col_count == 0) return error.InvalidColumnCount;

    const col_count_u8: u8 = @intCast(col_count);
    // row_data_len(2) + column_count(1) + null_bitmap
    var size: usize = 2 + 1 + nullBitmapBytes(col_count_u8);

    for (0..col_count) |i| {
        const v = row.values[i];
        const col_type = valueColumnType(v) orelse continue; // null: skip
        size += 1; // type_tag
        if (col_type == .string) {
            size += 2 + v.string.len; // u16 length + data
        } else {
            size += fixedPayloadSize(col_type);
        }
    }

    if (size > max_payload - page_row_count_size) return error.RowTooLargeForSpill;
    return @intCast(size);
}

// ---------------------------------------------------------------------------
// Row-level encode
// ---------------------------------------------------------------------------

/// Serialize a ResultRow into `buf`. Returns the number of bytes written.
///
/// Layout:
///   [row_data_len:u16 LE][column_count:u8][null_bitmap]
///   For each non-null column: [type_tag:u8][payload bytes]
///
/// The row_data_len field stores the byte count *after* itself (i.e. total - 2).
pub fn encodeSpillRow(row: *const ResultRow, buf: []u8) SpillError!u16 {
    const total_size = try spillRowSize(row);
    if (buf.len < total_size) return error.BufferTooSmall;

    const col_count: u8 = @intCast(row.column_count);
    const bitmap_bytes = nullBitmapBytes(col_count);
    const data_len: u16 = total_size - 2; // everything after row_data_len

    var offset: usize = 0;

    // row_data_len
    @memcpy(buf[offset..][0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, data_len)));
    offset += 2;

    // column_count
    buf[offset] = col_count;
    offset += 1;

    // null_bitmap — zero first, then set bits
    @memset(buf[offset..][0..bitmap_bytes], 0);
    for (0..col_count) |i| {
        if (row.values[i] == .null_value) {
            buf[offset + i / 8] |= @as(u8, 1) << @intCast(i % 8);
        }
    }
    offset += bitmap_bytes;

    // column values
    for (0..col_count) |i| {
        const v = row.values[i];
        const col_type = valueColumnType(v) orelse continue; // null: skip

        // type_tag
        buf[offset] = @intFromEnum(col_type);
        offset += 1;

        // payload
        switch (col_type) {
            .i8 => {
                buf[offset] = @bitCast(v.i8);
                offset += 1;
            },
            .u8 => {
                buf[offset] = v.u8;
                offset += 1;
            },
            .bool => {
                buf[offset] = if (v.bool) 1 else 0;
                offset += 1;
            },
            .i16 => {
                @memcpy(buf[offset..][0..2], std.mem.asBytes(&std.mem.nativeToLittle(i16, v.i16)));
                offset += 2;
            },
            .u16 => {
                @memcpy(buf[offset..][0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, v.u16)));
                offset += 2;
            },
            .i32 => {
                @memcpy(buf[offset..][0..4], std.mem.asBytes(&std.mem.nativeToLittle(i32, v.i32)));
                offset += 4;
            },
            .u32 => {
                @memcpy(buf[offset..][0..4], std.mem.asBytes(&std.mem.nativeToLittle(u32, v.u32)));
                offset += 4;
            },
            .i64 => {
                @memcpy(buf[offset..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(i64, v.i64)));
                offset += 8;
            },
            .u64 => {
                @memcpy(buf[offset..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, v.u64)));
                offset += 8;
            },
            .f64 => {
                const bits: u64 = @bitCast(v.f64);
                @memcpy(buf[offset..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, bits)));
                offset += 8;
            },
            .timestamp => {
                @memcpy(buf[offset..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(i64, v.timestamp)));
                offset += 8;
            },
            .string => {
                const str = v.string;
                const str_len: u16 = @intCast(str.len);
                @memcpy(buf[offset..][0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, str_len)));
                offset += 2;
                if (str.len > 0) {
                    @memcpy(buf[offset..][0..str.len], str);
                    offset += str.len;
                }
            },
        }
    }

    std.debug.assert(offset == total_size);
    return total_size;
}

// ---------------------------------------------------------------------------
// Row-level decode
// ---------------------------------------------------------------------------

/// Deserialize a row from `data` into `out`. Returns the number of bytes consumed.
///
/// String values are copied into `arena`; the returned ResultRow's string slices
/// point into arena memory (not into `data`), so `data` can be reused after decode.
pub fn decodeSpillRow(data: []const u8, out: *ResultRow, arena: *StringArena) SpillError!u16 {
    if (data.len < 3) return error.UnexpectedEndOfData; // need at least row_data_len + column_count

    // row_data_len
    const data_len = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, data[0..2]).*);
    const total_size: usize = @as(usize, data_len) + 2;
    if (data.len < total_size) return error.UnexpectedEndOfData;

    var offset: usize = 2;

    // column_count
    const col_count = data[offset];
    if (col_count == 0) return error.InvalidColumnCount;
    if (col_count > max_columns) return error.InvalidColumnCount;
    offset += 1;

    const bitmap_bytes = nullBitmapBytes(col_count);
    if (offset + bitmap_bytes > total_size) return error.UnexpectedEndOfData;
    const bitmap = data[offset..][0..bitmap_bytes];
    offset += bitmap_bytes;

    // Reset output row.
    out.* = ResultRow.init();
    out.column_count = col_count;

    for (0..col_count) |i| {
        // Check null bitmap.
        const is_null = (bitmap[i / 8] & (@as(u8, 1) << @intCast(i % 8))) != 0;
        if (is_null) {
            out.values[i] = .{ .null_value = {} };
            continue;
        }

        // type_tag
        if (offset >= total_size) return error.UnexpectedEndOfData;
        const tag_byte = data[offset];
        offset += 1;

        const col_type = std.meta.intToEnum(ColumnType, tag_byte) catch
            return error.InvalidColumnType;

        switch (col_type) {
            .i8 => {
                if (offset + 1 > total_size) return error.UnexpectedEndOfData;
                out.values[i] = .{ .i8 = @bitCast(data[offset]) };
                offset += 1;
            },
            .u8 => {
                if (offset + 1 > total_size) return error.UnexpectedEndOfData;
                out.values[i] = .{ .u8 = data[offset] };
                offset += 1;
            },
            .bool => {
                if (offset + 1 > total_size) return error.UnexpectedEndOfData;
                out.values[i] = .{ .bool = data[offset] != 0 };
                offset += 1;
            },
            .i16 => {
                if (offset + 2 > total_size) return error.UnexpectedEndOfData;
                out.values[i] = .{ .i16 = std.mem.littleToNative(i16, std.mem.bytesAsValue(i16, data[offset..][0..2]).*) };
                offset += 2;
            },
            .u16 => {
                if (offset + 2 > total_size) return error.UnexpectedEndOfData;
                out.values[i] = .{ .u16 = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, data[offset..][0..2]).*) };
                offset += 2;
            },
            .i32 => {
                if (offset + 4 > total_size) return error.UnexpectedEndOfData;
                out.values[i] = .{ .i32 = std.mem.littleToNative(i32, std.mem.bytesAsValue(i32, data[offset..][0..4]).*) };
                offset += 4;
            },
            .u32 => {
                if (offset + 4 > total_size) return error.UnexpectedEndOfData;
                out.values[i] = .{ .u32 = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, data[offset..][0..4]).*) };
                offset += 4;
            },
            .i64 => {
                if (offset + 8 > total_size) return error.UnexpectedEndOfData;
                out.values[i] = .{ .i64 = std.mem.littleToNative(i64, std.mem.bytesAsValue(i64, data[offset..][0..8]).*) };
                offset += 8;
            },
            .u64 => {
                if (offset + 8 > total_size) return error.UnexpectedEndOfData;
                out.values[i] = .{ .u64 = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, data[offset..][0..8]).*) };
                offset += 8;
            },
            .f64 => {
                if (offset + 8 > total_size) return error.UnexpectedEndOfData;
                const bits = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, data[offset..][0..8]).*);
                out.values[i] = .{ .f64 = @bitCast(bits) };
                offset += 8;
            },
            .timestamp => {
                if (offset + 8 > total_size) return error.UnexpectedEndOfData;
                out.values[i] = .{ .timestamp = std.mem.littleToNative(i64, std.mem.bytesAsValue(i64, data[offset..][0..8]).*) };
                offset += 8;
            },
            .string => {
                if (offset + 2 > total_size) return error.UnexpectedEndOfData;
                const str_len = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, data[offset..][0..2]).*);
                offset += 2;
                if (offset + str_len > total_size) return error.UnexpectedEndOfData;
                const str_data = data[offset..][0..str_len];
                offset += str_len;
                // Copy into arena so the slice survives after data buffer is reused.
                const arena_copy = arena.copyString(str_data) catch return error.OutOfMemory;
                out.values[i] = .{ .string = arena_copy };
            },
        }
    }

    std.debug.assert(offset == total_size);
    return @intCast(total_size);
}

// ---------------------------------------------------------------------------
// SpillPageWriter — packs multiple rows into a temp page payload
// ---------------------------------------------------------------------------

/// Accumulates serialized rows into a buffer sized for one temp page payload.
///
/// Usage:
///   var writer = SpillPageWriter.init();
///   while (has_rows) {
///       const ok = try writer.appendRow(&row);
///       if (!ok) {
///           const payload = writer.finalize();
///           // write payload to temp page via TempStorageManager
///           writer.reset();
///           _ = try writer.appendRow(&row); // retry after flush
///       }
///   }
///   if (writer.row_count > 0) {
///       const payload = writer.finalize();
///       // write final page
///   }
pub const SpillPageWriter = struct {
    buf: [max_payload]u8,
    offset: u16,
    row_count: u16,

    pub fn init() SpillPageWriter {
        return .{
            .buf = undefined,
            .offset = page_row_count_size, // reserve space for row_count header
            .row_count = 0,
        };
    }

    /// Try to append a serialized row. Returns `true` on success, `false` if the
    /// page has insufficient space (caller should finalize, flush, reset, and retry).
    /// Returns an error only for malformed rows (e.g. zero columns).
    pub fn appendRow(self: *SpillPageWriter, row: *const ResultRow) SpillError!bool {
        const row_size = try spillRowSize(row);
        if (@as(usize, self.offset) + row_size > max_payload) return false;

        const bytes_written = try encodeSpillRow(row, self.buf[self.offset..]);
        std.debug.assert(bytes_written == row_size);
        self.offset += row_size;
        self.row_count += 1;
        return true;
    }

    /// Finalize the page payload: patches the row_count header and returns the
    /// complete payload slice ready for `TempPage.writeChunk()`.
    pub fn finalize(self: *SpillPageWriter) []const u8 {
        std.debug.assert(self.row_count > 0);
        @memcpy(self.buf[0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, self.row_count)));
        return self.buf[0..self.offset];
    }

    /// Reset writer state for reuse with a new page.
    pub fn reset(self: *SpillPageWriter) void {
        self.offset = page_row_count_size;
        self.row_count = 0;
    }
};

// ---------------------------------------------------------------------------
// SpillPageReader — sequential reader for a packed temp page payload
// ---------------------------------------------------------------------------

/// Reads serialized rows sequentially from a temp page payload.
///
/// Usage:
///   var reader = try SpillPageReader.init(payload);
///   var row = ResultRow.init();
///   while (try reader.next(&row, &arena)) {
///       // process row
///   }
pub const SpillPageReader = struct {
    payload: []const u8,
    offset: u16,
    row_count: u16,
    rows_read: u16,

    pub fn init(payload: []const u8) SpillError!SpillPageReader {
        if (payload.len < page_row_count_size) return error.UnexpectedEndOfData;
        const row_count = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, payload[0..2]).*);
        return .{
            .payload = payload,
            .offset = page_row_count_size,
            .row_count = row_count,
            .rows_read = 0,
        };
    }

    /// Decode the next row into `out`. Returns `true` if a row was read, `false`
    /// when all rows have been consumed.
    pub fn next(self: *SpillPageReader, out: *ResultRow, arena: *StringArena) SpillError!bool {
        if (self.rows_read >= self.row_count) return false;
        const remaining = self.payload[self.offset..];
        const consumed = try decodeSpillRow(remaining, out, arena);
        self.offset += consumed;
        self.rows_read += 1;
        return true;
    }
};

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;

fn makeRow(values: []const Value) ResultRow {
    var row = ResultRow.init();
    row.column_count = @intCast(values.len);
    for (values, 0..) |v, i| {
        row.values[i] = v;
    }
    return row;
}

fn testArenaBuffer() [4096]u8 {
    return [_]u8{0} ** 4096;
}

test "roundtrip single i64" {
    var row = makeRow(&.{.{ .i64 = 42 }});
    var buf: [128]u8 = undefined;
    const written = try encodeSpillRow(&row, &buf);

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var decoded = ResultRow.init();
    const consumed = try decodeSpillRow(buf[0..written], &decoded, &arena);

    try testing.expectEqual(written, consumed);
    try testing.expectEqual(@as(u16, 1), decoded.column_count);
    try testing.expectEqual(@as(i64, 42), decoded.values[0].i64);
}

test "roundtrip all fixed types" {
    var row = makeRow(&.{
        .{ .i8 = -7 },
        .{ .i16 = -300 },
        .{ .i32 = -100_000 },
        .{ .i64 = -9_000_000_000 },
        .{ .u8 = 200 },
        .{ .u16 = 50_000 },
        .{ .u32 = 3_000_000_000 },
        .{ .u64 = 18_000_000_000_000_000_000 },
        .{ .f64 = 3.14159265358979 },
        .{ .bool = true },
        .{ .timestamp = 1_700_000_000_000_000 },
    });
    var buf: [256]u8 = undefined;
    const written = try encodeSpillRow(&row, &buf);

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var decoded = ResultRow.init();
    _ = try decodeSpillRow(buf[0..written], &decoded, &arena);

    try testing.expectEqual(@as(u16, 11), decoded.column_count);
    try testing.expectEqual(@as(i8, -7), decoded.values[0].i8);
    try testing.expectEqual(@as(i16, -300), decoded.values[1].i16);
    try testing.expectEqual(@as(i32, -100_000), decoded.values[2].i32);
    try testing.expectEqual(@as(i64, -9_000_000_000), decoded.values[3].i64);
    try testing.expectEqual(@as(u8, 200), decoded.values[4].u8);
    try testing.expectEqual(@as(u16, 50_000), decoded.values[5].u16);
    try testing.expectEqual(@as(u32, 3_000_000_000), decoded.values[6].u32);
    try testing.expectEqual(@as(u64, 18_000_000_000_000_000_000), decoded.values[7].u64);
    try testing.expectEqual(@as(f64, 3.14159265358979), decoded.values[8].f64);
    try testing.expectEqual(true, decoded.values[9].bool);
    try testing.expectEqual(@as(i64, 1_700_000_000_000_000), decoded.values[10].timestamp);
}

test "roundtrip string" {
    var row = makeRow(&.{.{ .string = "hello world" }});
    var buf: [128]u8 = undefined;
    const written = try encodeSpillRow(&row, &buf);

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var decoded = ResultRow.init();
    _ = try decodeSpillRow(buf[0..written], &decoded, &arena);

    try testing.expectEqual(@as(u16, 1), decoded.column_count);
    try testing.expectEqualStrings("hello world", decoded.values[0].string);
}

test "roundtrip mixed types" {
    var row = makeRow(&.{
        .{ .i64 = 999 },
        .{ .string = "pg2" },
        .{ .bool = false },
        .{ .f64 = -0.5 },
    });
    var buf: [128]u8 = undefined;
    const written = try encodeSpillRow(&row, &buf);

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var decoded = ResultRow.init();
    _ = try decodeSpillRow(buf[0..written], &decoded, &arena);

    try testing.expectEqual(@as(u16, 4), decoded.column_count);
    try testing.expectEqual(@as(i64, 999), decoded.values[0].i64);
    try testing.expectEqualStrings("pg2", decoded.values[1].string);
    try testing.expectEqual(false, decoded.values[2].bool);
    try testing.expectEqual(@as(f64, -0.5), decoded.values[3].f64);
}

test "roundtrip with nulls" {
    var row = makeRow(&.{
        .{ .i64 = 1 },
        .{ .null_value = {} },
        .{ .string = "test" },
        .{ .null_value = {} },
        .{ .bool = true },
    });
    var buf: [128]u8 = undefined;
    const written = try encodeSpillRow(&row, &buf);

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var decoded = ResultRow.init();
    _ = try decodeSpillRow(buf[0..written], &decoded, &arena);

    try testing.expectEqual(@as(u16, 5), decoded.column_count);
    try testing.expectEqual(@as(i64, 1), decoded.values[0].i64);
    try testing.expect(decoded.values[1] == .null_value);
    try testing.expectEqualStrings("test", decoded.values[2].string);
    try testing.expect(decoded.values[3] == .null_value);
    try testing.expectEqual(true, decoded.values[4].bool);
}

test "roundtrip all null columns" {
    var row = makeRow(&.{
        .{ .null_value = {} },
        .{ .null_value = {} },
        .{ .null_value = {} },
    });
    var buf: [128]u8 = undefined;
    const written = try encodeSpillRow(&row, &buf);

    // Size: row_data_len(2) + column_count(1) + null_bitmap(1) = 4 bytes
    try testing.expectEqual(@as(u16, 4), written);

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var decoded = ResultRow.init();
    _ = try decodeSpillRow(buf[0..written], &decoded, &arena);

    try testing.expectEqual(@as(u16, 3), decoded.column_count);
    for (0..3) |i| {
        try testing.expect(decoded.values[i] == .null_value);
    }
}

test "roundtrip empty string" {
    var row = makeRow(&.{.{ .string = "" }});
    var buf: [128]u8 = undefined;
    const written = try encodeSpillRow(&row, &buf);

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var decoded = ResultRow.init();
    _ = try decodeSpillRow(buf[0..written], &decoded, &arena);

    try testing.expectEqual(@as(u16, 1), decoded.column_count);
    try testing.expectEqualStrings("", decoded.values[0].string);
}

test "roundtrip max columns" {
    var values: [max_columns]Value = undefined;
    for (0..max_columns) |i| {
        values[i] = .{ .u8 = @intCast(i % 256) };
    }
    var row = makeRow(&values);
    var buf: [8192]u8 = undefined;
    const written = try encodeSpillRow(&row, &buf);

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var decoded = ResultRow.init();
    _ = try decodeSpillRow(buf[0..written], &decoded, &arena);

    try testing.expectEqual(@as(u16, max_columns), decoded.column_count);
    for (0..max_columns) |i| {
        try testing.expectEqual(@as(u8, @intCast(i % 256)), decoded.values[i].u8);
    }
}

test "page writer and reader multi-row roundtrip" {
    var writer = SpillPageWriter.init();

    const row1 = makeRow(&.{ .{ .i64 = 1 }, .{ .string = "first" } });
    const row2 = makeRow(&.{ .{ .i64 = 2 }, .{ .string = "second" } });
    const row3 = makeRow(&.{ .{ .i64 = 3 }, .{ .string = "third" } });

    try testing.expect(try writer.appendRow(&row1));
    try testing.expect(try writer.appendRow(&row2));
    try testing.expect(try writer.appendRow(&row3));

    const payload = writer.finalize();

    var reader = try SpillPageReader.init(payload);
    try testing.expectEqual(@as(u16, 3), reader.row_count);

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var decoded = ResultRow.init();

    try testing.expect(try reader.next(&decoded, &arena));
    try testing.expectEqual(@as(i64, 1), decoded.values[0].i64);
    try testing.expectEqualStrings("first", decoded.values[1].string);

    try testing.expect(try reader.next(&decoded, &arena));
    try testing.expectEqual(@as(i64, 2), decoded.values[0].i64);
    try testing.expectEqualStrings("second", decoded.values[1].string);

    try testing.expect(try reader.next(&decoded, &arena));
    try testing.expectEqual(@as(i64, 3), decoded.values[0].i64);
    try testing.expectEqualStrings("third", decoded.values[1].string);

    try testing.expect(!try reader.next(&decoded, &arena));
}

test "page writer returns false when full" {
    var writer = SpillPageWriter.init();

    // Create a row with a large string that takes ~4100 bytes serialized.
    const big_str = [_]u8{'x'} ** 4090;
    const big_row = makeRow(&.{.{ .string = &big_str }});

    // First row fits.
    try testing.expect(try writer.appendRow(&big_row));
    // Second row should not fit (~4100 * 2 > 8154).
    try testing.expect(!try writer.appendRow(&big_row));

    try testing.expectEqual(@as(u16, 1), writer.row_count);
}

test "page writer single row" {
    var writer = SpillPageWriter.init();
    const row = makeRow(&.{.{ .i32 = 7 }});
    try testing.expect(try writer.appendRow(&row));

    const payload = writer.finalize();
    var reader = try SpillPageReader.init(payload);
    try testing.expectEqual(@as(u16, 1), reader.row_count);

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var decoded = ResultRow.init();
    try testing.expect(try reader.next(&decoded, &arena));
    try testing.expectEqual(@as(i32, 7), decoded.values[0].i32);
    try testing.expect(!try reader.next(&decoded, &arena));
}

test "decode truncated buffer" {
    var row = makeRow(&.{.{ .i64 = 42 }});
    var buf: [128]u8 = undefined;
    const written = try encodeSpillRow(&row, &buf);

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var decoded = ResultRow.init();
    // Truncate by cutting off last byte.
    try testing.expectError(error.UnexpectedEndOfData, decodeSpillRow(buf[0 .. written - 1], &decoded, &arena));
}

test "decode invalid type tag" {
    // Manually craft a payload with an invalid type tag (0xFF).
    var buf: [16]u8 = undefined;
    // row_data_len = 3 (column_count + bitmap + type_tag)
    @memcpy(buf[0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, @as(u16, 3))));
    buf[2] = 1; // column_count = 1
    buf[3] = 0; // null_bitmap: not null
    buf[4] = 0xFF; // invalid type tag

    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var decoded = ResultRow.init();
    try testing.expectError(error.InvalidColumnType, decodeSpillRow(buf[0..5], &decoded, &arena));
}

test "encode zero columns" {
    var row = ResultRow.init();
    row.column_count = 0;
    try testing.expectError(error.InvalidColumnCount, spillRowSize(&row));
}

test "spillRowSize matches encode" {
    const rows = [_]ResultRow{
        makeRow(&.{.{ .i64 = 42 }}),
        makeRow(&.{ .{ .string = "hello" }, .{ .bool = true } }),
        makeRow(&.{ .{ .null_value = {} }, .{ .i32 = 7 }, .{ .null_value = {} } }),
        makeRow(&.{
            .{ .u64 = 1 },
            .{ .f64 = 2.5 },
            .{ .timestamp = 1000 },
            .{ .string = "abcd" },
            .{ .i8 = -1 },
        }),
    };

    var buf: [8192]u8 = undefined;
    for (&rows) |*row| {
        const predicted_size = try spillRowSize(row);
        const actual_size = try encodeSpillRow(row, &buf);
        try testing.expectEqual(predicted_size, actual_size);
    }
}

test "deterministic encoding" {
    var row = makeRow(&.{
        .{ .i64 = 12345 },
        .{ .string = "determinism" },
        .{ .null_value = {} },
        .{ .f64 = 1.0 },
    });

    var buf1: [256]u8 = undefined;
    var buf2: [256]u8 = undefined;
    const written1 = try encodeSpillRow(&row, &buf1);
    const written2 = try encodeSpillRow(&row, &buf2);

    try testing.expectEqual(written1, written2);
    try testing.expectEqualSlices(u8, buf1[0..written1], buf2[0..written2]);
}

test "golden byte vector" {
    // Row: [i64=42, string="AB", bool=true]
    // Expected layout:
    //   row_data_len:  u16 LE  (total - 2)
    //   column_count:  0x03
    //   null_bitmap:   0x00    (no nulls, 1 byte for 3 cols)
    //   col 0: type_tag=4 (i64), payload=42 as i64 LE
    //   col 1: type_tag=11 (string), str_len=2 LE, "AB"
    //   col 2: type_tag=10 (bool), 0x01
    var row = makeRow(&.{
        .{ .i64 = 42 },
        .{ .string = "AB" },
        .{ .bool = true },
    });

    var buf: [128]u8 = undefined;
    const written = try encodeSpillRow(&row, &buf);

    // Manual computation:
    //   row_data_len = 1 + 1 + (1+8) + (1+2+2) + (1+1) = 18
    //   total = 20
    const expected = [_]u8{
        // row_data_len = 18 (LE)
        0x12, 0x00,
        // column_count = 3
        0x03,
        // null_bitmap = 0x00
        0x00,
        // col 0: type_tag=4 (i64), 42 as i64 LE
        0x04, 0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // col 1: type_tag=11 (string), len=2 LE, "AB"
        0x0B, 0x02, 0x00, 0x41, 0x42,
        // col 2: type_tag=10 (bool), 0x01
        0x0A, 0x01,
    };

    try testing.expectEqual(@as(u16, 20), written);
    try testing.expectEqualSlices(u8, &expected, buf[0..written]);
}

test "page writer reset" {
    var writer = SpillPageWriter.init();
    const row = makeRow(&.{.{ .i64 = 1 }});
    try testing.expect(try writer.appendRow(&row));
    try testing.expectEqual(@as(u16, 1), writer.row_count);

    writer.reset();
    try testing.expectEqual(@as(u16, 0), writer.row_count);
    try testing.expectEqual(@as(u16, page_row_count_size), writer.offset);

    // Can write again after reset.
    const row2 = makeRow(&.{.{ .i64 = 2 }});
    try testing.expect(try writer.appendRow(&row2));

    const payload = writer.finalize();
    var arena_buf = testArenaBuffer();
    var arena = StringArena.init(&arena_buf);
    var reader = try SpillPageReader.init(payload);
    var decoded = ResultRow.init();
    try testing.expect(try reader.next(&decoded, &arena));
    try testing.expectEqual(@as(i64, 2), decoded.values[0].i64);
}
