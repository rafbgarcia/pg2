//! Persisted raw advisor metrics (v1) used by `pg2 advise`.
//!
//! Responsibilities in this file:
//! - Defines versioned raw metric record schema for advisor ingestion.
//! - Appends raw metric records to `advisor_metrics.pg2` deterministically.
//! - Reads/validates persisted metric records with corruption checks.
const std = @import("std");

pub const metrics_filename = "advisor_metrics.pg2";

const file_magic = "PG2ADVM1";
const file_version: u16 = 1;
const header_size: usize = 12;
const record_size: usize = 88;

pub const MetricsError = error{
    InvalidFormat,
    UnsupportedVersion,
    OutOfMemory,
} || std.fs.File.OpenError || std.fs.File.ReadError || std.fs.File.WriteError || std.fs.File.StatError || std.fs.File.SeekError;

pub const OperationKind = enum(u8) {
    unknown = 0,
    select = 1,
    insert = 2,
    update = 3,
    delete = 4,
    mixed = 5,
};

/// Raw persisted metric input record for advisor rule evaluation.
pub const MetricRecord = struct {
    timestamp_unix_ns: u64 = 0,
    query_fingerprint: u64 = 0,
    operation_kind: OperationKind = .unknown,
    has_predicate_filter: bool = false,
    had_error: bool = false,
    spill_triggered: bool = false,
    scan_strategy: u8 = 0,
    join_strategy: u8 = 0,
    rows_scanned: u32 = 0,
    rows_matched: u32 = 0,
    rows_returned: u32 = 0,
    rows_inserted: u32 = 0,
    rows_updated: u32 = 0,
    rows_deleted: u32 = 0,
    temp_pages_allocated: u32 = 0,
    temp_pages_reclaimed: u32 = 0,
    temp_bytes_written: u64 = 0,
    temp_bytes_read: u64 = 0,
    queue_depth: u32 = 0,
    workers_busy: u32 = 0,
    queue_timeout_total: u64 = 0,
};

pub fn appendRecord(root_dir: *std.fs.Dir, record: *const MetricRecord) MetricsError!void {
    var file = root_dir.createFile(metrics_filename, .{
        .read = true,
        .truncate = false,
        .exclusive = false,
    }) catch |err| switch (err) {
        error.PathAlreadyExists => try root_dir.openFile(metrics_filename, .{ .mode = .read_write }),
        else => return err,
    };
    defer file.close();

    const stat = try file.stat();
    if (stat.size == 0) {
        try writeHeader(&file);
    } else {
        try validateHeader(&file);
    }

    try file.seekFromEnd(0);
    var encoded: [record_size]u8 = undefined;
    encodeRecord(record, &encoded);
    try file.writeAll(&encoded);
}

pub fn readAll(allocator: std.mem.Allocator, root_dir: *std.fs.Dir) MetricsError![]MetricRecord {
    var file = try root_dir.openFile(metrics_filename, .{});
    defer file.close();

    const stat = try file.stat();
    if (stat.size < header_size) return error.InvalidFormat;
    const payload_size: usize = @intCast(stat.size);
    const raw = try allocator.alloc(u8, payload_size);
    defer allocator.free(raw);

    const read_len = try file.readAll(raw);
    if (read_len != raw.len) return error.InvalidFormat;

    validateHeaderBytes(raw[0..header_size]) catch |err| switch (err) {
        error.InvalidFormat => return error.InvalidFormat,
        error.UnsupportedVersion => return error.UnsupportedVersion,
    };

    const records_bytes = raw[header_size..];
    if ((records_bytes.len % record_size) != 0) return error.InvalidFormat;
    const count = records_bytes.len / record_size;
    const out = try allocator.alloc(MetricRecord, count);
    var offset: usize = 0;
    var index: usize = 0;
    while (index < count) : (index += 1) {
        out[index] = decodeRecord(records_bytes[offset .. offset + record_size]) catch {
            allocator.free(out);
            return error.InvalidFormat;
        };
        offset += record_size;
    }
    return out;
}

fn writeHeader(file: *std.fs.File) MetricsError!void {
    var header: [header_size]u8 = undefined;
    @memcpy(header[0..file_magic.len], file_magic);
    std.mem.writeInt(u16, header[8..10], file_version, .little);
    std.mem.writeInt(u16, header[10..12], 0, .little);
    try file.writeAll(&header);
}

fn validateHeader(file: *std.fs.File) MetricsError!void {
    try file.seekTo(0);
    var header: [header_size]u8 = undefined;
    const read_len = try file.readAll(&header);
    if (read_len != header_size) return error.InvalidFormat;
    try validateHeaderBytes(&header);
}

fn validateHeaderBytes(header: []const u8) error{ InvalidFormat, UnsupportedVersion }!void {
    if (header.len != header_size) return error.InvalidFormat;
    if (!std.mem.eql(u8, header[0..file_magic.len], file_magic)) return error.InvalidFormat;
    const version = std.mem.readInt(u16, header[8..10], .little);
    if (version != file_version) return error.UnsupportedVersion;
}

fn encodeRecord(record: *const MetricRecord, out: *[record_size]u8) void {
    @memset(out, 0);
    var cursor: usize = 0;

    writeU64(out, &cursor, record.timestamp_unix_ns);
    writeU64(out, &cursor, record.query_fingerprint);
    writeU8(out, &cursor, @intFromEnum(record.operation_kind));
    writeU8(out, &cursor, @intFromBool(record.has_predicate_filter));
    writeU8(out, &cursor, @intFromBool(record.had_error));
    writeU8(out, &cursor, @intFromBool(record.spill_triggered));
    writeU8(out, &cursor, record.scan_strategy);
    writeU8(out, &cursor, record.join_strategy);
    cursor += 2; // reserved

    writeU32(out, &cursor, record.rows_scanned);
    writeU32(out, &cursor, record.rows_matched);
    writeU32(out, &cursor, record.rows_returned);
    writeU32(out, &cursor, record.rows_inserted);
    writeU32(out, &cursor, record.rows_updated);
    writeU32(out, &cursor, record.rows_deleted);
    writeU32(out, &cursor, record.temp_pages_allocated);
    writeU32(out, &cursor, record.temp_pages_reclaimed);

    writeU64(out, &cursor, record.temp_bytes_written);
    writeU64(out, &cursor, record.temp_bytes_read);

    writeU32(out, &cursor, record.queue_depth);
    writeU32(out, &cursor, record.workers_busy);
    writeU64(out, &cursor, record.queue_timeout_total);

    std.debug.assert(cursor == record_size);
}

fn decodeRecord(raw: []const u8) error{InvalidFormat}!MetricRecord {
    if (raw.len != record_size) return error.InvalidFormat;
    var cursor: usize = 0;

    const timestamp_unix_ns = readU64(raw, &cursor);
    const query_fingerprint = readU64(raw, &cursor);
    const op_kind_int = readU8(raw, &cursor);
    const has_predicate = readU8(raw, &cursor);
    const had_error = readU8(raw, &cursor);
    const spill_triggered = readU8(raw, &cursor);
    const scan_strategy = readU8(raw, &cursor);
    const join_strategy = readU8(raw, &cursor);
    cursor += 2;

    return .{
        .timestamp_unix_ns = timestamp_unix_ns,
        .query_fingerprint = query_fingerprint,
        .operation_kind = std.meta.intToEnum(OperationKind, op_kind_int) catch return error.InvalidFormat,
        .has_predicate_filter = has_predicate != 0,
        .had_error = had_error != 0,
        .spill_triggered = spill_triggered != 0,
        .scan_strategy = scan_strategy,
        .join_strategy = join_strategy,
        .rows_scanned = readU32(raw, &cursor),
        .rows_matched = readU32(raw, &cursor),
        .rows_returned = readU32(raw, &cursor),
        .rows_inserted = readU32(raw, &cursor),
        .rows_updated = readU32(raw, &cursor),
        .rows_deleted = readU32(raw, &cursor),
        .temp_pages_allocated = readU32(raw, &cursor),
        .temp_pages_reclaimed = readU32(raw, &cursor),
        .temp_bytes_written = readU64(raw, &cursor),
        .temp_bytes_read = readU64(raw, &cursor),
        .queue_depth = readU32(raw, &cursor),
        .workers_busy = readU32(raw, &cursor),
        .queue_timeout_total = readU64(raw, &cursor),
    };
}

fn writeU8(buf: *[record_size]u8, cursor: *usize, value: u8) void {
    buf[cursor.*] = value;
    cursor.* += 1;
}

fn writeU32(buf: *[record_size]u8, cursor: *usize, value: u32) void {
    const view: *[4]u8 = @ptrCast(buf[cursor.* .. cursor.* + 4]);
    std.mem.writeInt(u32, view, value, .little);
    cursor.* += 4;
}

fn writeU64(buf: *[record_size]u8, cursor: *usize, value: u64) void {
    const view: *[8]u8 = @ptrCast(buf[cursor.* .. cursor.* + 8]);
    std.mem.writeInt(u64, view, value, .little);
    cursor.* += 8;
}

fn readU8(buf: []const u8, cursor: *usize) u8 {
    const value = buf[cursor.*];
    cursor.* += 1;
    return value;
}

fn readU32(buf: []const u8, cursor: *usize) u32 {
    const view: *const [4]u8 = @ptrCast(buf[cursor.* .. cursor.* + 4]);
    const value = std.mem.readInt(u32, view, .little);
    cursor.* += 4;
    return value;
}

fn readU64(buf: []const u8, cursor: *usize) u64 {
    const view: *const [8]u8 = @ptrCast(buf[cursor.* .. cursor.* + 8]);
    const value = std.mem.readInt(u64, view, .little);
    cursor.* += 8;
    return value;
}

test "appendRecord and readAll round-trip records" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const input = MetricRecord{
        .timestamp_unix_ns = 1234,
        .query_fingerprint = 0xABCD,
        .operation_kind = .select,
        .has_predicate_filter = true,
        .spill_triggered = true,
        .scan_strategy = 2,
        .join_strategy = 1,
        .rows_scanned = 100,
        .rows_matched = 20,
        .rows_returned = 20,
        .temp_pages_allocated = 4,
        .temp_pages_reclaimed = 2,
        .temp_bytes_written = 8192,
        .temp_bytes_read = 4096,
        .queue_depth = 3,
        .workers_busy = 1,
        .queue_timeout_total = 5,
    };

    try appendRecord(&tmp.dir, &input);
    try appendRecord(&tmp.dir, &input);

    const out = try readAll(std.testing.allocator, &tmp.dir);
    defer std.testing.allocator.free(out);
    try std.testing.expectEqual(@as(usize, 2), out.len);
    try std.testing.expectEqual(input.rows_scanned, out[0].rows_scanned);
    try std.testing.expectEqual(input.rows_matched, out[0].rows_matched);
    try std.testing.expectEqual(input.operation_kind, out[0].operation_kind);
    try std.testing.expect(out[0].has_predicate_filter);
}

test "readAll rejects invalid header magic" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile(metrics_filename, .{});
    defer file.close();
    try file.writeAll("bad-header");

    try std.testing.expectError(error.InvalidFormat, readAll(std.testing.allocator, &tmp.dir));
}

test "readAll rejects truncated payload" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile(metrics_filename, .{});
    defer file.close();
    var header: [header_size]u8 = undefined;
    @memcpy(header[0..file_magic.len], file_magic);
    std.mem.writeInt(u16, header[8..10], file_version, .little);
    std.mem.writeInt(u16, header[10..12], 0, .little);
    try file.writeAll(&header);
    try file.writeAll("partial");

    try std.testing.expectError(error.InvalidFormat, readAll(std.testing.allocator, &tmp.dir));
}
