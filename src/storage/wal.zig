const std = @import("std");
const io = @import("io.zig");

/// WAL record types.
pub const RecordType = enum(u8) {
    tx_begin = 1,
    tx_commit = 2,
    tx_abort = 3,
    insert = 4,
    update = 5,
    delete = 6,
    checkpoint = 7,
    btree_insert = 8,
    btree_delete = 9,
    btree_split_leaf = 10,
    btree_split_internal = 11,
    btree_new_root = 12,
};

/// A single WAL record.
///
/// On-disk layout:
///   lsn:          u64   (8 bytes)
///   tx_id:        u64   (8 bytes)
///   record_type:  u8    (1 byte)
///   page_id:      u64   (8 bytes)
///   payload_len:  u16   (2 bytes)
///   payload:      [payload_len]u8
///   crc32:        u32   (4 bytes)
///
/// Fixed header = 27 bytes, then variable payload, then 4-byte CRC.
pub const Record = struct {
    lsn: u64,
    tx_id: u64,
    record_type: RecordType,
    page_id: u64,
    payload: []const u8,

    pub const header_size = 27;
    pub const crc_size = 4;

    /// Total serialized size of this record.
    pub fn serializedSize(self: *const Record) usize {
        return header_size + self.payload.len + crc_size;
    }

    /// Serialize into a caller-provided buffer. Returns the slice written.
    pub fn serialize(self: *const Record, buf: []u8) []u8 {
        const total = self.serializedSize();
        std.debug.assert(buf.len >= total);

        var offset: usize = 0;

        // lsn
        @memcpy(buf[offset..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, self.lsn)));
        offset += 8;

        // tx_id
        @memcpy(buf[offset..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, self.tx_id)));
        offset += 8;

        // record_type
        buf[offset] = @intFromEnum(self.record_type);
        offset += 1;

        // page_id
        @memcpy(buf[offset..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, self.page_id)));
        offset += 8;

        // payload_len
        const plen: u16 = @intCast(self.payload.len);
        @memcpy(buf[offset..][0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, plen)));
        offset += 2;

        // payload
        @memcpy(buf[offset..][0..self.payload.len], self.payload);
        offset += self.payload.len;

        // CRC over everything before the CRC field.
        const crc = std.hash.crc.Crc32Iscsi.hash(buf[0..offset]);
        const crc_le = std.mem.nativeToLittle(u32, crc);
        @memcpy(buf[offset..][0..4], std.mem.asBytes(&crc_le));
        offset += 4;

        return buf[0..offset];
    }

    /// Deserialize a record from a buffer. Returns the record and number of
    /// bytes consumed. The returned record's payload points into `buf`.
    pub fn deserialize(buf: []const u8) error{ Truncated, ChecksumMismatch }!struct { record: Record, bytes_consumed: usize } {
        if (buf.len < header_size + crc_size) return error.Truncated;

        var offset: usize = 0;

        const lsn = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, buf[offset..][0..8]).*);
        offset += 8;

        const tx_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, buf[offset..][0..8]).*);
        offset += 8;

        const record_type = std.meta.intToEnum(RecordType, buf[offset]) catch return error.ChecksumMismatch;
        offset += 1;

        const page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, buf[offset..][0..8]).*);
        offset += 8;

        const payload_len = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, buf[offset..][0..2]).*);
        offset += 2;

        const total = header_size + @as(usize, payload_len) + crc_size;
        if (buf.len < total) return error.Truncated;

        const payload = buf[offset..][0..payload_len];
        offset += payload_len;

        // Verify CRC.
        const stored_crc = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, buf[offset..][0..4]).*);
        const computed_crc = std.hash.crc.Crc32Iscsi.hash(buf[0..offset]);
        if (stored_crc != computed_crc) return error.ChecksumMismatch;
        offset += 4;

        return .{
            .record = .{
                .lsn = lsn,
                .tx_id = tx_id,
                .record_type = record_type,
                .page_id = page_id,
                .payload = payload,
            },
            .bytes_consumed = offset,
        };
    }
};

/// Maximum payload size (u16 max).
pub const max_payload_size = std.math.maxInt(u16);

pub const WalError = error{
    OutOfMemory,
    PayloadTooLarge,
    RecordBufferTooSmall,
    PayloadBufferTooSmall,
    WalReadError,
    WalWriteError,
    WalFsyncError,
    InvalidEnvelope,
    CorruptEnvelope,
    UnsupportedEnvelopeVersion,
};

const envelope_magic: u32 = 0x50473257; // "PG2W"
const envelope_version: u16 = 1;

const RecoveryEnvelope = struct {
    magic: u32,
    version: u16,
    reserved: u16,
    wal_page_offset: u64,
    wal_byte_offset: u64,
    next_lsn: u64,
    flushed_lsn: u64,
    checksum: u32,

    const size: usize = 4 + 2 + 2 + 8 + 8 + 8 + 8 + 4;

    fn init(wal: *const Wal) RecoveryEnvelope {
        return .{
            .magic = envelope_magic,
            .version = envelope_version,
            .reserved = 0,
            .wal_page_offset = wal.wal_page_offset,
            .wal_byte_offset = wal.wal_byte_offset,
            .next_lsn = wal.next_lsn,
            .flushed_lsn = wal.flushed_lsn,
            .checksum = 0,
        };
    }

    fn serialize(self: *const RecoveryEnvelope, out: *[size]u8) void {
        var offset: usize = 0;
        @memcpy(out[offset..][0..4], std.mem.asBytes(&std.mem.nativeToLittle(u32, self.magic)));
        offset += 4;
        @memcpy(out[offset..][0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, self.version)));
        offset += 2;
        @memcpy(out[offset..][0..2], std.mem.asBytes(&std.mem.nativeToLittle(u16, self.reserved)));
        offset += 2;
        @memcpy(out[offset..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, self.wal_page_offset)));
        offset += 8;
        @memcpy(out[offset..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, self.wal_byte_offset)));
        offset += 8;
        @memcpy(out[offset..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, self.next_lsn)));
        offset += 8;
        @memcpy(out[offset..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, self.flushed_lsn)));
        offset += 8;
        @memset(out[offset..][0..4], 0);

        const cksum = std.hash.crc.Crc32Iscsi.hash(out[0 .. size - 4]);
        @memcpy(out[size - 4 ..][0..4], std.mem.asBytes(&std.mem.nativeToLittle(u32, cksum)));
    }

    fn deserialize(in: []const u8) WalError!RecoveryEnvelope {
        if (in.len < size) return error.InvalidEnvelope;

        const stored_cksum = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, in[size - 4 ..][0..4]).*);
        const computed_cksum = std.hash.crc.Crc32Iscsi.hash(in[0 .. size - 4]);
        if (stored_cksum != computed_cksum) return error.CorruptEnvelope;

        const magic = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, in[0..4]).*);
        if (magic != envelope_magic) return error.InvalidEnvelope;

        const version = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, in[4..6]).*);
        if (version != envelope_version) return error.UnsupportedEnvelopeVersion;

        return .{
            .magic = magic,
            .version = version,
            .reserved = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, in[6..8]).*),
            .wal_page_offset = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, in[8..16]).*),
            .wal_byte_offset = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, in[16..24]).*),
            .next_lsn = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, in[24..32]).*),
            .flushed_lsn = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, in[32..40]).*),
            .checksum = stored_cksum,
        };
    }
};

/// Write-Ahead Log.
///
/// Append-only log of records backed by the Storage interface. Records
/// are buffered in memory and flushed to storage on commit (or when the
/// buffer fills). The WAL tracks the current LSN and the flushed LSN.
///
/// The buffer pool checks `flushed_lsn` before writing dirty pages to
/// ensure the WAL protocol is maintained.
pub const Wal = struct {
    storage: io.Storage,
    allocator: std.mem.Allocator,
    inline_buffer: [io.page_size]u8,

    /// Monotonically increasing log sequence number.
    next_lsn: u64 = 1,
    /// LSN up to which the WAL has been durably fsynced.
    flushed_lsn: u64 = 0,

    /// In-memory write buffer carved at startup (fixed capacity).
    buffer: []u8 = &.{},
    /// Number of valid bytes currently buffered.
    buffer_len: usize = 0,
    /// True when `buffer` was allocator-backed (not inline storage).
    buffer_owned: bool = false,
    /// When true, runtime growth is forbidden and append fails closed.
    buffer_fixed_capacity: bool = false,
    /// LSN of the most recent record in the buffer (unflushed).
    buffer_max_lsn: u64 = 0,

    /// Page ID used for WAL storage. We use a simple scheme: WAL data
    /// is stored in sequential pages starting at a high page_id range
    /// to avoid collision with data pages.
    wal_page_base: u64 = 1_000_000,
    /// Metadata page storing recovery envelope.
    wal_meta_page_id: u64 = 999_999,
    /// Next WAL page to write.
    wal_page_offset: u64 = 0,
    /// Byte offset within the current WAL page.
    wal_byte_offset: usize = 0,

    // Stats
    records_written: u64 = 0,
    flushes: u64 = 0,
    bytes_flushed: u64 = 0,

    pub const ReadIntoResult = struct {
        records_len: usize,
        payload_bytes_used: usize,
    };

    pub fn init(allocator: std.mem.Allocator, storage: io.Storage) Wal {
        return .{
            .storage = storage,
            .allocator = allocator,
            .inline_buffer = undefined,
        };
    }

    pub fn deinit(self: *Wal) void {
        if (self.buffer_owned) {
            self.allocator.free(self.buffer);
        }
    }

    /// Reserve WAL buffer bytes during startup and lock append growth to this
    /// ceiling. Useful with sealed allocators where runtime growth is forbidden.
    pub fn reserveBufferCapacity(self: *Wal, capacity_bytes: usize) WalError!void {
        self.ensureInlineBufferBound();
        if (self.buffer_owned) return error.OutOfMemory;
        if (capacity_bytes <= self.inline_buffer.len) {
            self.buffer = self.inline_buffer[0..capacity_bytes];
            self.buffer_len = 0;
            self.buffer_owned = false;
            self.buffer_fixed_capacity = true;
            return;
        }
        self.buffer = self.allocator.alloc(u8, capacity_bytes) catch
            return error.OutOfMemory;
        self.buffer_len = 0;
        self.buffer_owned = true;
        self.buffer_fixed_capacity = true;
    }

    /// Append a record to the WAL. Returns the LSN assigned to this record.
    /// The record is buffered in memory — call `flush()` to make it durable.
    pub fn append(self: *Wal, tx_id: u64, record_type: RecordType, page_id: u64, payload: []const u8) WalError!u64 {
        self.ensureInlineBufferBound();
        if (payload.len > max_payload_size) return error.PayloadTooLarge;
        const lsn = self.next_lsn;
        self.next_lsn += 1;

        const rec = Record{
            .lsn = lsn,
            .tx_id = tx_id,
            .record_type = record_type,
            .page_id = page_id,
            .payload = payload,
        };

        const size = rec.serializedSize();
        const start = self.buffer_len;
        if (self.buffer.len == 0) return error.OutOfMemory;
        if (start + size > self.buffer.len) {
            if (self.buffer_fixed_capacity) return error.OutOfMemory;
            // Runtime is expected to pre-reserve at startup. Tests and
            // non-sealed allocators may grow dynamically as a fallback.
            try self.growBuffer(start + size);
        }
        _ = rec.serialize(self.buffer[start .. start + size]);
        self.buffer_len += size;

        self.buffer_max_lsn = lsn;
        self.records_written += 1;

        return lsn;
    }

    /// Flush the WAL buffer to storage and fsync. After this returns,
    /// all appended records are durable.
    pub fn flush(self: *Wal) WalError!void {
        if (self.buffer_len == 0) return;

        const page_size = io.page_size;
        var buf_offset: usize = 0;

        while (buf_offset < self.buffer_len) {
            var page_buf: [page_size]u8 = std.mem.zeroes([page_size]u8);
            const space = page_size - self.wal_byte_offset;
            const remaining = self.buffer_len - buf_offset;
            const to_copy = @min(space, remaining);

            // If continuing a partial page, read it first.
            if (self.wal_byte_offset > 0) {
                self.storage.read(self.wal_page_base + self.wal_page_offset, &page_buf) catch
                    return error.WalReadError;
            }

            @memcpy(page_buf[self.wal_byte_offset..][0..to_copy], self.buffer[buf_offset..][0..to_copy]);
            buf_offset += to_copy;

            self.storage.write(self.wal_page_base + self.wal_page_offset, &page_buf) catch
                return error.WalWriteError;

            self.wal_byte_offset += to_copy;
            if (self.wal_byte_offset >= page_size) {
                self.wal_byte_offset = 0;
                self.wal_page_offset += 1;
            }
        }

        self.storage.fsync() catch return error.WalFsyncError;

        self.bytes_flushed += self.buffer_len;
        self.flushed_lsn = self.buffer_max_lsn;
        self.buffer_len = 0;
        self.flushes += 1;

        try self.persistEnvelope();
    }

    /// Bounded no-allocation WAL decode path. Caller provides fixed-capacity
    /// record and payload buffers; returned payload slices point into payload_buf.
    pub fn readFromInto(
        self: *Wal,
        from_lsn: u64,
        out_records: []Record,
        payload_buf: []u8,
    ) WalError!ReadIntoResult {
        const total_pages = self.wal_page_offset + @as(u64, if (self.wal_byte_offset > 0) 1 else 0);
        if (total_pages == 0) return .{ .records_len = 0, .payload_bytes_used = 0 };

        var parse_buf: [Record.header_size + max_payload_size + Record.crc_size]u8 = undefined;
        var parse_len: usize = 0;
        var payload_used: usize = 0;
        var records_len: usize = 0;
        var stop_parse = false;

        var page_idx: u64 = 0;
        while (page_idx < total_pages and !stop_parse) : (page_idx += 1) {
            var page_buf: [io.page_size]u8 = undefined;
            self.storage.read(self.wal_page_base + page_idx, &page_buf) catch
                return error.WalReadError;

            const page_data_len: usize = if (page_idx == self.wal_page_offset and self.wal_byte_offset > 0)
                self.wal_byte_offset
            else
                io.page_size;

            var offset: usize = 0;
            while (offset < page_data_len and !stop_parse) {
                const free_space = parse_buf.len - parse_len;
                if (free_space == 0) break;
                const to_copy = @min(free_space, page_data_len - offset);
                @memcpy(parse_buf[parse_len..][0..to_copy], page_buf[offset..][0..to_copy]);
                parse_len += to_copy;
                offset += to_copy;

                while (true) {
                    const total_len = serializedRecordLen(parse_buf[0..parse_len]) orelse break;
                    if (parse_len < total_len) break;

                    const result = Record.deserialize(parse_buf[0..total_len]) catch {
                        stop_parse = true;
                        break;
                    };

                    if (result.record.lsn >= from_lsn) {
                        if (records_len >= out_records.len) return error.RecordBufferTooSmall;
                        if (payload_used + result.record.payload.len > payload_buf.len) {
                            return error.PayloadBufferTooSmall;
                        }

                        const payload_start = payload_used;
                        const payload_end = payload_start + result.record.payload.len;
                        if (result.record.payload.len > 0) {
                            @memcpy(payload_buf[payload_start..payload_end], result.record.payload);
                        }
                        payload_used = payload_end;

                        out_records[records_len] = .{
                            .lsn = result.record.lsn,
                            .tx_id = result.record.tx_id,
                            .record_type = result.record.record_type,
                            .page_id = result.record.page_id,
                            .payload = payload_buf[payload_start..payload_end],
                        };
                        records_len += 1;
                    }

                    const remaining = parse_len - total_len;
                    if (remaining > 0) {
                        std.mem.copyForwards(u8, parse_buf[0..remaining], parse_buf[total_len..parse_len]);
                    }
                    parse_len = remaining;
                }
            }
        }

        return .{ .records_len = records_len, .payload_bytes_used = payload_used };
    }

    /// Restore in-memory WAL pointers from the on-disk recovery envelope.
    pub fn recover(self: *Wal) WalError!void {
        var page_buf: [io.page_size]u8 = undefined;
        self.storage.read(self.wal_meta_page_id, &page_buf) catch
            return error.WalReadError;

        if (isAllZero(&page_buf)) return;

        const envelope = try RecoveryEnvelope.deserialize(page_buf[0..RecoveryEnvelope.size]);
        self.wal_page_offset = envelope.wal_page_offset;
        self.wal_byte_offset = @intCast(envelope.wal_byte_offset);
        self.next_lsn = envelope.next_lsn;
        self.flushed_lsn = envelope.flushed_lsn;
        self.buffer_len = 0;
    }

    /// Convenience: begin a transaction.
    pub fn beginTx(self: *Wal, tx_id: u64) WalError!u64 {
        return self.append(tx_id, .tx_begin, 0, &.{});
    }

    /// Convenience: commit a transaction. Flushes the WAL (group commit point).
    pub fn commitTx(self: *Wal, tx_id: u64) WalError!u64 {
        const lsn = try self.append(tx_id, .tx_commit, 0, &.{});
        try self.flush();
        return lsn;
    }

    /// Convenience: abort a transaction.
    pub fn abortTx(self: *Wal, tx_id: u64) WalError!u64 {
        return self.append(tx_id, .tx_abort, 0, &.{});
    }

    fn persistEnvelope(self: *Wal) WalError!void {
        var page_buf: [io.page_size]u8 = std.mem.zeroes([io.page_size]u8);
        const envelope = RecoveryEnvelope.init(self);
        var env_bytes: [RecoveryEnvelope.size]u8 = undefined;
        envelope.serialize(&env_bytes);
        @memcpy(page_buf[0..RecoveryEnvelope.size], &env_bytes);

        self.storage.write(self.wal_meta_page_id, &page_buf) catch
            return error.WalWriteError;
        self.storage.fsync() catch return error.WalFsyncError;
    }

    fn ensureInlineBufferBound(self: *Wal) void {
        if (!self.buffer_owned and self.buffer.len == 0) {
            self.buffer = self.inline_buffer[0..];
        }
    }

    fn growBuffer(self: *Wal, required_len: usize) WalError!void {
        std.debug.assert(required_len > self.buffer.len);
        const grown_len = @max(required_len, self.buffer.len * 2);
        const new_buf = self.allocator.alloc(u8, grown_len) catch
            return error.OutOfMemory;
        if (self.buffer_len > 0) {
            @memcpy(new_buf[0..self.buffer_len], self.buffer[0..self.buffer_len]);
        }
        if (self.buffer_owned) {
            self.allocator.free(self.buffer);
        }
        self.buffer = new_buf;
        self.buffer_owned = true;
    }
};

fn serializedRecordLen(prefix: []const u8) ?usize {
    if (prefix.len < Record.header_size) return null;
    const payload_len = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, prefix[25..27]).*);
    return Record.header_size + @as(usize, payload_len) + Record.crc_size;
}

fn isAllZero(buf: *const [io.page_size]u8) bool {
    for (buf) |b| {
        if (b != 0) return false;
    }
    return true;
}

// --- Tests ---

test "record serialize/deserialize roundtrip" {
    const payload = "hello world";
    const rec = Record{
        .lsn = 42,
        .tx_id = 7,
        .record_type = .insert,
        .page_id = 100,
        .payload = payload,
    };

    var buf: [256]u8 = undefined;
    const written = rec.serialize(&buf);

    const result = try Record.deserialize(written);
    try std.testing.expectEqual(@as(u64, 42), result.record.lsn);
    try std.testing.expectEqual(@as(u64, 7), result.record.tx_id);
    try std.testing.expectEqual(RecordType.insert, result.record.record_type);
    try std.testing.expectEqual(@as(u64, 100), result.record.page_id);
    try std.testing.expectEqualSlices(u8, payload, result.record.payload);
}

test "record CRC detects corruption" {
    const rec = Record{
        .lsn = 1,
        .tx_id = 1,
        .record_type = .tx_begin,
        .page_id = 0,
        .payload = &.{},
    };

    var buf: [64]u8 = undefined;
    const written = rec.serialize(&buf);

    // Corrupt a byte.
    var corrupt: [64]u8 = undefined;
    @memcpy(corrupt[0..written.len], written);
    corrupt[5] ^= 0xFF;

    const result = Record.deserialize(corrupt[0..written.len]);
    try std.testing.expectError(error.ChecksumMismatch, result);
}

test "empty payload roundtrip" {
    const rec = Record{
        .lsn = 1,
        .tx_id = 0,
        .record_type = .tx_commit,
        .page_id = 0,
        .payload = &.{},
    };

    var buf: [64]u8 = undefined;
    const written = rec.serialize(&buf);

    const result = try Record.deserialize(written);
    try std.testing.expectEqual(@as(u64, 1), result.record.lsn);
    try std.testing.expectEqual(RecordType.tx_commit, result.record.record_type);
    try std.testing.expectEqual(@as(usize, 0), result.record.payload.len);
}

test "WAL append and flush" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    _ = try wal.append(1, .tx_begin, 0, &.{});
    _ = try wal.append(1, .insert, 5, "row data");
    _ = try wal.append(1, .tx_commit, 0, &.{});
    try wal.flush();

    try std.testing.expectEqual(@as(u64, 3), wal.records_written);
    try std.testing.expectEqual(@as(u64, 3), wal.flushed_lsn);
    try std.testing.expectEqual(@as(u64, 1), wal.flushes);
}

test "WAL commitTx flushes automatically" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    _ = try wal.beginTx(1);
    _ = try wal.append(1, .insert, 10, "some data");
    const commit_lsn = try wal.commitTx(1);

    try std.testing.expectEqual(@as(u64, 3), commit_lsn);
    try std.testing.expectEqual(@as(u64, 3), wal.flushed_lsn);
    // WAL is durable — verify fsync was called.
    // One fsync for WAL data, one for recovery envelope.
    try std.testing.expectEqual(@as(u64, 2), disk.fsyncs);
}

test "WAL readFrom filters by LSN" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    _ = try wal.beginTx(1);     // lsn 1
    _ = try wal.append(1, .insert, 5, "a"); // lsn 2
    _ = try wal.commitTx(1);    // lsn 3

    _ = try wal.beginTx(2);     // lsn 4
    _ = try wal.append(2, .insert, 6, "b"); // lsn 5
    _ = try wal.commitTx(2);    // lsn 6

    // Read from LSN 4 onwards.
    var records_buf: [8]Record = undefined;
    var payload_buf: [64]u8 = undefined;
    const decoded = try wal.readFromInto(4, &records_buf, &payload_buf);
    const records = records_buf[0..decoded.records_len];

    try std.testing.expectEqual(@as(usize, 3), records.len);
    try std.testing.expectEqual(@as(u64, 4), records[0].lsn);
    try std.testing.expectEqual(@as(u64, 2), records[0].tx_id);
}

test "WAL survives crash after flush" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    {
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();

        _ = try wal.beginTx(1);
        _ = try wal.append(1, .insert, 5, "important");
        _ = try wal.commitTx(1);
        // WAL is flushed and fsynced. Simulate crash.
    }

    // After crash — recover offsets from metadata and read records.
    var wal2 = Wal.init(std.testing.allocator, disk.storage());
    defer wal2.deinit();
    try wal2.recover();

    var records_buf: [8]Record = undefined;
    var payload_buf: [64]u8 = undefined;
    const decoded = try wal2.readFromInto(1, &records_buf, &payload_buf);
    const records = records_buf[0..decoded.records_len];

    try std.testing.expectEqual(@as(usize, 3), records.len);
    try std.testing.expectEqual(RecordType.tx_begin, records[0].record_type);
    try std.testing.expectEqual(RecordType.insert, records[1].record_type);
    try std.testing.expectEqualSlices(u8, "important", records[1].payload);
    try std.testing.expectEqual(RecordType.tx_commit, records[2].record_type);
}

test "WAL unflushed data lost on crash" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    {
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();

        _ = try wal.beginTx(1);
        _ = try wal.append(1, .insert, 5, "lost");
        // No flush! Crash.
    }

    disk.crash();

    // Nothing should be recoverable.
    var wal2 = Wal.init(std.testing.allocator, disk.storage());
    defer wal2.deinit();
    wal2.wal_page_offset = 1;

    var records_buf: [8]Record = undefined;
    var payload_buf: [64]u8 = undefined;
    const decoded = try wal2.readFromInto(1, &records_buf, &payload_buf);
    const records = records_buf[0..decoded.records_len];

    try std.testing.expectEqual(@as(usize, 0), records.len);
}

test "WAL recover restores offsets from envelope" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    _ = try wal.beginTx(1);
    _ = try wal.append(1, .insert, 42, "envelope");
    _ = try wal.commitTx(1);

    var wal2 = Wal.init(std.testing.allocator, disk.storage());
    defer wal2.deinit();
    try wal2.recover();

    try std.testing.expectEqual(wal.wal_page_offset, wal2.wal_page_offset);
    try std.testing.expectEqual(wal.wal_byte_offset, wal2.wal_byte_offset);
    try std.testing.expectEqual(wal.next_lsn, wal2.next_lsn);
    try std.testing.expectEqual(wal.flushed_lsn, wal2.flushed_lsn);
}

test "WAL recover detects corrupt envelope" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    _ = try wal.beginTx(1);
    _ = try wal.commitTx(1);

    var meta_page: [io.page_size]u8 = undefined;
    try disk.storage().read(wal.wal_meta_page_id, &meta_page);
    meta_page[10] ^= 0xFF;
    try disk.storage().write(wal.wal_meta_page_id, &meta_page);
    try disk.storage().fsync();

    var wal2 = Wal.init(std.testing.allocator, disk.storage());
    defer wal2.deinit();

    try std.testing.expectError(error.CorruptEnvelope, wal2.recover());
}

test "WAL spanning multiple pages" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    // Write enough records to span multiple pages.
    // Each record with 200-byte payload = ~231 bytes. 8192/231 ≈ 35 per page.
    // 100 records should span ~3 pages.
    var payload: [200]u8 = undefined;
    @memset(&payload, 0xAB);

    var i: u64 = 0;
    while (i < 100) : (i += 1) {
        _ = try wal.append(1, .insert, i, &payload);
    }
    try wal.flush();

    var records_buf: [128]Record = undefined;
    var payload_buf: [24 * 1024]u8 = undefined;
    const decoded = try wal.readFromInto(1, &records_buf, &payload_buf);
    const records = records_buf[0..decoded.records_len];

    try std.testing.expectEqual(@as(usize, 100), records.len);
    try std.testing.expectEqual(@as(u64, 1), records[0].lsn);
    try std.testing.expectEqual(@as(u64, 100), records[99].lsn);
}

test "WAL recover handles deterministic torn-write corruption" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    {
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();

        // Corrupt the first WAL page write deterministically (torn write).
        disk.partialWriteAt(1, Record.header_size / 2);
        _ = try wal.beginTx(1);
        _ = try wal.append(1, .insert, 99, "faulty");
        _ = try wal.commitTx(1);
    }

    disk.crash();

    // Recovery envelope should still be readable; record parsing may stop
    // early due to CRC/truncation, but must not crash.
    var recovered = Wal.init(std.testing.allocator, disk.storage());
    defer recovered.deinit();
    try recovered.recover();

    var records_buf: [8]Record = undefined;
    var payload_buf: [64]u8 = undefined;
    const decoded = try recovered.readFromInto(1, &records_buf, &payload_buf);
    const records = records_buf[0..decoded.records_len];
    try std.testing.expect(records.len <= 3);
}

test "WAL readFromInto decodes records with caller-owned buffers" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    _ = try wal.beginTx(1);
    _ = try wal.append(1, .insert, 11, "abc");
    _ = try wal.commitTx(1);

    var records_buf: [8]Record = undefined;
    var payload_buf: [64]u8 = undefined;
    const result = try wal.readFromInto(1, &records_buf, &payload_buf);

    try std.testing.expectEqual(@as(usize, 3), result.records_len);
    try std.testing.expectEqual(RecordType.tx_begin, records_buf[0].record_type);
    try std.testing.expectEqual(RecordType.insert, records_buf[1].record_type);
    try std.testing.expectEqualSlices(u8, "abc", records_buf[1].payload);
    try std.testing.expectEqual(RecordType.tx_commit, records_buf[2].record_type);
}

test "WAL readFromInto returns RecordBufferTooSmall" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    _ = try wal.beginTx(1);
    _ = try wal.append(1, .insert, 22, "x");
    _ = try wal.commitTx(1);

    var tiny_records: [2]Record = undefined;
    var payload_buf: [64]u8 = undefined;
    try std.testing.expectError(error.RecordBufferTooSmall, wal.readFromInto(1, &tiny_records, &payload_buf));
}

test "WAL readFromInto returns PayloadBufferTooSmall" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    var payload: [40]u8 = undefined;
    @memset(&payload, 0xAA);

    _ = try wal.beginTx(1);
    _ = try wal.append(1, .insert, 33, &payload);
    _ = try wal.commitTx(1);

    var records_buf: [8]Record = undefined;
    var tiny_payload: [8]u8 = undefined;
    try std.testing.expectError(error.PayloadBufferTooSmall, wal.readFromInto(1, &records_buf, &tiny_payload));
}
