//! Deterministic simulated disk with crash/fault injection.
//!
//! Responsibilities in this file:
//! - Implements the `Storage` interface with durable vs pending write tiers.
//! - Models fsync durability boundaries and crash loss of unflushed writes.
//! - Provides deterministic one-shot fault injection for read/write/fsync paths.
//! - Exposes helpers for crash/restart and durability assertions in tests.
const std = @import("std");
const io = @import("../storage/io.zig");

const page_size = io.page_size;

/// Deterministic simulated disk for testing. All writes go to a pending
/// buffer; only fsync persists them to the durable store. This accurately
/// models real disk semantics where data can be lost on crash if not fsynced.
///
/// Fault injection supports deterministic one-shot failures on the Nth
/// read/write/fsync operation.
pub const SimulatedDisk = struct {
    /// Durable pages — survive crashes.
    pages: std.AutoHashMap(u64, [page_size]u8),
    /// Pending writes — lost on crash.
    pending: std.AutoHashMap(u64, [page_size]u8),
    allocator: std.mem.Allocator,

    // Stats
    reads: u64 = 0,
    writes: u64 = 0,
    fsyncs: u64 = 0,
    fail_read_at: ?u64 = null,
    fail_write_at: ?u64 = null,
    fail_fsync_at: ?u64 = null,
    partial_write_at: ?u64 = null,
    partial_write_prefix_bytes: usize = 0,
    bitflip_write_at: ?u64 = null,
    bitflip_write_offset: usize = 0,
    bitflip_write_mask: u8 = 0,

    pub fn init(allocator: std.mem.Allocator) SimulatedDisk {
        return .{
            .pages = std.AutoHashMap(u64, [page_size]u8).init(allocator),
            .pending = std.AutoHashMap(u64, [page_size]u8).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SimulatedDisk) void {
        self.pages.deinit();
        self.pending.deinit();
    }

    pub fn storage(self: *SimulatedDisk) io.Storage {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = io.Storage.VTable{
        .read = &readImpl,
        .write = &writeImpl,
        .fsync = &fsyncImpl,
    };

    fn readImpl(ptr: *anyopaque, page_id: u64, buf: *[page_size]u8) io.StorageError!void {
        const self: *SimulatedDisk = @ptrCast(@alignCast(ptr));
        self.reads += 1;
        if (self.fail_read_at) |n| {
            if (self.reads == n) {
                self.fail_read_at = null;
                return error.ReadError;
            }
        }

        // Pending writes are visible to reads (models OS page cache behavior).
        if (self.pending.get(page_id)) |data| {
            @memcpy(buf, &data);
            return;
        }
        if (self.pages.get(page_id)) |data| {
            @memcpy(buf, &data);
            return;
        }
        // Page never written — return zeroes.
        @memset(buf, 0);
    }

    fn writeImpl(ptr: *anyopaque, page_id: u64, data: *const [page_size]u8) io.StorageError!void {
        const self: *SimulatedDisk = @ptrCast(@alignCast(ptr));
        self.writes += 1;
        if (self.fail_write_at) |n| {
            if (self.writes == n) {
                self.fail_write_at = null;
                return error.WriteError;
            }
        }

        var page_data = data.*;

        if (self.partial_write_at) |n| {
            if (self.writes == n) {
                self.partial_write_at = null;
                const keep_prefix = @min(self.partial_write_prefix_bytes, page_size);
                if (keep_prefix < page_size) {
                    var previous = std.mem.zeroes([page_size]u8);
                    if (self.pending.get(page_id)) |pending_data| {
                        previous = pending_data;
                    } else if (self.pages.get(page_id)) |durable_data| {
                        previous = durable_data;
                    }
                    @memcpy(page_data[keep_prefix..], previous[keep_prefix..]);
                }
            }
        }

        if (self.bitflip_write_at) |n| {
            if (self.writes == n) {
                self.bitflip_write_at = null;
                const offset = @min(self.bitflip_write_offset, page_size - 1);
                page_data[offset] ^= self.bitflip_write_mask;
            }
        }

        self.pending.put(page_id, page_data) catch return error.WriteError;
    }

    fn fsyncImpl(ptr: *anyopaque) io.StorageError!void {
        const self: *SimulatedDisk = @ptrCast(@alignCast(ptr));
        self.fsyncs += 1;
        if (self.fail_fsync_at) |n| {
            if (self.fsyncs == n) {
                self.fail_fsync_at = null;
                return error.FsyncError;
            }
        }

        // Move all pending writes to durable storage.
        var it = self.pending.iterator();
        while (it.next()) |entry| {
            self.pages.put(entry.key_ptr.*, entry.value_ptr.*) catch return error.FsyncError;
        }
        self.pending.clearRetainingCapacity();
    }

    /// Simulate a crash: discard all pending (unfsynced) writes.
    pub fn crash(self: *SimulatedDisk) void {
        self.pending.clearRetainingCapacity();
    }

    /// Returns true if the page exists in durable storage.
    pub fn isDurable(self: *SimulatedDisk, page_id: u64) bool {
        return self.pages.contains(page_id);
    }

    /// Returns true if the page has a pending (unfsynced) write.
    pub fn hasPending(self: *SimulatedDisk, page_id: u64) bool {
        return self.pending.contains(page_id);
    }

    /// Inject a one-shot read failure on the Nth read operation.
    pub fn failReadAt(self: *SimulatedDisk, n: u64) void {
        std.debug.assert(n > 0);
        self.fail_read_at = n;
    }

    /// Inject a one-shot write failure on the Nth write operation.
    pub fn failWriteAt(self: *SimulatedDisk, n: u64) void {
        std.debug.assert(n > 0);
        self.fail_write_at = n;
    }

    /// Inject a one-shot fsync failure on the Nth fsync operation.
    pub fn failFsyncAt(self: *SimulatedDisk, n: u64) void {
        std.debug.assert(n > 0);
        self.fail_fsync_at = n;
    }

    /// Inject a one-shot partial write on the Nth write operation.
    /// Only the first `prefix_bytes` are taken from the incoming page;
    /// remaining bytes retain prior page contents (or zeroes if new page).
    pub fn partialWriteAt(self: *SimulatedDisk, n: u64, prefix_bytes: usize) void {
        std.debug.assert(n > 0);
        self.partial_write_at = n;
        self.partial_write_prefix_bytes = prefix_bytes;
    }

    /// Inject a one-shot byte corruption on the Nth write operation.
    pub fn bitflipWriteAt(self: *SimulatedDisk, n: u64, byte_offset: usize, mask: u8) void {
        std.debug.assert(n > 0);
        self.bitflip_write_at = n;
        self.bitflip_write_offset = byte_offset;
        self.bitflip_write_mask = mask;
    }
};

test "read unwritten page returns zeroes" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();
    const s = disk.storage();

    var buf: [page_size]u8 = undefined;
    try s.read(0, &buf);
    for (buf) |b| {
        try std.testing.expectEqual(@as(u8, 0), b);
    }
}

test "write then read returns written data" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();
    const s = disk.storage();

    var data: [page_size]u8 = undefined;
    @memset(&data, 0xAB);
    try s.write(0, &data);

    var buf: [page_size]u8 = undefined;
    try s.read(0, &buf);
    try std.testing.expectEqualSlices(u8, &data, &buf);
}

test "crash discards unfsynced writes" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();
    const s = disk.storage();

    var data: [page_size]u8 = undefined;
    @memset(&data, 0xAB);
    try s.write(0, &data);

    // Crash before fsync.
    disk.crash();

    var buf: [page_size]u8 = undefined;
    try s.read(0, &buf);
    // Should be zeroes — the write was lost.
    for (buf) |b| {
        try std.testing.expectEqual(@as(u8, 0), b);
    }
}

test "fsync makes writes durable across crash" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();
    const s = disk.storage();

    var data: [page_size]u8 = undefined;
    @memset(&data, 0xAB);
    try s.write(0, &data);
    try s.fsync();

    // Crash after fsync — data should survive.
    disk.crash();

    var buf: [page_size]u8 = undefined;
    try s.read(0, &buf);
    try std.testing.expectEqualSlices(u8, &data, &buf);
}

test "multiple pages independent" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();
    const s = disk.storage();

    var data0: [page_size]u8 = undefined;
    @memset(&data0, 0x01);
    var data1: [page_size]u8 = undefined;
    @memset(&data1, 0x02);

    try s.write(0, &data0);
    try s.write(1, &data1);
    try s.fsync();

    var buf: [page_size]u8 = undefined;
    try s.read(0, &buf);
    try std.testing.expectEqualSlices(u8, &data0, &buf);
    try s.read(1, &buf);
    try std.testing.expectEqualSlices(u8, &data1, &buf);
}

test "overwrite before fsync uses latest" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();
    const s = disk.storage();

    var data1: [page_size]u8 = undefined;
    @memset(&data1, 0x01);
    var data2: [page_size]u8 = undefined;
    @memset(&data2, 0x02);

    try s.write(0, &data1);
    try s.write(0, &data2); // overwrite
    try s.fsync();

    var buf: [page_size]u8 = undefined;
    try s.read(0, &buf);
    try std.testing.expectEqualSlices(u8, &data2, &buf);
}

test "stats track operations" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();
    const s = disk.storage();

    var data: [page_size]u8 = undefined;
    @memset(&data, 0);
    var buf: [page_size]u8 = undefined;

    try s.write(0, &data);
    try s.read(0, &buf);
    try s.fsync();

    try std.testing.expectEqual(@as(u64, 1), disk.reads);
    try std.testing.expectEqual(@as(u64, 1), disk.writes);
    try std.testing.expectEqual(@as(u64, 1), disk.fsyncs);
}

test "deterministic read failure injection" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();
    const s = disk.storage();

    disk.failReadAt(1);
    var buf: [page_size]u8 = undefined;
    try std.testing.expectError(error.ReadError, s.read(0, &buf));

    // One-shot failure should clear; second read succeeds.
    try s.read(0, &buf);
}

test "deterministic write failure injection" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();
    const s = disk.storage();

    disk.failWriteAt(1);
    var data: [page_size]u8 = undefined;
    @memset(&data, 0xAB);
    try std.testing.expectError(error.WriteError, s.write(0, &data));

    // One-shot failure should clear; second write succeeds.
    try s.write(0, &data);
}

test "deterministic fsync failure injection" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();
    const s = disk.storage();

    var data: [page_size]u8 = undefined;
    @memset(&data, 0x11);
    try s.write(0, &data);

    disk.failFsyncAt(1);
    try std.testing.expectError(error.FsyncError, s.fsync());
    try std.testing.expect(disk.hasPending(0));

    // One-shot failure should clear; next fsync succeeds and persists.
    try s.fsync();
    try std.testing.expect(disk.isDurable(0));
}

test "deterministic partial write injection" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();
    const s = disk.storage();

    var baseline: [page_size]u8 = undefined;
    @memset(&baseline, 0x11);
    try s.write(7, &baseline);
    try s.fsync();

    var replacement: [page_size]u8 = undefined;
    @memset(&replacement, 0xAA);

    // Write #2 should be torn: first 128 bytes from replacement, rest preserved.
    disk.partialWriteAt(2, 128);
    try s.write(7, &replacement);
    try s.fsync();

    var out: [page_size]u8 = undefined;
    try s.read(7, &out);
    for (out[0..128]) |b| {
        try std.testing.expectEqual(@as(u8, 0xAA), b);
    }
    for (out[128..]) |b| {
        try std.testing.expectEqual(@as(u8, 0x11), b);
    }

    // One-shot should clear; next write should be complete.
    try s.write(7, &replacement);
    try s.fsync();
    try s.read(7, &out);
    try std.testing.expectEqualSlices(u8, &replacement, &out);
}

test "deterministic bitflip write injection" {
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();
    const s = disk.storage();

    var data: [page_size]u8 = undefined;
    @memset(&data, 0x5A);

    disk.bitflipWriteAt(1, 17, 0x0F);
    try s.write(9, &data);
    try s.fsync();

    var out: [page_size]u8 = undefined;
    try s.read(9, &out);
    try std.testing.expectEqual(@as(u8, 0x5A ^ 0x0F), out[17]);
    try std.testing.expectEqual(@as(u8, 0x5A), out[16]);
    try std.testing.expectEqual(@as(u8, 0x5A), out[18]);

    // One-shot should clear; following write has no corruption.
    try s.write(9, &data);
    try s.fsync();
    try s.read(9, &out);
    try std.testing.expectEqualSlices(u8, &data, &out);
}
