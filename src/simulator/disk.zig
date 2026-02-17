const std = @import("std");
const io = @import("../storage/io.zig");

const page_size = io.page_size;

/// Deterministic simulated disk for testing. All writes go to a pending
/// buffer; only fsync persists them to the durable store. This accurately
/// models real disk semantics where data can be lost on crash if not fsynced.
///
/// Fault injection (partial writes, read errors, bit flips) is not yet
/// implemented — this is the correct-behavior baseline.
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
        self.pending.put(page_id, data.*) catch return error.WriteError;
    }

    fn fsyncImpl(ptr: *anyopaque) io.StorageError!void {
        const self: *SimulatedDisk = @ptrCast(@alignCast(ptr));
        self.fsyncs += 1;

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
