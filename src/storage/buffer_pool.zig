const std = @import("std");
const io = @import("io.zig");
const page_mod = @import("page.zig");
const wal_mod = @import("wal.zig");

const Page = page_mod.Page;
const PageHeader = page_mod.PageHeader;
const page_size = io.page_size;
const Wal = wal_mod.Wal;

pub const BufferPoolError = error{
    AllFramesPinned,
    ChecksumMismatch,
    StorageRead,
    StorageWrite,
    StorageFsync,
    WalNotFlushed,
};

/// A frame in the buffer pool holds one page in memory.
const Frame = struct {
    page: Page,
    page_id: u64,
    valid: bool,
    dirty: bool,
    pin_count: u32,
    ref_bit: bool, // for clock sweep
};

/// Fixed-size buffer pool with clock-sweep eviction.
///
/// All page access goes through this pool. Pages are loaded from storage
/// on demand and written back when dirty. Eviction uses the clock algorithm
/// (approximation of LRU, same as PostgreSQL).
pub const BufferPool = struct {
    frames: []Frame,
    /// Maps page_id -> frame index for O(1) lookup.
    page_table: std.AutoHashMap(u64, usize),
    storage: io.Storage,
    /// Optional WAL reference. When set, the buffer pool enforces the WAL
    /// protocol: a dirty page cannot be flushed to disk until its LSN has
    /// been durably flushed to the WAL.
    wal: ?*Wal = null,
    clock_hand: usize,
    allocator: std.mem.Allocator,

    // Stats
    hits: u64 = 0,
    misses: u64 = 0,
    evictions: u64 = 0,
    flushes: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, storage: io.Storage, num_frames: usize) !BufferPool {
        const frames = try allocator.alloc(Frame, num_frames);
        for (frames) |*f| {
            f.* = .{
                .page = undefined,
                .page_id = 0,
                .valid = false,
                .dirty = false,
                .pin_count = 0,
                .ref_bit = false,
            };
        }

        return .{
            .frames = frames,
            .page_table = std.AutoHashMap(u64, usize).init(allocator),
            .storage = storage,
            .clock_hand = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BufferPool) void {
        self.page_table.deinit();
        self.allocator.free(self.frames);
    }

    /// Pin a page: load it into the buffer pool if not present, increment
    /// its pin count, and return a pointer to it. The page stays in memory
    /// (cannot be evicted) until unpinned.
    pub fn pin(self: *BufferPool, page_id: u64) BufferPoolError!*Page {
        // Fast path: page already in pool.
        if (self.page_table.get(page_id)) |frame_idx| {
            var frame = &self.frames[frame_idx];
            frame.pin_count += 1;
            frame.ref_bit = true;
            self.hits += 1;
            return &frame.page;
        }

        // Slow path: load from storage.
        self.misses += 1;
        const frame_idx = try self.findVictim();
        var frame = &self.frames[frame_idx];

        // Evict old page if present.
        if (frame.valid) {
            if (frame.dirty) {
                try self.flushFrame(frame_idx);
            }
            _ = self.page_table.remove(frame.page_id);
            self.evictions += 1;
        }

        // Load new page from disk.
        var raw: [page_size]u8 = undefined;
        self.storage.read(page_id, &raw) catch return error.StorageRead;
        frame.page = Page.deserialize(&raw) catch blk: {
            // If checksum fails, the page might be new (all zeroes).
            // All-zero pages are valid — they're just uninitialized.
            const all_zero = for (raw) |b| {
                if (b != 0) break false;
            } else true;

            if (!all_zero) return error.ChecksumMismatch;
            break :blk Page.init(page_id, .free);
        };

        frame.page_id = page_id;
        frame.valid = true;
        frame.dirty = false;
        frame.pin_count = 1;
        frame.ref_bit = true;

        self.page_table.put(page_id, frame_idx) catch return error.StorageRead;

        return &frame.page;
    }

    /// Unpin a page. If dirty is true, mark the page as dirty (needs flushing).
    /// The page can be evicted once its pin count reaches zero.
    pub fn unpin(self: *BufferPool, page_id: u64, dirty: bool) void {
        const frame_idx = self.page_table.get(page_id) orelse return;
        var frame = &self.frames[frame_idx];
        if (frame.pin_count > 0) {
            frame.pin_count -= 1;
        }
        if (dirty) {
            frame.dirty = true;
        }
    }

    /// Returns true if the page is currently pinned (pin_count > 0).
    pub fn isPinned(self: *BufferPool, page_id: u64) bool {
        const frame_idx = self.page_table.get(page_id) orelse return false;
        return self.frames[frame_idx].pin_count > 0;
    }

    /// Flush a specific page to disk.
    pub fn flush(self: *BufferPool, page_id: u64) BufferPoolError!void {
        const frame_idx = self.page_table.get(page_id) orelse return;
        try self.flushFrame(frame_idx);
    }

    /// Flush all dirty pages to disk and fsync.
    pub fn flushAll(self: *BufferPool) BufferPoolError!void {
        for (self.frames, 0..) |*frame, i| {
            if (frame.valid and frame.dirty) {
                try self.flushFrame(i);
            }
        }
        self.storage.fsync() catch return error.StorageFsync;
    }

    fn flushFrame(self: *BufferPool, frame_idx: usize) BufferPoolError!void {
        var frame = &self.frames[frame_idx];
        if (!frame.dirty) return;

        // WAL protocol: page cannot be flushed until its LSN is WAL-flushed.
        if (self.wal) |wal| {
            if (frame.page.header.lsn > wal.flushed_lsn) {
                return error.WalNotFlushed;
            }
        }

        var raw: [page_size]u8 = undefined;
        frame.page.serialize(&raw);
        self.storage.write(frame.page_id, &raw) catch return error.StorageWrite;
        frame.dirty = false;
        self.flushes += 1;
    }

    /// Clock-sweep eviction: find an unpinned frame to reuse.
    fn findVictim(self: *BufferPool) BufferPoolError!usize {
        const n = self.frames.len;
        // Two full sweeps: first clears ref bits, second finds victim.
        var attempts: usize = 0;
        while (attempts < 2 * n) : (attempts += 1) {
            var frame = &self.frames[self.clock_hand];
            const idx = self.clock_hand;
            self.clock_hand = (self.clock_hand + 1) % n;

            if (!frame.valid) {
                return idx;
            }
            if (frame.pin_count == 0) {
                if (frame.ref_bit) {
                    frame.ref_bit = false;
                } else {
                    return idx;
                }
            }
        }
        return error.AllFramesPinned;
    }
};

// --- Tests ---

test "pin loads page and unpin releases it" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 4);
    defer pool.deinit();

    const page = try pool.pin(0);
    try std.testing.expectEqual(@as(u64, 0), page.header.page_id);
    pool.unpin(0, false);
}

test "pin same page twice increments pin count" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 4);
    defer pool.deinit();

    const p1 = try pool.pin(0);
    const p2 = try pool.pin(0);
    // Same page, same pointer.
    try std.testing.expect(p1 == p2);
    try std.testing.expectEqual(@as(u64, 1), pool.hits);

    pool.unpin(0, false);
    pool.unpin(0, false);
}

test "dirty page is flushed to disk" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 4);
    defer pool.deinit();

    const page = try pool.pin(0);
    page.header.page_type = .heap;
    page.header.lsn = 42;
    @memset(&page.content, 0xBE);
    pool.unpin(0, true);

    try pool.flush(0);
    const s = disk.storage();

    // Verify it's on disk by reading directly.
    var raw: [page_size]u8 = undefined;
    try s.read(0, &raw);
    const restored = try Page.deserialize(&raw);
    try std.testing.expectEqual(page_mod.PageType.heap, restored.header.page_type);
    try std.testing.expectEqual(@as(u64, 42), restored.header.lsn);
}

test "eviction works when pool is full" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    // Pool with only 2 frames.
    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 2);
    defer pool.deinit();

    // Pin and unpin pages 0 and 1.
    _ = try pool.pin(0);
    pool.unpin(0, false);
    _ = try pool.pin(1);
    pool.unpin(1, false);

    // Pin page 2 — must evict one of the previous pages.
    _ = try pool.pin(2);
    pool.unpin(2, false);

    try std.testing.expectEqual(@as(u64, 1), pool.evictions);
}

test "all frames pinned returns error" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 2);
    defer pool.deinit();

    _ = try pool.pin(0);
    _ = try pool.pin(1);
    // Both pinned — no room.
    const result = pool.pin(2);
    try std.testing.expectError(BufferPoolError.AllFramesPinned, result);

    pool.unpin(0, false);
    pool.unpin(1, false);
}

test "dirty eviction writes page to disk" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 1);
    defer pool.deinit();

    // Write to page 0.
    const p0 = try pool.pin(0);
    p0.header.page_type = .heap;
    p0.header.lsn = 10;
    @memset(&p0.content, 0xCC);
    pool.unpin(0, true); // dirty

    // Pin page 1 — forces eviction of dirty page 0.
    _ = try pool.pin(1);
    pool.unpin(1, false);

    // Page 0 should now be on disk (pending write).
    try std.testing.expect(disk.hasPending(0));
}

test "flushAll writes all dirty pages and fsyncs" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 4);
    defer pool.deinit();

    const p0 = try pool.pin(0);
    @memset(&p0.content, 0x01);
    pool.unpin(0, true);

    const p1 = try pool.pin(1);
    @memset(&p1.content, 0x02);
    pool.unpin(1, true);

    try pool.flushAll();

    // Both pages should be durable.
    try std.testing.expect(disk.isDurable(0));
    try std.testing.expect(disk.isDurable(1));
}

test "cache hit tracking" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 4);
    defer pool.deinit();

    _ = try pool.pin(0); // miss
    pool.unpin(0, false);
    _ = try pool.pin(0); // hit
    pool.unpin(0, false);
    _ = try pool.pin(1); // miss
    pool.unpin(1, false);

    try std.testing.expectEqual(@as(u64, 2), pool.misses);
    try std.testing.expectEqual(@as(u64, 1), pool.hits);
}

test "WAL protocol: flush blocked until WAL is flushed" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 4);
    defer pool.deinit();
    pool.wal = &wal;

    // Write a WAL record (LSN 1) but don't flush the WAL.
    _ = try wal.append(1, .insert, 0, "data");

    // Modify page through buffer pool, set its LSN to 1.
    const page = try pool.pin(0);
    page.header.lsn = 1;
    @memset(&page.content, 0xAA);
    pool.unpin(0, true);

    // Flushing the page should fail — WAL not flushed yet.
    const result = pool.flush(0);
    try std.testing.expectError(BufferPoolError.WalNotFlushed, result);

    // Now flush the WAL.
    try wal.flush();
    try std.testing.expectEqual(@as(u64, 1), wal.flushed_lsn);

    // Now flushing the page should succeed.
    try pool.flush(0);
    try std.testing.expect(disk.hasPending(0));
}

test "WAL protocol: page with LSN 0 flushes without WAL" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var wal = Wal.init(std.testing.allocator, disk.storage());
    defer wal.deinit();

    var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 4);
    defer pool.deinit();
    pool.wal = &wal;

    // Page with LSN 0 (no WAL record) should flush fine.
    const page = try pool.pin(0);
    @memset(&page.content, 0xBB);
    pool.unpin(0, true);

    try pool.flush(0);
    try std.testing.expect(disk.hasPending(0));
}
