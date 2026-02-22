//! Temporary spill-page storage primitives for query-scoped intermediate data.
//!
//! Responsibilities in this file:
//! - Defines the on-page temp chunk format (`TempHeader` + payload bytes).
//! - Provides read/write helpers for a single temp page (`TempPage`).
//! - Defines a per-query-slot page-id allocator for temp regions (`TempPageAllocator`).
//! - Provides a `TempStorageManager` that owns storage I/O for temp pages.
//!
//! Why this exists:
//! - Sort, group, and join operators may need to spill intermediate data to disk
//!   when working sets exceed in-memory capacity.
//! - Temp pages are ephemeral and query-scoped: they bypass buffer pool and WAL.
//! - Direct Storage interface access enables deterministic fault injection in simulation.
//!
//! How it works:
//! - Each query slot gets a disjoint sub-region of temp page IDs.
//! - `TempStorageManager` reads/writes temp pages directly through `Storage` (no buffer pool).
//! - On query completion, the allocator resets to reclaim all temp pages in O(1).
//! - Crash recovery simply drops all temp data (no durability).
//!
//! Boundaries and non-responsibilities:
//! - This file does not decide *when* to spill; operator logic does that.
//! - This file does not provide cross-query temp data sharing.
//! - No WAL logging, no transaction semantics, no undo entries for temp data.
//!
//! Contributor notes:
//! - Treat `TempHeader` as an on-disk contract. Any incompatible change must
//!   be versioned and accompanied by upgrade/read-compat handling.
//! - Keep validation fail-closed (`InvalidPageFormat` / `UnsupportedPageVersion`)
//!   instead of accepting ambiguous bytes.
//! - Preserve allocator region disjointness so temp page ids do not collide
//!   with other reserved page-id spaces.
const std = @import("std");
const page_mod = @import("page.zig");
const io_mod = @import("io.zig");

const Page = page_mod.Page;
const content_size = page_mod.content_size;
const page_size = io_mod.page_size;
const Storage = io_mod.Storage;

// ---------------------------------------------------------------------------
// Region constants
// ---------------------------------------------------------------------------

/// Dedicated page-id region for temp/spill pages.
/// Disjoint from heap (low), WAL (1M), and overflow (10M) ranges.
pub const default_region_start_page_id: u64 = 20_000_000;

/// Default temp page budget per query slot (1024 pages = 8 MB at 8 KB/page).
pub const default_pages_per_query_slot: u64 = 1024;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

pub const TempAllocatorError = error{
    InvalidRegion,
    RegionExhausted,
};

pub const TempPageError = error{
    PageFull,
    InvalidPageFormat,
    UnsupportedPageVersion,
};

pub const TempStorageError = error{
    ReadError,
    WriteError,
};

// ---------------------------------------------------------------------------
// TempPageAllocator — per-query-slot monotonic page-id allocator
// ---------------------------------------------------------------------------

pub const TempPageAllocator = struct {
    region_start_page_id: u64,
    region_end_page_id: u64, // exclusive
    next_page_id: u64,

    /// Create an allocator for a specific query slot.
    /// Each slot gets the range `[region_start + slot * pages_per_slot,
    ///                            region_start + (slot + 1) * pages_per_slot)`.
    pub fn initForSlot(
        slot_index: u16,
        pages_per_slot: u64,
        region_start: u64,
    ) TempAllocatorError!TempPageAllocator {
        if (pages_per_slot == 0) return error.InvalidRegion;
        const slot_offset = std.math.mul(
            u64,
            @as(u64, slot_index),
            pages_per_slot,
        ) catch return error.InvalidRegion;
        const slot_start = std.math.add(u64, region_start, slot_offset) catch
            return error.InvalidRegion;
        const slot_end = std.math.add(u64, slot_start, pages_per_slot) catch
            return error.InvalidRegion;
        return .{
            .region_start_page_id = slot_start,
            .region_end_page_id = slot_end,
            .next_page_id = slot_start,
        };
    }

    /// Allocate the next page ID. Returns `RegionExhausted` when the slot
    /// region has no more capacity.
    pub fn allocate(self: *TempPageAllocator) TempAllocatorError!u64 {
        if (self.next_page_id >= self.region_end_page_id) {
            return error.RegionExhausted;
        }
        const page_id = self.next_page_id;
        self.next_page_id += 1;
        return page_id;
    }

    /// O(1) bulk reclaim: reset allocator to the beginning of the region.
    /// All previously allocated page IDs become stale.
    pub fn reset(self: *TempPageAllocator) void {
        self.next_page_id = self.region_start_page_id;
    }

    /// Number of pages currently allocated (not yet reclaimed).
    pub fn pagesInUse(self: *const TempPageAllocator) u64 {
        return self.next_page_id - self.region_start_page_id;
    }

    /// Total capacity of this slot's region.
    pub fn capacity(self: *const TempPageAllocator) u64 {
        return self.region_end_page_id - self.region_start_page_id;
    }

    /// Whether the given page ID falls within this allocator's region.
    pub fn ownsPageId(self: *const TempPageAllocator, page_id: u64) bool {
        return page_id >= self.region_start_page_id and page_id < self.region_end_page_id;
    }
};

// ---------------------------------------------------------------------------
// TempPage — on-page format
// ---------------------------------------------------------------------------

const TempHeader = struct {
    format_magic: u16,
    format_version: u8,
    reserved: u8,
    next_page_id: u64,
    payload_len: u16,

    const size = 14;
    const format_magic_value: u16 = 0x5432; // "T2"
    const format_version_value: u8 = 1;

    fn read(content: *const [content_size]u8) TempHeader {
        return .{
            .format_magic = std.mem.littleToNative(
                u16,
                std.mem.bytesAsValue(u16, content[0..2]).*,
            ),
            .format_version = content[2],
            .reserved = content[3],
            .next_page_id = std.mem.littleToNative(
                u64,
                std.mem.bytesAsValue(u64, content[4..12]).*,
            ),
            .payload_len = std.mem.littleToNative(
                u16,
                std.mem.bytesAsValue(u16, content[12..14]).*,
            ),
        };
    }

    fn write(self: TempHeader, content: *[content_size]u8) void {
        @memcpy(
            content[0..2],
            std.mem.asBytes(&std.mem.nativeToLittle(u16, self.format_magic)),
        );
        content[2] = self.format_version;
        content[3] = self.reserved;
        @memcpy(
            content[4..12],
            std.mem.asBytes(&std.mem.nativeToLittle(u64, self.next_page_id)),
        );
        @memcpy(
            content[12..14],
            std.mem.asBytes(&std.mem.nativeToLittle(u16, self.payload_len)),
        );
    }
};

/// Temp page stores a single payload chunk and a next-page pointer.
/// Chaining semantics are caller-defined.
pub const TempPage = struct {
    pub const null_page_id: u64 = 0;

    pub fn max_payload_len() u16 {
        return content_size - TempHeader.size;
    }

    /// Initialize a page as a temp page with empty payload.
    pub fn init(page: *Page) void {
        std.debug.assert(page.header.page_type == .free or page.header.page_type == .temp);
        page.header.page_type = .temp;
        @memset(&page.content, 0);
        const header: TempHeader = .{
            .format_magic = TempHeader.format_magic_value,
            .format_version = TempHeader.format_version_value,
            .reserved = 0,
            .next_page_id = null_page_id,
            .payload_len = 0,
        };
        header.write(&page.content);
    }

    /// Write a payload chunk and set the chain pointer.
    pub fn writeChunk(page: *Page, payload: []const u8, next_page_id: u64) TempPageError!void {
        std.debug.assert(page.header.page_type == .temp);
        if (payload.len > max_payload_len()) return error.PageFull;

        var header = try readHeader(page);
        header.next_page_id = next_page_id;
        header.payload_len = @intCast(payload.len);
        header.write(&page.content);

        if (payload.len > 0) {
            @memcpy(page.content[TempHeader.size..][0..payload.len], payload);
        }
        // Zero-fill trailing bytes for deterministic content.
        const payload_end = TempHeader.size + payload.len;
        if (payload_end < content_size) {
            @memset(page.content[payload_end..content_size], 0);
        }
    }

    /// Read the payload chunk and chain pointer.
    pub fn readChunk(page: *const Page) TempPageError!struct {
        payload: []const u8,
        next_page_id: u64,
    } {
        std.debug.assert(page.header.page_type == .temp);
        const header = try readHeader(page);
        const payload_end = TempHeader.size + header.payload_len;
        std.debug.assert(payload_end <= content_size);
        return .{
            .payload = page.content[TempHeader.size..payload_end],
            .next_page_id = header.next_page_id,
        };
    }

    fn readHeader(page: *const Page) TempPageError!TempHeader {
        std.debug.assert(page.header.page_type == .temp);
        const header = TempHeader.read(&page.content);
        if (header.format_magic != TempHeader.format_magic_value) return error.InvalidPageFormat;
        if (header.format_version != TempHeader.format_version_value) {
            return error.UnsupportedPageVersion;
        }
        if (header.payload_len > max_payload_len()) return error.InvalidPageFormat;
        return header;
    }
};

// ---------------------------------------------------------------------------
// TempSpillStats — telemetry counters
// ---------------------------------------------------------------------------

pub const TempSpillStats = struct {
    temp_pages_allocated: u32 = 0,
    temp_pages_reclaimed: u32 = 0,
    temp_bytes_written: u64 = 0,
    temp_bytes_read: u64 = 0,
};

// ---------------------------------------------------------------------------
// TempStorageManager — I/O coordinator
// ---------------------------------------------------------------------------

pub const TempStorageManager = struct {
    allocator: TempPageAllocator,
    storage: Storage,
    stats: TempSpillStats = .{},

    pub fn init(
        slot_index: u16,
        storage: Storage,
        pages_per_slot: u64,
        region_start: u64,
    ) TempAllocatorError!TempStorageManager {
        return .{
            .allocator = try TempPageAllocator.initForSlot(
                slot_index,
                pages_per_slot,
                region_start,
            ),
            .storage = storage,
        };
    }

    pub fn initDefault(slot_index: u16, storage: Storage) TempAllocatorError!TempStorageManager {
        return init(
            slot_index,
            storage,
            default_pages_per_query_slot,
            default_region_start_page_id,
        );
    }

    /// Allocate a new temp page, write payload to it, and persist to storage.
    /// Returns the allocated page ID.
    pub fn allocateAndWrite(
        self: *TempStorageManager,
        payload: []const u8,
        next_page_id: u64,
    ) (TempAllocatorError || TempPageError || TempStorageError)!u64 {
        const page_id = try self.allocator.allocate();
        self.stats.temp_pages_allocated += 1;

        var page = Page.init(page_id, .temp);
        TempPage.init(&page);
        try TempPage.writeChunk(&page, payload, next_page_id);

        var raw: [page_size]u8 = undefined;
        page.serialize(&raw);
        self.storage.write(page_id, &raw) catch return error.WriteError;
        self.stats.temp_bytes_written += page_size;
        return page_id;
    }

    /// Read a temp page from storage.
    /// The returned `page` must stay alive while the caller uses `payload`.
    pub fn readPage(
        self: *TempStorageManager,
        page_id: u64,
    ) (TempPageError || TempStorageError || page_mod.PageDeserializeError)!struct {
        payload: []const u8,
        next_page_id: u64,
        page: Page,
    } {
        var raw: [page_size]u8 = undefined;
        self.storage.read(page_id, &raw) catch return error.ReadError;
        self.stats.temp_bytes_read += page_size;

        const page = try Page.deserialize(&raw);
        const chunk = try TempPage.readChunk(&page);
        return .{
            .payload = chunk.payload,
            .next_page_id = chunk.next_page_id,
            .page = page,
        };
    }

    /// O(1) bulk reclaim of all temp pages for this query slot.
    /// Previously allocated page IDs become stale on storage but will be
    /// overwritten on next use.
    pub fn reset(self: *TempStorageManager) void {
        const in_use = self.allocator.pagesInUse();
        self.stats.temp_pages_reclaimed +|= @intCast(in_use);
        self.allocator.reset();
    }

    pub fn pagesInUse(self: *const TempStorageManager) u64 {
        return self.allocator.pagesInUse();
    }

    pub fn snapshotStats(self: *const TempStorageManager) TempSpillStats {
        return self.stats;
    }
};

// ===========================================================================
// Tests
// ===========================================================================

test "temp page init writes header metadata" {
    var page = Page.init(0, .free);
    TempPage.init(&page);

    try std.testing.expectEqual(page_mod.PageType.temp, page.header.page_type);
    const chunk = try TempPage.readChunk(&page);
    try std.testing.expectEqual(@as(usize, 0), chunk.payload.len);
    try std.testing.expectEqual(TempPage.null_page_id, chunk.next_page_id);
}

test "temp page write/read chunk roundtrip" {
    var page = Page.init(1, .free);
    TempPage.init(&page);

    const payload = "hello temp spill";
    try TempPage.writeChunk(&page, payload, 42);
    const chunk = try TempPage.readChunk(&page);
    try std.testing.expectEqualSlices(u8, payload, chunk.payload);
    try std.testing.expectEqual(@as(u64, 42), chunk.next_page_id);
}

test "temp page write rejects payload bigger than page capacity" {
    var page = Page.init(2, .free);
    TempPage.init(&page);

    const too_large = [_]u8{0xAB} ** (TempPage.max_payload_len() + 1);
    const result = TempPage.writeChunk(&page, too_large[0..], TempPage.null_page_id);
    try std.testing.expectError(TempPageError.PageFull, result);
}

test "temp page read rejects invalid format magic" {
    var page = Page.init(3, .free);
    TempPage.init(&page);
    // Corrupt magic bytes.
    page.content[0] = 0x00;
    page.content[1] = 0x00;

    try std.testing.expectError(TempPageError.InvalidPageFormat, TempPage.readChunk(&page));
}

test "temp page read rejects unsupported version" {
    var page = Page.init(4, .free);
    TempPage.init(&page);
    // Corrupt version byte.
    page.content[2] = 0xFF;

    try std.testing.expectError(TempPageError.UnsupportedPageVersion, TempPage.readChunk(&page));
}

test "temp page-id allocator allocates monotonically and exhausts fail-closed" {
    var alloc = try TempPageAllocator.initForSlot(0, 2, 200);
    try std.testing.expectEqual(@as(u64, 200), try alloc.allocate());
    try std.testing.expectEqual(@as(u64, 201), try alloc.allocate());
    try std.testing.expectError(error.RegionExhausted, alloc.allocate());
}

test "temp page-id allocator rejects invalid region" {
    try std.testing.expectError(error.InvalidRegion, TempPageAllocator.initForSlot(0, 0, 100));
}

test "temp page-id allocator reset reclaims all pages" {
    var alloc = try TempPageAllocator.initForSlot(0, 4, 300);
    _ = try alloc.allocate();
    _ = try alloc.allocate();
    try std.testing.expectEqual(@as(u64, 2), alloc.pagesInUse());

    alloc.reset();
    try std.testing.expectEqual(@as(u64, 0), alloc.pagesInUse());

    // Can allocate again after reset, same page IDs reused.
    try std.testing.expectEqual(@as(u64, 300), try alloc.allocate());
    try std.testing.expectEqual(@as(u64, 301), try alloc.allocate());
}

test "temp page-id allocator per-slot regions are disjoint" {
    const pages_per_slot: u64 = 8;
    const region_start: u64 = 1000;

    var slot0 = try TempPageAllocator.initForSlot(0, pages_per_slot, region_start);
    var slot1 = try TempPageAllocator.initForSlot(1, pages_per_slot, region_start);

    const id0 = try slot0.allocate();
    const id1 = try slot1.allocate();

    // Slot 0 starts at 1000, slot 1 starts at 1008.
    try std.testing.expectEqual(@as(u64, 1000), id0);
    try std.testing.expectEqual(@as(u64, 1008), id1);

    // Neither slot owns the other's page IDs.
    try std.testing.expect(!slot0.ownsPageId(id1));
    try std.testing.expect(!slot1.ownsPageId(id0));
    try std.testing.expect(slot0.ownsPageId(id0));
    try std.testing.expect(slot1.ownsPageId(id1));
}

test "temp storage manager write then read roundtrip" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var mgr = try TempStorageManager.init(0, disk.storage(), 16, 5000);
    const payload = "spill data chunk 1";
    const page_id = try mgr.allocateAndWrite(payload, TempPage.null_page_id);

    try std.testing.expectEqual(@as(u64, 5000), page_id);
    try std.testing.expectEqual(@as(u64, 1), mgr.pagesInUse());

    const read_result = try mgr.readPage(page_id);
    try std.testing.expectEqualSlices(u8, payload, read_result.payload);
    try std.testing.expectEqual(TempPage.null_page_id, read_result.next_page_id);
}

test "temp storage manager chained write then sequential read" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var mgr = try TempStorageManager.init(0, disk.storage(), 16, 6000);

    // Write chain: page2 → page1 → null.
    const id1 = try mgr.allocateAndWrite("chunk-1", TempPage.null_page_id);
    const id2 = try mgr.allocateAndWrite("chunk-2", id1);

    // Read back chain from id2.
    const r2 = try mgr.readPage(id2);
    try std.testing.expectEqualSlices(u8, "chunk-2", r2.payload);
    try std.testing.expectEqual(id1, r2.next_page_id);

    const r1 = try mgr.readPage(r2.next_page_id);
    try std.testing.expectEqualSlices(u8, "chunk-1", r1.payload);
    try std.testing.expectEqual(TempPage.null_page_id, r1.next_page_id);
}

test "temp storage manager reset reclaims pages" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var mgr = try TempStorageManager.init(0, disk.storage(), 16, 7000);
    _ = try mgr.allocateAndWrite("a", TempPage.null_page_id);
    _ = try mgr.allocateAndWrite("b", TempPage.null_page_id);
    try std.testing.expectEqual(@as(u64, 2), mgr.pagesInUse());

    mgr.reset();
    try std.testing.expectEqual(@as(u64, 0), mgr.pagesInUse());

    // Stats reflect allocation and reclaim.
    const stats = mgr.snapshotStats();
    try std.testing.expectEqual(@as(u32, 2), stats.temp_pages_allocated);
    try std.testing.expectEqual(@as(u32, 2), stats.temp_pages_reclaimed);
}

test "temp storage manager tracks stats correctly" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var mgr = try TempStorageManager.init(0, disk.storage(), 16, 8000);
    const id = try mgr.allocateAndWrite("payload", TempPage.null_page_id);
    _ = try mgr.readPage(id);

    const stats = mgr.snapshotStats();
    try std.testing.expectEqual(@as(u32, 1), stats.temp_pages_allocated);
    try std.testing.expectEqual(@as(u64, page_size), stats.temp_bytes_written);
    try std.testing.expectEqual(@as(u64, page_size), stats.temp_bytes_read);
    try std.testing.expectEqual(@as(u32, 0), stats.temp_pages_reclaimed);
}

test "temp storage manager survives write fault" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    // Fail the first write.
    disk.failWriteAt(1);

    var mgr = try TempStorageManager.init(0, disk.storage(), 16, 9000);
    const result = mgr.allocateAndWrite("data", TempPage.null_page_id);
    try std.testing.expectError(error.WriteError, result);

    // Allocator consumed a page ID, but no successful write occurred.
    try std.testing.expectEqual(@as(u32, 1), mgr.snapshotStats().temp_pages_allocated);
    try std.testing.expectEqual(@as(u64, 0), mgr.snapshotStats().temp_bytes_written);
}

test "temp storage manager survives read fault" {
    const disk_mod = @import("../simulator/disk.zig");
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var mgr = try TempStorageManager.init(0, disk.storage(), 16, 10_000);
    const id = try mgr.allocateAndWrite("data", TempPage.null_page_id);

    // Fail the next read.
    disk.failReadAt(disk.reads + 1);
    const result = mgr.readPage(id);
    try std.testing.expectError(error.ReadError, result);

    // Write stats still reflect the successful write.
    try std.testing.expectEqual(@as(u64, page_size), mgr.snapshotStats().temp_bytes_written);
    // Read stats: no successful read counted.
    try std.testing.expectEqual(@as(u64, 0), mgr.snapshotStats().temp_bytes_read);
}
