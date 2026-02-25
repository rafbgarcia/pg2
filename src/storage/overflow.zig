//! Overflow storage primitives for large field values.
//!
//! Responsibilities in this file:
//! - Defines the on-page overflow chunk format (`OverflowHeader` + payload bytes).
//! - Provides read/write helpers for a single overflow page (`OverflowPage`).
//! - Defines a dedicated page-id allocator for overflow chains (`PageIdAllocator`).
//! - Tracks chain roots pending reclamation via a bounded FIFO (`ReclaimQueue`).
//!
//! Why this exists:
//! - Row storage can keep small values inline for locality and simplicity.
//! - Larger values (for example long strings above `string_inline_threshold_bytes`)
//!   are split into chunks and linked across overflow pages.
//! - Keeping this logic isolated gives one explicit place for overflow format
//!   validation, version checks, and fail-closed behavior.
//!
//! How it works:
//! - Each overflow page stores:
//!   - a versioned header (`format_magic`, `format_version`, `next_page_id`,
//!     `payload_len`), and
//!   - one payload chunk of up to `OverflowPage.max_payload_len()` bytes.
//! - Chaining is singly linked via `next_page_id`; `0` means end of chain.
//! - Header reads validate magic/version/payload bounds before exposing bytes.
//! - `writeChunk` rewrites header + payload and zero-fills trailing bytes to keep
//!   deterministic page content.
//!
//! Boundaries and non-responsibilities:
//! - This file does not decide *when* to overflow a value; callers do.
//! - This file does not walk full chains, manage transactions, WAL ordering, or
//!   crash recovery policy; those belong to higher storage/MVCC layers.
//! - Reclaim queue entries are chain root page ids only; actual reclamation logic
//!   is external and must consume the queue explicitly.
//!
//! Contributor notes:
//! - Treat `OverflowHeader` as an on-disk contract. Any incompatible change must
//!   be versioned and accompanied by upgrade/read-compat handling.
//! - Keep validation fail-closed (`InvalidPageFormat` /
//!   `UnsupportedPageVersion`) instead of accepting ambiguous bytes.
//! - Preserve allocator region disjointness so overflow page ids do not collide
//!   with other reserved page-id spaces.
const std = @import("std");
const page_mod = @import("page.zig");

const Page = page_mod.Page;
const content_size = page_mod.content_size;

pub const OverflowError = error{
    PageFull,
    InvalidPageFormat,
    UnsupportedPageVersion,
};

pub const OverflowAllocatorError = error{
    InvalidRegion,
    RegionExhausted,
    InvalidPageId,
};

pub const OverflowReclaimError = error{
    InvalidChainRoot,
    QueueFull,
    QueueEmpty,
    DuplicateChainRoot,
};

/// Dedicated page-id region for overflow chains.
/// This region is intentionally disjoint from low heap page-id ranges and
/// from WAL's high page-id region.
pub const default_region_start_page_id: u64 = 10_000_000;
pub const default_region_page_count: u64 = 65_536;
pub const default_region_end_page_id: u64 =
    default_region_start_page_id + default_region_page_count;

/// String payloads at or below this threshold stay inline in row bytes.
pub const string_inline_threshold_bytes: usize = 1024;
pub const reclaim_queue_capacity: usize = 256;

pub const ReclaimEntryState = enum(u8) {
    pending,
    committed,
};

pub const ReclaimQueueEntry = struct {
    first_page_id: u64,
    tx_id: u64,
    state: ReclaimEntryState,
};

pub const PageIdAllocator = struct {
    region_start_page_id: u64 = default_region_start_page_id,
    region_end_page_id: u64 = default_region_end_page_id, // exclusive
    metadata_page_id: u64 = default_region_start_page_id - 1,
    next_page_id: u64 = default_region_start_page_id,
    free_list_head: u64 = 0,
    metadata_loaded: bool = false,

    pub fn initDefault() PageIdAllocator {
        return .{};
    }

    pub fn initWithBounds(
        start_page_id: u64,
        page_count: u64,
    ) OverflowAllocatorError!PageIdAllocator {
        if (page_count == 0 or start_page_id == 0) return error.InvalidRegion;
        const end_page_id = std.math.add(u64, start_page_id, page_count) catch
            return error.InvalidRegion;
        return .{
            .region_start_page_id = start_page_id,
            .region_end_page_id = end_page_id,
            .metadata_page_id = start_page_id - 1,
            .next_page_id = start_page_id,
            .free_list_head = 0,
            .metadata_loaded = false,
        };
    }

    pub fn allocateFresh(self: *PageIdAllocator) OverflowAllocatorError!u64 {
        if (self.next_page_id >= self.region_end_page_id) {
            return error.RegionExhausted;
        }
        const page_id = self.next_page_id;
        self.next_page_id += 1;
        return page_id;
    }

    pub fn markMetadataLoaded(self: *PageIdAllocator) void {
        self.metadata_loaded = true;
    }

    pub fn metadataPageId(self: *const PageIdAllocator) u64 {
        return self.metadata_page_id;
    }

    pub fn hasFreePages(self: *const PageIdAllocator) bool {
        return self.free_list_head != 0;
    }

    pub fn freeListHead(self: *const PageIdAllocator) u64 {
        return self.free_list_head;
    }

    pub fn setAllocatorState(
        self: *PageIdAllocator,
        free_list_head: u64,
        next_page_id: u64,
    ) OverflowAllocatorError!void {
        if (next_page_id < self.firstAllocatablePageId() or next_page_id > self.region_end_page_id) {
            return error.InvalidRegion;
        }
        if (free_list_head != 0 and !self.ownsPageId(free_list_head)) {
            return error.InvalidPageId;
        }
        self.free_list_head = free_list_head;
        self.next_page_id = next_page_id;
        self.metadata_loaded = true;
    }

    pub fn popFreeListHead(self: *PageIdAllocator, new_head: u64) OverflowAllocatorError!u64 {
        const head = self.free_list_head;
        if (head == 0) return error.RegionExhausted;
        if (!self.ownsPageId(head)) return error.InvalidPageId;
        if (new_head != 0 and !self.ownsPageId(new_head)) return error.InvalidPageId;
        self.free_list_head = new_head;
        return head;
    }

    pub fn pushFreeListHead(self: *PageIdAllocator, page_id: u64) OverflowAllocatorError!u64 {
        if (!self.ownsPageId(page_id)) return error.InvalidPageId;
        const prev_head = self.free_list_head;
        self.free_list_head = page_id;
        return prev_head;
    }

    pub fn ownsPageId(self: *const PageIdAllocator, page_id: u64) bool {
        return page_id >= self.firstAllocatablePageId() and page_id < self.region_end_page_id;
    }

    pub fn firstAllocatablePageId(self: *const PageIdAllocator) u64 {
        return self.region_start_page_id;
    }

    pub fn capacity(self: *const PageIdAllocator) u64 {
        return self.region_end_page_id - self.firstAllocatablePageId();
    }
};

pub const ReclaimQueue = struct {
    entries: [reclaim_queue_capacity]ReclaimQueueEntry = [_]ReclaimQueueEntry{
        .{
            .first_page_id = 0,
            .tx_id = 0,
            .state = .pending,
        },
    } ** reclaim_queue_capacity,
    head: usize = 0,
    tail: usize = 0,
    len: usize = 0,

    pub fn enqueue(self: *ReclaimQueue, tx_id: u64, first_page_id: u64) OverflowReclaimError!void {
        if (tx_id == 0) return error.InvalidChainRoot;
        if (first_page_id == 0) return error.InvalidChainRoot;
        if (self.contains(first_page_id)) return error.DuplicateChainRoot;
        if (self.len >= reclaim_queue_capacity) return error.QueueFull;
        self.entries[self.tail] = .{
            .first_page_id = first_page_id,
            .tx_id = tx_id,
            .state = .pending,
        };
        self.tail = (self.tail + 1) % reclaim_queue_capacity;
        self.len += 1;
    }

    pub fn commitTx(self: *ReclaimQueue, tx_id: u64) void {
        if (tx_id == 0 or self.len == 0) return;
        var idx = self.head;
        var remaining = self.len;
        while (remaining > 0) : (remaining -= 1) {
            if (self.entries[idx].tx_id == tx_id and
                self.entries[idx].state == .pending)
            {
                self.entries[idx].state = .committed;
            }
            idx = (idx + 1) % reclaim_queue_capacity;
        }
    }

    pub fn abortTx(self: *ReclaimQueue, tx_id: u64) void {
        if (tx_id == 0 or self.len == 0) return;

        var new_entries: [reclaim_queue_capacity]ReclaimQueueEntry = [_]ReclaimQueueEntry{
            .{
                .first_page_id = 0,
                .tx_id = 0,
                .state = .pending,
            },
        } ** reclaim_queue_capacity;
        var new_len: usize = 0;
        var idx = self.head;
        var remaining = self.len;
        while (remaining > 0) : (remaining -= 1) {
            const entry = self.entries[idx];
            if (!(entry.tx_id == tx_id and entry.state == .pending)) {
                new_entries[new_len] = entry;
                new_len += 1;
            }
            idx = (idx + 1) % reclaim_queue_capacity;
        }

        self.entries = new_entries;
        self.head = 0;
        self.len = new_len;
        self.tail = new_len % reclaim_queue_capacity;
    }

    pub fn dequeueCommitted(self: *ReclaimQueue) OverflowReclaimError!?u64 {
        if (self.len == 0) return error.QueueEmpty;
        const entry = self.entries[self.head];
        if (entry.state != .committed) return null;
        const out = entry.first_page_id;
        self.entries[self.head] = .{
            .first_page_id = 0,
            .tx_id = 0,
            .state = .pending,
        };
        self.head = (self.head + 1) % reclaim_queue_capacity;
        self.len -= 1;
        return out;
    }

    pub fn isEmpty(self: *const ReclaimQueue) bool {
        return self.len == 0;
    }

    pub fn contains(self: *const ReclaimQueue, first_page_id: u64) bool {
        if (first_page_id == 0) return false;
        var idx = self.head;
        var remaining = self.len;
        while (remaining > 0) : (remaining -= 1) {
            if (self.entries[idx].first_page_id == first_page_id) return true;
            idx = (idx + 1) % reclaim_queue_capacity;
        }
        return false;
    }
};

const OverflowHeader = struct {
    format_magic: u16,
    format_version: u8,
    reserved: u8,
    next_page_id: u64,
    payload_len: u16,

    const size = 14;
    const format_magic_value: u16 = 0x4F32; // "O2"
    const format_version_value: u8 = 1;

    fn read(content: *const [content_size]u8) OverflowHeader {
        std.debug.assert(content_size >= size);
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

    fn write(self: OverflowHeader, content: *[content_size]u8) void {
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

/// Overflow page stores a single payload chunk and a next-page pointer.
/// Chaining forms one field-value overflow chain.
pub const OverflowPage = struct {
    pub const null_page_id: u64 = 0;

    pub fn max_payload_len() u16 {
        return content_size - OverflowHeader.size;
    }

    pub fn init(page: *Page) void {
        std.debug.assert(page.header.page_type == .free or page.header.page_type == .overflow);
        page.header.page_type = .overflow;
        @memset(&page.content, 0);
        const header: OverflowHeader = .{
            .format_magic = OverflowHeader.format_magic_value,
            .format_version = OverflowHeader.format_version_value,
            .reserved = 0,
            .next_page_id = null_page_id,
            .payload_len = 0,
        };
        header.write(&page.content);
    }

    pub fn writeChunk(page: *Page, payload: []const u8, next_page_id: u64) OverflowError!void {
        std.debug.assert(page.header.page_type == .overflow);
        if (payload.len > max_payload_len()) return error.PageFull;

        var header = try readHeader(page);
        header.next_page_id = next_page_id;
        header.payload_len = @intCast(payload.len);
        header.write(&page.content);

        if (payload.len > 0) {
            @memcpy(page.content[OverflowHeader.size..][0..payload.len], payload);
        }
        const payload_end = OverflowHeader.size + payload.len;
        if (payload_end < content_size) {
            @memset(page.content[payload_end..content_size], 0);
        }
    }

    pub fn readChunk(page: *const Page) OverflowError!struct {
        payload: []const u8,
        next_page_id: u64,
    } {
        std.debug.assert(page.header.page_type == .overflow);
        const header = try readHeader(page);
        const payload_end = OverflowHeader.size + header.payload_len;
        std.debug.assert(payload_end <= content_size);
        return .{
            .payload = page.content[OverflowHeader.size..payload_end],
            .next_page_id = header.next_page_id,
        };
    }

    fn readHeader(page: *const Page) OverflowError!OverflowHeader {
        std.debug.assert(page.header.page_type == .overflow);
        const header = OverflowHeader.read(&page.content);
        if (header.format_magic != OverflowHeader.format_magic_value) return error.InvalidPageFormat;
        if (header.format_version != OverflowHeader.format_version_value) {
            return error.UnsupportedPageVersion;
        }
        if (header.payload_len > max_payload_len()) return error.InvalidPageFormat;
        return header;
    }
};

test "overflow init writes header metadata" {
    var page = Page.init(0, .free);
    OverflowPage.init(&page);

    const chunk = try OverflowPage.readChunk(&page);
    try std.testing.expectEqual(@as(usize, 0), chunk.payload.len);
    try std.testing.expectEqual(OverflowPage.null_page_id, chunk.next_page_id);
}

test "overflow write/read chunk roundtrip" {
    var page = Page.init(1, .free);
    OverflowPage.init(&page);

    const payload = "hello overflow";
    try OverflowPage.writeChunk(&page, payload, 55);
    const chunk = try OverflowPage.readChunk(&page);
    try std.testing.expectEqualSlices(u8, payload, chunk.payload);
    try std.testing.expectEqual(@as(u64, 55), chunk.next_page_id);
}

test "overflow write rejects payload bigger than page capacity" {
    var page = Page.init(2, .free);
    OverflowPage.init(&page);

    const too_large = [_]u8{0xAB} ** (OverflowPage.max_payload_len() + 1);
    const result = OverflowPage.writeChunk(&page, too_large[0..], OverflowPage.null_page_id);
    try std.testing.expectError(OverflowError.PageFull, result);
}

test "overflow read rejects invalid format magic" {
    var page = Page.init(3, .free);
    OverflowPage.init(&page);
    page.content[0] = 0x00;
    page.content[1] = 0x00;

    try std.testing.expectError(OverflowError.InvalidPageFormat, OverflowPage.readChunk(&page));
}

test "page-id allocator allocates monotonically and exhausts fail-closed" {
    var allocator = try PageIdAllocator.initWithBounds(200, 2);
    try std.testing.expectEqual(@as(u64, 200), try allocator.allocateFresh());
    try std.testing.expectEqual(@as(u64, 201), try allocator.allocateFresh());
    try std.testing.expectError(error.RegionExhausted, allocator.allocateFresh());
}

test "page-id allocator tracks free-list head in LIFO order" {
    var allocator = try PageIdAllocator.initWithBounds(300, 5);
    const a = try allocator.allocateFresh();
    const b = try allocator.allocateFresh();
    const c = try allocator.allocateFresh();
    try std.testing.expectEqual(@as(u64, 300), a);
    try std.testing.expectEqual(@as(u64, 301), b);
    try std.testing.expectEqual(@as(u64, 302), c);

    const prev_a = try allocator.pushFreeListHead(a);
    try std.testing.expectEqual(@as(u64, 0), prev_a);
    const prev_b = try allocator.pushFreeListHead(b);
    try std.testing.expectEqual(a, prev_b);

    const head_1 = try allocator.popFreeListHead(a);
    try std.testing.expectEqual(b, head_1);
    const head_2 = try allocator.popFreeListHead(0);
    try std.testing.expectEqual(a, head_2);
    try std.testing.expectError(error.RegionExhausted, allocator.popFreeListHead(0));
}

test "page-id allocator rejects invalid region" {
    try std.testing.expectError(error.InvalidRegion, PageIdAllocator.initWithBounds(10, 0));
    try std.testing.expectError(error.InvalidRegion, PageIdAllocator.initWithBounds(0, 1));
}

test "reclaim queue is FIFO and rejects duplicates" {
    var queue: ReclaimQueue = .{};
    try queue.enqueue(10, 50);
    try queue.enqueue(10, 60);
    try std.testing.expectError(error.DuplicateChainRoot, queue.enqueue(10, 50));
    queue.commitTx(10);
    try std.testing.expectEqual(@as(u64, 50), (try queue.dequeueCommitted()).?);
    try std.testing.expectEqual(@as(u64, 60), (try queue.dequeueCommitted()).?);
    try std.testing.expect(queue.isEmpty());
    try std.testing.expectError(error.QueueEmpty, queue.dequeueCommitted());
}

test "reclaim queue abort removes only pending entries for tx" {
    var queue: ReclaimQueue = .{};
    try queue.enqueue(11, 101);
    try queue.enqueue(12, 102);
    queue.commitTx(12);
    queue.abortTx(11);

    try std.testing.expectEqual(@as(usize, 1), queue.len);
    try std.testing.expectEqual(@as(u64, 102), (try queue.dequeueCommitted()).?);
    try std.testing.expect(queue.isEmpty());
}

test "reclaim queue blocks dequeue when head tx is pending" {
    var queue: ReclaimQueue = .{};
    try queue.enqueue(21, 201);
    try queue.enqueue(22, 202);
    queue.commitTx(22);

    try std.testing.expectEqual(@as(?u64, null), try queue.dequeueCommitted());
    queue.commitTx(21);
    try std.testing.expectEqual(@as(u64, 201), (try queue.dequeueCommitted()).?);
    try std.testing.expectEqual(@as(u64, 202), (try queue.dequeueCommitted()).?);
}
