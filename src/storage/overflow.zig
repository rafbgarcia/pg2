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

pub const PageIdAllocator = struct {
    region_start_page_id: u64 = default_region_start_page_id,
    region_end_page_id: u64 = default_region_end_page_id, // exclusive
    next_page_id: u64 = default_region_start_page_id,

    pub fn initDefault() PageIdAllocator {
        return .{};
    }

    pub fn initWithBounds(
        start_page_id: u64,
        page_count: u64,
    ) OverflowAllocatorError!PageIdAllocator {
        if (page_count == 0) return error.InvalidRegion;
        const end_page_id = std.math.add(u64, start_page_id, page_count) catch
            return error.InvalidRegion;
        return .{
            .region_start_page_id = start_page_id,
            .region_end_page_id = end_page_id,
            .next_page_id = start_page_id,
        };
    }

    pub fn allocate(self: *PageIdAllocator) OverflowAllocatorError!u64 {
        if (self.next_page_id >= self.region_end_page_id) {
            return error.RegionExhausted;
        }
        const page_id = self.next_page_id;
        self.next_page_id += 1;
        return page_id;
    }

    pub fn ownsPageId(self: *const PageIdAllocator, page_id: u64) bool {
        return page_id >= self.region_start_page_id and page_id < self.region_end_page_id;
    }

    pub fn capacity(self: *const PageIdAllocator) u64 {
        return self.region_end_page_id - self.region_start_page_id;
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
    try std.testing.expectEqual(@as(u64, 200), try allocator.allocate());
    try std.testing.expectEqual(@as(u64, 201), try allocator.allocate());
    try std.testing.expectError(error.RegionExhausted, allocator.allocate());
}

test "page-id allocator rejects invalid region" {
    try std.testing.expectError(error.InvalidRegion, PageIdAllocator.initWithBounds(10, 0));
}
