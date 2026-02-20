//! Core page format and (de)serialization for persistent storage.
//!
//! Responsibilities in this file:
//! - Defines the database-wide page header contract (`PageHeader`).
//! - Defines valid page kinds (`PageType`) used by storage structures.
//! - Serializes/deserializes pages with checksum and format validation.
//! - Provides `Page.init` for deterministic zeroed page construction.
//!
//! Why this exists:
//! - Every storage subsystem reads/writes the same 8KB page primitive.
//! - A single canonical format prevents per-module drift and hidden coupling.
//!
//! Integrity behavior:
//! - Deserialization verifies checksum first, then type/magic/version fields.
//! - Invalid bytes fail closed via explicit errors instead of permissive parsing.
//!
//! Contributor notes:
//! - `PageHeader` layout is an on-disk contract; incompatible changes require
//!   versioning and compatibility handling.
//! - Keep serialization deterministic (little-endian encoding, fixed offsets).
//! - Higher layers own semantics of page content; this module only handles the
//!   common envelope and validation.
const std = @import("std");
const io = @import("io.zig");

const page_size = io.page_size;

/// Page types. Extended as more storage structures are added.
pub const PageType = enum(u8) {
    free = 0,
    heap = 1,
    btree_leaf = 2,
    btree_internal = 3,
    overflow = 4,
};

/// On-disk page header. 24 bytes.
///
/// Layout:
///   page_id:    u64  (bytes 0..8)
///   page_type:  u8   (byte 8)
///   version:    u8   (byte 9)
///   magic:      u16  (bytes 10..12)
///   lsn:        u64  (bytes 12..20)
///   checksum:   u32  (bytes 20..24)
pub const PageHeader = struct {
    page_id: u64,
    page_type: PageType,
    format_version: u8,
    format_magic: u16,
    lsn: u64,
    checksum: u32,

    pub const size = 24;
};

pub const page_format_version: u8 = 1;
pub const page_format_magic: u16 = 0x4732; // "G2"
pub const PageDeserializeError = error{
    ChecksumMismatch,
    InvalidPageType,
    InvalidPageFormat,
    UnsupportedPageVersion,
};

/// Content area size after the header.
pub const content_size = page_size - PageHeader.size;

/// An 8KB page. The fundamental unit of storage.
pub const Page = struct {
    header: PageHeader,
    content: [content_size]u8,

    /// Serialize the page to a raw byte buffer for writing to disk.
    pub fn serialize(self: *const Page, buf: *[page_size]u8) void {
        var offset: usize = 0;

        // page_id
        @memcpy(buf[offset..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, self.header.page_id)));
        offset += 8;

        // page_type
        buf[offset] = @intFromEnum(self.header.page_type);
        offset += 1;

        // version
        buf[offset] = self.header.format_version;
        offset += 1;

        // magic
        @memcpy(
            buf[offset..][0..2],
            std.mem.asBytes(&std.mem.nativeToLittle(u16, self.header.format_magic)),
        );
        offset += 2;

        // lsn
        @memcpy(buf[offset..][0..8], std.mem.asBytes(&std.mem.nativeToLittle(u64, self.header.lsn)));
        offset += 8;

        // checksum — compute over everything except the checksum field itself
        // Write a placeholder first, compute checksum, then fill it in.
        @memset(buf[offset..][0..4], 0);
        offset += 4;

        // content
        @memcpy(buf[offset..][0..content_size], &self.content);

        // Now compute checksum over the full page with checksum field zeroed.
        const cksum = computeChecksum(buf);
        const cksum_le = std.mem.nativeToLittle(u32, cksum);
        @memcpy(buf[20..24], std.mem.asBytes(&cksum_le));
    }

    /// Deserialize a raw byte buffer into a Page. Returns error if checksum fails.
    pub fn deserialize(buf: *const [page_size]u8) PageDeserializeError!Page {
        // Read stored checksum.
        const stored_cksum = std.mem.littleToNative(u32, std.mem.bytesAsValue(u32, buf[20..24]).*);

        // Zero checksum field for verification.
        var verify_buf: [page_size]u8 = buf.*;
        @memset(verify_buf[20..24], 0);
        const computed = computeChecksum(&verify_buf);

        if (stored_cksum != computed) {
            return error.ChecksumMismatch;
        }

        const page_id = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, buf[0..8]).*);
        const page_type = std.meta.intToEnum(PageType, buf[8]) catch
            return error.InvalidPageType;
        const format_version = buf[9];
        const format_magic = std.mem.littleToNative(u16, std.mem.bytesAsValue(u16, buf[10..12]).*);
        if (format_magic != page_format_magic) return error.InvalidPageFormat;
        if (format_version != page_format_version) return error.UnsupportedPageVersion;
        const lsn = std.mem.littleToNative(u64, std.mem.bytesAsValue(u64, buf[12..20]).*);

        var page = Page{
            .header = .{
                .page_id = page_id,
                .page_type = page_type,
                .format_version = format_version,
                .format_magic = format_magic,
                .lsn = lsn,
                .checksum = stored_cksum,
            },
            .content = undefined,
        };
        @memcpy(&page.content, buf[PageHeader.size..]);
        return page;
    }

    /// Create a new page with zeroed content.
    pub fn init(page_id: u64, page_type: PageType) Page {
        return .{
            .header = .{
                .page_id = page_id,
                .page_type = page_type,
                .format_version = page_format_version,
                .format_magic = page_format_magic,
                .lsn = 0,
                .checksum = 0,
            },
            .content = std.mem.zeroes([content_size]u8),
        };
    }
};

/// CRC-32 (Castagnoli) checksum over a full page buffer.
fn computeChecksum(buf: *const [page_size]u8) u32 {
    return std.hash.crc.Crc32Iscsi.hash(buf);
}

// --- Tests ---

test "serialize then deserialize roundtrip" {
    var page = Page.init(42, .heap);
    page.header.lsn = 100;
    @memset(&page.content, 0xDE);

    var buf: [page_size]u8 = undefined;
    page.serialize(&buf);

    const restored = try Page.deserialize(&buf);
    try std.testing.expectEqual(@as(u64, 42), restored.header.page_id);
    try std.testing.expectEqual(PageType.heap, restored.header.page_type);
    try std.testing.expectEqual(@as(u64, 100), restored.header.lsn);
    try std.testing.expectEqualSlices(u8, &page.content, &restored.content);
}

test "checksum detects corruption" {
    var page = Page.init(1, .btree_leaf);
    @memset(&page.content, 0xAA);

    var buf: [page_size]u8 = undefined;
    page.serialize(&buf);

    // Corrupt a byte in the content area.
    buf[PageHeader.size + 10] ^= 0xFF;

    const result = Page.deserialize(&buf);
    try std.testing.expectError(error.ChecksumMismatch, result);
}

test "checksum detects header corruption" {
    var page = Page.init(1, .heap);

    var buf: [page_size]u8 = undefined;
    page.serialize(&buf);

    // Corrupt the page_id field.
    buf[0] ^= 0x01;

    const result = Page.deserialize(&buf);
    try std.testing.expectError(error.ChecksumMismatch, result);
}

test "different pages produce different checksums" {
    var page1 = Page.init(1, .heap);
    var page2 = Page.init(2, .heap);

    var buf1: [page_size]u8 = undefined;
    var buf2: [page_size]u8 = undefined;
    page1.serialize(&buf1);
    page2.serialize(&buf2);

    const restored1 = try Page.deserialize(&buf1);
    const restored2 = try Page.deserialize(&buf2);
    try std.testing.expect(restored1.header.checksum != restored2.header.checksum);
}

test "all page types roundtrip" {
    const types = [_]PageType{ .free, .heap, .btree_leaf, .btree_internal, .overflow };
    for (types) |pt| {
        var page = Page.init(0, pt);
        var buf: [page_size]u8 = undefined;
        page.serialize(&buf);
        const restored = try Page.deserialize(&buf);
        try std.testing.expectEqual(pt, restored.header.page_type);
    }
}

test "reject page with unsupported format version" {
    var page = Page.init(7, .heap);
    var buf: [page_size]u8 = undefined;
    page.serialize(&buf);

    buf[9] = page_format_version + 1;
    @memset(buf[20..24], 0);
    const cksum = computeChecksum(&buf);
    @memcpy(buf[20..24], std.mem.asBytes(&std.mem.nativeToLittle(u32, cksum)));

    try std.testing.expectError(error.UnsupportedPageVersion, Page.deserialize(&buf));
}

test "reject page with invalid format magic" {
    var page = Page.init(9, .heap);
    var buf: [page_size]u8 = undefined;
    page.serialize(&buf);

    const bad_magic = std.mem.nativeToLittle(u16, @as(u16, 0x0000));
    @memcpy(buf[10..12], std.mem.asBytes(&bad_magic));
    @memset(buf[20..24], 0);
    const cksum = computeChecksum(&buf);
    @memcpy(buf[20..24], std.mem.asBytes(&std.mem.nativeToLittle(u32, cksum)));

    try std.testing.expectError(error.InvalidPageFormat, Page.deserialize(&buf));
}
