//! Production file-backed `Storage` adapter.
//!
//! Responsibilities in this file:
//! - Implements page read/write/fsync against a single backing file.
//! - Preserves `Storage` contract semantics (zero-fill unwritten pages).
//! - Maps OS I/O failures to explicit storage boundary errors.
const std = @import("std");
const io_mod = @import("io.zig");

const Storage = io_mod.Storage;
const StorageError = io_mod.StorageError;
const page_size = io_mod.page_size;

pub const OpenError = error{
    OpenFailed,
};

pub const FileStorage = struct {
    file: std.fs.File,
    fail_fsync_once_for_test: bool = false,

    pub fn openOrCreateAt(dir: std.fs.Dir, sub_path: []const u8) OpenError!FileStorage {
        const file = dir.createFile(sub_path, .{
            .read = true,
            .truncate = false,
            .exclusive = false,
            .lock = .none,
        }) catch return error.OpenFailed;
        return .{ .file = file };
    }

    pub fn close(self: *FileStorage) void {
        self.file.close();
    }

    pub fn storage(self: *FileStorage) Storage {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    pub fn failFsyncOnceForTest(self: *FileStorage) void {
        self.fail_fsync_once_for_test = true;
    }

    pub fn sizeBytes(self: *const FileStorage) !u64 {
        const stat = try self.file.stat();
        return stat.size;
    }

    pub fn truncate(self: *FileStorage, size_bytes: u64) !void {
        try self.file.setEndPos(size_bytes);
    }

    const vtable = Storage.VTable{
        .read = &readImpl,
        .write = &writeImpl,
        .fsync = &fsyncImpl,
    };

    fn readImpl(ptr: *anyopaque, page_id: u64, buf: *[page_size]u8) StorageError!void {
        const self: *FileStorage = @ptrCast(@alignCast(ptr));
        const page_offset = std.math.mul(u64, page_id, page_size) catch return error.ReadError;

        var bytes_read_total: usize = 0;
        while (bytes_read_total < page_size) {
            const read_offset = std.math.add(
                u64,
                page_offset,
                @as(u64, bytes_read_total),
            ) catch return error.ReadError;
            const bytes_read = self.file.pread(buf[bytes_read_total..], read_offset) catch
                return error.ReadError;
            if (bytes_read == 0) {
                @memset(buf[bytes_read_total..], 0);
                return;
            }
            bytes_read_total += bytes_read;
        }
    }

    fn writeImpl(ptr: *anyopaque, page_id: u64, data: *const [page_size]u8) StorageError!void {
        const self: *FileStorage = @ptrCast(@alignCast(ptr));
        const page_offset = std.math.mul(u64, page_id, page_size) catch return error.WriteError;
        self.file.pwriteAll(data, page_offset) catch return error.WriteError;
    }

    fn fsyncImpl(ptr: *anyopaque) StorageError!void {
        const self: *FileStorage = @ptrCast(@alignCast(ptr));
        if (self.fail_fsync_once_for_test) {
            self.fail_fsync_once_for_test = false;
            return error.FsyncError;
        }
        self.file.sync() catch return error.FsyncError;
    }
};

test "FileStorage round-trip read/write" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var storage_file = try FileStorage.openOrCreateAt(tmp.dir, "data.pg2");
    defer storage_file.close();

    var page: [page_size]u8 = undefined;
    @memset(&page, 0xA5);
    try storage_file.storage().write(42, &page);

    var read_back: [page_size]u8 = undefined;
    try storage_file.storage().read(42, &read_back);
    try std.testing.expectEqualSlices(u8, &page, &read_back);
}

test "FileStorage persists across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        var storage_file = try FileStorage.openOrCreateAt(tmp.dir, "data.pg2");
        defer storage_file.close();

        var page: [page_size]u8 = undefined;
        @memset(&page, 0x5C);
        try storage_file.storage().write(7, &page);
        try storage_file.storage().fsync();
    }

    {
        var reopened = try FileStorage.openOrCreateAt(tmp.dir, "data.pg2");
        defer reopened.close();

        var read_back: [page_size]u8 = undefined;
        try reopened.storage().read(7, &read_back);
        for (read_back) |byte| {
            try std.testing.expectEqual(@as(u8, 0x5C), byte);
        }
    }
}

test "FileStorage read on unwritten page returns zeroes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var storage_file = try FileStorage.openOrCreateAt(tmp.dir, "data.pg2");
    defer storage_file.close();

    var read_back: [page_size]u8 = undefined;
    try storage_file.storage().read(999, &read_back);
    for (read_back) |byte| {
        try std.testing.expectEqual(@as(u8, 0), byte);
    }
}

test "FileStorage fsync failure is surfaced" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var storage_file = try FileStorage.openOrCreateAt(tmp.dir, "data.pg2");
    defer storage_file.close();
    storage_file.failFsyncOnceForTest();
    try std.testing.expectError(error.FsyncError, storage_file.storage().fsync());
}
