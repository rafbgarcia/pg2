//! File-backed restart durability matrix for WF14 Phase 4.
const std = @import("std");
const pg2 = @import("pg2");

const storage_root_mod = pg2.runtime.storage_root;
const wal_mod = pg2.storage.wal;
const io_mod = pg2.storage.io;

const RuntimeStorageRoot = storage_root_mod.RuntimeStorageRoot;
const Wal = wal_mod.Wal;
const Storage = io_mod.Storage;
const page_size = io_mod.page_size;

const FaultInjectingStorage = struct {
    backing: Storage,
    writes: u64 = 0,
    fsyncs: u64 = 0,
    fail_write_at: ?u64 = null,
    fail_fsync_at: ?u64 = null,

    fn storage(self: *FaultInjectingStorage) Storage {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Storage.VTable{
        .read = &readImpl,
        .write = &writeImpl,
        .fsync = &fsyncImpl,
    };

    fn readImpl(ptr: *anyopaque, page_id: u64, buf: *[page_size]u8) io_mod.StorageError!void {
        const self: *FaultInjectingStorage = @ptrCast(@alignCast(ptr));
        try self.backing.read(page_id, buf);
    }

    fn writeImpl(ptr: *anyopaque, page_id: u64, data: *const [page_size]u8) io_mod.StorageError!void {
        const self: *FaultInjectingStorage = @ptrCast(@alignCast(ptr));
        self.writes += 1;
        if (self.fail_write_at) |n| {
            if (self.writes == n) {
                self.fail_write_at = null;
                return error.WriteError;
            }
        }
        try self.backing.write(page_id, data);
    }

    fn fsyncImpl(ptr: *anyopaque) io_mod.StorageError!void {
        const self: *FaultInjectingStorage = @ptrCast(@alignCast(ptr));
        self.fsyncs += 1;
        if (self.fail_fsync_at) |n| {
            if (self.fsyncs == n) {
                self.fail_fsync_at = null;
                return error.FsyncError;
            }
        }
        try self.backing.fsync();
    }
};

fn appendCommittedWal(wal: *Wal, page_id: u64, payload: []const u8) !void {
    const tx_id: u64 = 41;
    _ = try wal.beginTx(tx_id);
    _ = try wal.append(tx_id, .insert, page_id, payload);
    _ = try wal.commitTx(tx_id);
}

fn recoveredRecordCount(dir_name: []const u8) !usize {
    var root = try RuntimeStorageRoot.openOrCreate(dir_name);
    defer root.deinit();

    var wal = Wal.init(std.testing.allocator, root.storage());
    defer wal.deinit();
    try wal.recover();

    var records: [64]wal_mod.Record = undefined;
    var payload: [16 * 1024]u8 = undefined;
    const decoded = try wal.readFromInto(1, records[0..], payload[0..]);
    return decoded.records_len;
}

fn enterTmpCwd(tmp: *std.testing.TmpDir) !std.fs.Dir {
    const original_dir = try std.fs.cwd().openDir(".", .{});
    try std.posix.fchdir(tmp.dir.fd);
    return original_dir;
}

test "file backend restart: flushed committed WAL survives" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var original_dir = try enterTmpCwd(&tmp);
    defer {
        std.posix.fchdir(original_dir.fd) catch {};
        original_dir.close();
    }

    const dir_name = "durability-a";
    {
        var root = try RuntimeStorageRoot.openOrCreate(dir_name);
        defer root.deinit();
        var wal = Wal.init(std.testing.allocator, root.storage());
        defer wal.deinit();

        try appendCommittedWal(&wal, 77, "row-a");
        try wal.forceFlush();
    }

    try std.testing.expectEqual(@as(usize, 3), try recoveredRecordCount(dir_name));
}

test "file backend restart: unflushed committed WAL is not visible" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var original_dir = try enterTmpCwd(&tmp);
    defer {
        std.posix.fchdir(original_dir.fd) catch {};
        original_dir.close();
    }

    const dir_name = "durability-b";
    {
        var root = try RuntimeStorageRoot.openOrCreate(dir_name);
        defer root.deinit();
        var wal = Wal.init(std.testing.allocator, root.storage());
        defer wal.deinit();

        try appendCommittedWal(&wal, 88, "row-b");
        // Intentionally no forceFlush() before restart.
    }

    try std.testing.expectEqual(@as(usize, 0), try recoveredRecordCount(dir_name));
}

test "file backend crash matrix: write/fsync/metadata boundaries fail closed" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var original_dir = try enterTmpCwd(&tmp);
    defer {
        std.posix.fchdir(original_dir.fd) catch {};
        original_dir.close();
    }

    const scenarios = [_]struct {
        name: []const u8,
        fail_write_at: ?u64,
        fail_fsync_at: ?u64,
        expected_error: anyerror,
    }{
        .{
            .name = "wal_data_write",
            .fail_write_at = 1,
            .fail_fsync_at = null,
            .expected_error = error.WalWriteError,
        },
        .{
            .name = "wal_data_fsync",
            .fail_write_at = null,
            .fail_fsync_at = 1,
            .expected_error = error.WalFsyncError,
        },
        .{
            .name = "wal_metadata_write",
            .fail_write_at = 2,
            .fail_fsync_at = null,
            .expected_error = error.WalWriteError,
        },
    };

    for (scenarios) |scenario| {
        {
            var root = try RuntimeStorageRoot.openOrCreate(scenario.name);
            defer root.deinit();

            var fault = FaultInjectingStorage{
                .backing = root.storage(),
                .fail_write_at = scenario.fail_write_at,
                .fail_fsync_at = scenario.fail_fsync_at,
            };
            var wal = Wal.init(std.testing.allocator, fault.storage());
            defer wal.deinit();

            try appendCommittedWal(&wal, 99, "row-c");
            try std.testing.expectError(scenario.expected_error, wal.forceFlush());
        }

        // Restart must remain readable and not expose records from failed flush.
        try std.testing.expectEqual(@as(usize, 0), try recoveredRecordCount(scenario.name));
    }
}

test "file backend restart: temp domain resets across restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var original_dir = try enterTmpCwd(&tmp);
    defer {
        std.posix.fchdir(original_dir.fd) catch {};
        original_dir.close();
    }

    const dir_name = "durability-temp";
    {
        var root = try RuntimeStorageRoot.openOrCreate(dir_name);
        defer root.deinit();

        var temp_page: [page_size]u8 = undefined;
        @memset(&temp_page, 0xCD);
        try root.storage().write(20_000_000, &temp_page);
        try root.storage().fsync();
        const usage_before = try root.snapshotUsage();
        try std.testing.expect(usage_before.temp_pg2_bytes >= page_size);
    }
    {
        var reopened = try RuntimeStorageRoot.openOrCreate(dir_name);
        defer reopened.deinit();
        const usage_after = try reopened.snapshotUsage();
        try std.testing.expectEqual(@as(u64, 0), usage_after.temp_pg2_bytes);
    }
}
