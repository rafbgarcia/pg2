//! Runtime storage-root lifecycle for production file-backed server mode.
//!
//! Responsibilities in this file:
//! - Opens/creates a storage root directory and fixed backing files.
//! - Acquires and holds an OS-level exclusive writer lock (`LOCK` file).
//! - Writes/reads lock metadata for operator diagnostics.
//! - Wires data/wal/temp files behind a routed `Storage` adapter.
const std = @import("std");
const io_mod = @import("../storage/io.zig");
const file_storage_mod = @import("../storage/file_storage.zig");
const routing_storage_mod = @import("../storage/routing_storage.zig");

const FileStorage = file_storage_mod.FileStorage;
const RoutingStorage = routing_storage_mod.RoutingStorage;
const Storage = io_mod.Storage;

pub const default_storage_root = ".pg2";

pub const OpenError = error{
    StorageRootOpenFailed,
    LockOpenFailed,
    WriterAlreadyActive,
    LockAcquireFailed,
    LockMetadataWriteFailed,
    DataFileOpenFailed,
    WalFileOpenFailed,
    TempFileOpenFailed,
    TempTruncateFailed,
};

pub const InspectError = error{
    StorageRootOpenFailed,
    LockOpenFailed,
    LockReadFailed,
    InvalidLockMetadata,
};

pub const LockMetadata = struct {
    pid: i64,
    hostname_len: usize,
    hostname_buf: [256]u8,
    started_at_unix_ns: u64,

    pub fn hostname(self: *const LockMetadata) []const u8 {
        return self.hostname_buf[0..self.hostname_len];
    }
};

pub const StorageUsage = struct {
    data_pg2_bytes: u64,
    wal_pg2_bytes: u64,
    temp_pg2_bytes: u64,
    data_pages: u64,
    wal_pages: u64,
    temp_pages: u64,
};

pub const RuntimeStorageRoot = struct {
    root_dir: std.fs.Dir,
    lock_file: std.fs.File,
    data_file: FileStorage,
    wal_file: FileStorage,
    temp_file: FileStorage,
    routing: RoutingStorage,

    pub fn openOrCreate(storage_root: []const u8) OpenError!RuntimeStorageRoot {
        var root_dir = std.fs.cwd().makeOpenPath(storage_root, .{}) catch
            return error.StorageRootOpenFailed;
        errdefer root_dir.close();

        const lock_file = root_dir.createFile("LOCK", .{
            .read = true,
            .truncate = false,
            .exclusive = false,
            .lock = .none,
        }) catch return error.LockOpenFailed;
        errdefer lock_file.close();

        acquireExclusiveLock(lock_file) catch |err| switch (err) {
            error.Locked => return error.WriterAlreadyActive,
            else => return error.LockAcquireFailed,
        };

        writeLockMetadata(lock_file) catch return error.LockMetadataWriteFailed;

        var data_file = FileStorage.openOrCreateAt(root_dir, "data.pg2") catch
            return error.DataFileOpenFailed;
        errdefer data_file.close();

        var wal_file = FileStorage.openOrCreateAt(root_dir, "wal.pg2") catch
            return error.WalFileOpenFailed;
        errdefer wal_file.close();

        var temp_file = FileStorage.openOrCreateAt(root_dir, "temp.pg2") catch
            return error.TempFileOpenFailed;
        errdefer temp_file.close();

        temp_file.truncate(0) catch return error.TempTruncateFailed;

        const runtime_storage: RuntimeStorageRoot = .{
            .root_dir = root_dir,
            .lock_file = lock_file,
            .data_file = data_file,
            .wal_file = wal_file,
            .temp_file = temp_file,
            .routing = undefined,
        };
        return runtime_storage;
    }

    pub fn deinit(self: *RuntimeStorageRoot) void {
        self.temp_file.close();
        self.wal_file.close();
        self.data_file.close();
        self.lock_file.close();
        self.root_dir.close();
    }

    pub fn storage(self: *RuntimeStorageRoot) Storage {
        self.routing = RoutingStorage.init(
            self.data_file.storage(),
            self.wal_file.storage(),
            self.temp_file.storage(),
        );
        return self.routing.storage();
    }

    pub fn snapshotUsage(self: *RuntimeStorageRoot) !StorageUsage {
        const data_pg2_bytes = try self.data_file.sizeBytes();
        const wal_pg2_bytes = try self.wal_file.sizeBytes();
        const temp_pg2_bytes = try self.temp_file.sizeBytes();
        return .{
            .data_pg2_bytes = data_pg2_bytes,
            .wal_pg2_bytes = wal_pg2_bytes,
            .temp_pg2_bytes = temp_pg2_bytes,
            .data_pages = data_pg2_bytes / io_mod.page_size,
            .wal_pages = wal_pg2_bytes / io_mod.page_size,
            .temp_pages = temp_pg2_bytes / io_mod.page_size,
        };
    }

    pub fn inspectLockMetadata(storage_root: []const u8) InspectError!LockMetadata {
        var root_dir = std.fs.cwd().openDir(storage_root, .{}) catch
            return error.StorageRootOpenFailed;
        defer root_dir.close();

        const lock_file = root_dir.openFile("LOCK", .{}) catch return error.LockOpenFailed;
        defer lock_file.close();

        var content_buf: [1024]u8 = undefined;
        const bytes_read = lock_file.readAll(&content_buf) catch return error.LockReadFailed;
        return parseLockMetadata(content_buf[0..bytes_read]) catch error.InvalidLockMetadata;
    }

    pub fn writeLockMetadata(lock_file: std.fs.File) !void {
        try lock_file.setEndPos(0);
        try lock_file.seekTo(0);

        var hostname_buf: [std.posix.HOST_NAME_MAX]u8 = undefined;
        const hostname = std.posix.gethostname(&hostname_buf) catch "unknown";
        const started_at_unix_ns: u64 = @intCast(@max(@as(i64, 0), std.time.nanoTimestamp()));
        var buf: [512]u8 = undefined;
        const content = try std.fmt.bufPrint(
            &buf,
            "pid={d}\nhostname={s}\nstarted_at_unix_ns={d}\n",
            .{
                std.c.getpid(),
                hostname,
                started_at_unix_ns,
            },
        );
        try lock_file.writeAll(content);
    }
};

fn acquireExclusiveLock(lock_file: std.fs.File) std.posix.FcntlError!void {
    var lock_spec = std.mem.zeroes(std.posix.Flock);
    lock_spec.type = std.c.F.WRLCK;
    lock_spec.whence = std.c.SEEK.SET;
    lock_spec.start = 0;
    lock_spec.len = 0;
    lock_spec.pid = 0;
    _ = try std.posix.fcntl(
        lock_file.handle,
        std.c.F.SETLK,
        @intFromPtr(&lock_spec),
    );
}

fn parseLockMetadata(content: []const u8) error{InvalidLockMetadata}!LockMetadata {
    var pid_opt: ?i64 = null;
    var started_opt: ?u64 = null;
    var hostname_len: usize = 0;
    var hostname_buf: [256]u8 = undefined;
    @memset(&hostname_buf, 0);

    var line_iter = std.mem.splitScalar(u8, content, '\n');
    while (line_iter.next()) |line| {
        if (line.len == 0) continue;
        if (std.mem.startsWith(u8, line, "pid=")) {
            pid_opt = std.fmt.parseInt(i64, line["pid=".len..], 10) catch
                return error.InvalidLockMetadata;
            continue;
        }
        if (std.mem.startsWith(u8, line, "hostname=")) {
            const hostname = line["hostname=".len..];
            if (hostname.len == 0 or hostname.len > hostname_buf.len) {
                return error.InvalidLockMetadata;
            }
            @memcpy(hostname_buf[0..hostname.len], hostname);
            hostname_len = hostname.len;
            continue;
        }
        if (std.mem.startsWith(u8, line, "started_at_unix_ns=")) {
            started_opt = std.fmt.parseInt(
                u64,
                line["started_at_unix_ns=".len..],
                10,
            ) catch return error.InvalidLockMetadata;
            continue;
        }
    }

    return .{
        .pid = pid_opt orelse return error.InvalidLockMetadata,
        .hostname_len = hostname_len,
        .hostname_buf = hostname_buf,
        .started_at_unix_ns = started_opt orelse return error.InvalidLockMetadata,
    };
}

test "RuntimeStorageRoot creates fixed files and writable storage route" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var original_dir = try std.fs.cwd().openDir(".", .{});
    defer original_dir.close();
    try std.posix.fchdir(tmp.dir.fd);
    defer std.posix.fchdir(original_dir.fd) catch {};

    const storage_root = "runtime-a";
    {
        var storage_root_runtime = try RuntimeStorageRoot.openOrCreate(storage_root);
        defer storage_root_runtime.deinit();

        var page: [io_mod.page_size]u8 = undefined;
        @memset(&page, 0x5A);
        try storage_root_runtime.storage().write(3, &page);
        try storage_root_runtime.storage().fsync();

        _ = try storage_root_runtime.root_dir.statFile("LOCK");
        _ = try storage_root_runtime.root_dir.statFile("data.pg2");
        _ = try storage_root_runtime.root_dir.statFile("wal.pg2");
        _ = try storage_root_runtime.root_dir.statFile("temp.pg2");
    }
}

test "RuntimeStorageRoot lock is process-scoped on this platform" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var original_dir = try std.fs.cwd().openDir(".", .{});
    defer original_dir.close();
    try std.posix.fchdir(tmp.dir.fd);
    defer std.posix.fchdir(original_dir.fd) catch {};

    const storage_root = "runtime-lock";
    var holder = try RuntimeStorageRoot.openOrCreate(storage_root);
    defer holder.deinit();

    // fcntl locks are process-scoped on macOS: re-acquiring the same lock
    // from the same process succeeds. Inter-process writer exclusion is
    // validated by production process boundaries.
    var second = try RuntimeStorageRoot.openOrCreate(storage_root);
    defer second.deinit();
}

test "RuntimeStorageRoot startup truncates temp.pg2" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var original_dir = try std.fs.cwd().openDir(".", .{});
    defer original_dir.close();
    try std.posix.fchdir(tmp.dir.fd);
    defer std.posix.fchdir(original_dir.fd) catch {};

    const storage_root = "runtime-temp";
    {
        var first = try RuntimeStorageRoot.openOrCreate(storage_root);
        defer first.deinit();

        var temp_page: [io_mod.page_size]u8 = undefined;
        @memset(&temp_page, 0xAB);
        try first.storage().write(20_000_000, &temp_page);
        try first.storage().fsync();
        const temp_size = try first.temp_file.sizeBytes();
        try std.testing.expect(temp_size >= io_mod.page_size);
    }

    {
        var reopened = try RuntimeStorageRoot.openOrCreate(storage_root);
        defer reopened.deinit();
        try std.testing.expectEqual(@as(u64, 0), try reopened.temp_file.sizeBytes());
    }
}

test "RuntimeStorageRoot stale lock file does not block startup" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var original_dir = try std.fs.cwd().openDir(".", .{});
    defer original_dir.close();
    try std.posix.fchdir(tmp.dir.fd);
    defer std.posix.fchdir(original_dir.fd) catch {};

    const storage_root = "runtime-stale";
    {
        var first = try RuntimeStorageRoot.openOrCreate(storage_root);
        defer first.deinit();
    }

    var second = try RuntimeStorageRoot.openOrCreate(storage_root);
    defer second.deinit();
}

test "inspectLockMetadata parses lock file fields" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var original_dir = try std.fs.cwd().openDir(".", .{});
    defer original_dir.close();
    try std.posix.fchdir(tmp.dir.fd);
    defer std.posix.fchdir(original_dir.fd) catch {};

    const storage_root = "runtime-inspect";
    {
        var runtime_storage = try RuntimeStorageRoot.openOrCreate(storage_root);
        defer runtime_storage.deinit();

        const metadata = try RuntimeStorageRoot.inspectLockMetadata(storage_root);
        try std.testing.expect(metadata.pid > 0);
        try std.testing.expect(metadata.hostname().len > 0);
        try std.testing.expect(metadata.started_at_unix_ns > 0);
    }
}
