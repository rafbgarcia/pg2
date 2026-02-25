//! Multi-domain storage router for data/WAL/temp page-id bands.
const std = @import("std");
const io_mod = @import("io.zig");
const temp_mod = @import("temp.zig");

const Storage = io_mod.Storage;
const StorageError = io_mod.StorageError;
const page_size = io_mod.page_size;

pub const data_last_page_id: u64 = 999_998;
pub const wal_meta_page_id: u64 = 999_999;
pub const wal_last_page_id: u64 = temp_mod.default_region_start_page_id - 1;
pub const temp_first_page_id: u64 = temp_mod.default_region_start_page_id;

pub const RoutingStorage = struct {
    data_storage: Storage,
    wal_storage: Storage,
    temp_storage: Storage,

    pub fn init(data_storage: Storage, wal_storage: Storage, temp_storage: Storage) RoutingStorage {
        std.debug.assert(data_last_page_id + 1 == wal_meta_page_id);
        std.debug.assert(wal_meta_page_id < temp_first_page_id);
        return .{
            .data_storage = data_storage,
            .wal_storage = wal_storage,
            .temp_storage = temp_storage,
        };
    }

    pub fn storage(self: *RoutingStorage) Storage {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const RoutedStorage = struct {
        storage: Storage,
        local_page_id: u64,
    };

    fn route(self: *const RoutingStorage, page_id: u64) RoutedStorage {
        if (page_id <= data_last_page_id) {
            return .{
                .storage = self.data_storage,
                .local_page_id = page_id,
            };
        }
        if (page_id <= wal_last_page_id) {
            return .{
                .storage = self.wal_storage,
                .local_page_id = page_id - wal_meta_page_id,
            };
        }
        return .{
            .storage = self.temp_storage,
            .local_page_id = page_id - temp_first_page_id,
        };
    }

    const vtable = Storage.VTable{
        .read = &readImpl,
        .write = &writeImpl,
        .fsync = &fsyncImpl,
    };

    fn readImpl(ptr: *anyopaque, page_id: u64, buf: *[page_size]u8) StorageError!void {
        const self: *RoutingStorage = @ptrCast(@alignCast(ptr));
        const routed = self.route(page_id);
        try routed.storage.read(routed.local_page_id, buf);
    }

    fn writeImpl(ptr: *anyopaque, page_id: u64, data: *const [page_size]u8) StorageError!void {
        const self: *RoutingStorage = @ptrCast(@alignCast(ptr));
        const routed = self.route(page_id);
        try routed.storage.write(routed.local_page_id, data);
    }

    fn fsyncImpl(ptr: *anyopaque) StorageError!void {
        const self: *RoutingStorage = @ptrCast(@alignCast(ptr));
        try self.data_storage.fsync();
        try self.wal_storage.fsync();
        try self.temp_storage.fsync();
    }
};

test "RoutingStorage maps boundary page ids to the expected domain with local ids" {
    const disk_mod = @import("../simulator/disk.zig");
    const SimulatedDisk = disk_mod.SimulatedDisk;

    var data_disk = SimulatedDisk.init(std.testing.allocator);
    defer data_disk.deinit();
    var wal_disk = SimulatedDisk.init(std.testing.allocator);
    defer wal_disk.deinit();
    var temp_disk = SimulatedDisk.init(std.testing.allocator);
    defer temp_disk.deinit();

    var routing = RoutingStorage.init(
        data_disk.storage(),
        wal_disk.storage(),
        temp_disk.storage(),
    );
    const storage = routing.storage();

    var page: [page_size]u8 = undefined;
    @memset(&page, 0x11);
    try storage.write(999_998, &page);
    @memset(&page, 0x22);
    try storage.write(999_999, &page);
    @memset(&page, 0x33);
    try storage.write(19_999_999, &page);
    @memset(&page, 0x44);
    try storage.write(20_000_000, &page);

    try std.testing.expect(data_disk.hasPending(999_998));
    try std.testing.expect(!data_disk.hasPending(999_999));

    // WAL local ids: 999_999 -> 0, 19_999_999 -> 19_000_000
    try std.testing.expect(wal_disk.hasPending(0));
    try std.testing.expect(wal_disk.hasPending(19_000_000));
    try std.testing.expect(!wal_disk.hasPending(20_000_000));

    // Temp local id: 20_000_000 -> 0
    try std.testing.expect(temp_disk.hasPending(0));
}

test "RoutingStorage returns data from routed backing stores" {
    const disk_mod = @import("../simulator/disk.zig");
    const SimulatedDisk = disk_mod.SimulatedDisk;

    var data_disk = SimulatedDisk.init(std.testing.allocator);
    defer data_disk.deinit();
    var wal_disk = SimulatedDisk.init(std.testing.allocator);
    defer wal_disk.deinit();
    var temp_disk = SimulatedDisk.init(std.testing.allocator);
    defer temp_disk.deinit();

    var routing = RoutingStorage.init(
        data_disk.storage(),
        wal_disk.storage(),
        temp_disk.storage(),
    );
    const storage = routing.storage();

    var write_page: [page_size]u8 = undefined;
    var read_page: [page_size]u8 = undefined;

    @memset(&write_page, 0xA1);
    try storage.write(12, &write_page);
    try storage.read(12, &read_page);
    try std.testing.expectEqualSlices(u8, &write_page, &read_page);

    @memset(&write_page, 0xB2);
    try storage.write(1_500_000, &write_page);
    try storage.read(1_500_000, &read_page);
    try std.testing.expectEqualSlices(u8, &write_page, &read_page);

    @memset(&write_page, 0xC3);
    try storage.write(25_000_000, &write_page);
    try storage.read(25_000_000, &read_page);
    try std.testing.expectEqualSlices(u8, &write_page, &read_page);
}

test "RoutingStorage fsync propagates domain fsync failures" {
    const disk_mod = @import("../simulator/disk.zig");
    const SimulatedDisk = disk_mod.SimulatedDisk;

    var data_disk = SimulatedDisk.init(std.testing.allocator);
    defer data_disk.deinit();
    var wal_disk = SimulatedDisk.init(std.testing.allocator);
    defer wal_disk.deinit();
    var temp_disk = SimulatedDisk.init(std.testing.allocator);
    defer temp_disk.deinit();

    wal_disk.failFsyncAt(1);

    var routing = RoutingStorage.init(
        data_disk.storage(),
        wal_disk.storage(),
        temp_disk.storage(),
    );
    try std.testing.expectError(error.FsyncError, routing.storage().fsync());
}
