const std = @import("std");
const io_mod = @import("../storage/io.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const wal_mod = @import("../storage/wal.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const undo_mod = @import("../mvcc/undo.zig");
const static_alloc_mod = @import("../tiger/static_allocator.zig");
const disk_mod = @import("../simulator/disk.zig");

const Storage = io_mod.Storage;
const BufferPool = buffer_pool_mod.BufferPool;
const Wal = wal_mod.Wal;
const TxManager = tx_mod.TxManager;
const UndoLog = undo_mod.UndoLog;
const StaticAllocator = static_alloc_mod.StaticAllocator;

pub const BootstrapError = error{
    OutOfMemory,
};

pub const BootstrapConfig = struct {
    buffer_pool_frames: u16 = 16,
    undo_max_entries: u32 = 1024,
    undo_max_data_bytes: u32 = 64 * 1024,
    wal_buffer_capacity_bytes: usize = io_mod.page_size,
};

/// Core runtime composition built in allocator init phase and sealed before
/// runtime operations.
pub const BootstrappedRuntime = struct {
    static_allocator: StaticAllocator,
    pool: BufferPool,
    wal: Wal,
    tx_manager: TxManager,
    undo_log: UndoLog,

    pub fn init(
        memory_region: []u8,
        storage: Storage,
        config: BootstrapConfig,
    ) BootstrapError!BootstrappedRuntime {
        var runtime: BootstrappedRuntime = undefined;
        runtime.static_allocator = StaticAllocator.init(memory_region);
        const allocator = runtime.static_allocator.allocator();

        runtime.pool = BufferPool.init(
            allocator,
            storage,
            config.buffer_pool_frames,
        ) catch return error.OutOfMemory;
        errdefer runtime.pool.deinit();

        runtime.wal = Wal.init(allocator, storage);
        errdefer runtime.wal.deinit();
        runtime.wal.reserveBufferCapacity(config.wal_buffer_capacity_bytes) catch
            return error.OutOfMemory;

        runtime.undo_log = UndoLog.init(
            allocator,
            config.undo_max_entries,
            config.undo_max_data_bytes,
        ) catch return error.OutOfMemory;
        errdefer runtime.undo_log.deinit();

        runtime.tx_manager = TxManager.init(allocator);

        runtime.static_allocator.seal();
        return runtime;
    }

    pub fn deinit(self: *BootstrappedRuntime) void {
        self.undo_log.deinit();
        self.tx_manager.deinit();
        self.wal.deinit();
        self.pool.deinit();
        self.* = undefined;
    }
};

test "bootstrap seals allocator before runtime operations" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var backing_memory: [256 * 1024]u8 = undefined;
    var runtime = try BootstrappedRuntime.init(
        backing_memory[0..],
        disk.storage(),
        .{
            .buffer_pool_frames = 8,
            .undo_max_entries = 128,
            .undo_max_data_bytes = 16 * 1024,
            .wal_buffer_capacity_bytes = 512,
        },
    );
    defer runtime.deinit();

    try std.testing.expect(runtime.static_allocator.isSealed());

    // These operations run after seal. If they allocate, test fails via panic.
    const page = try runtime.pool.pin(1);
    page.header.lsn = 1;
    runtime.pool.unpin(1, true);
    _ = try runtime.wal.append(1, .tx_begin, 1, "x");
    try runtime.wal.flush();
}

test "runtime wal growth beyond startup cap fails closed" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    var backing_memory: [128 * 1024]u8 = undefined;
    var runtime = try BootstrappedRuntime.init(
        backing_memory[0..],
        disk.storage(),
        .{
            .buffer_pool_frames = 4,
            .undo_max_entries = 64,
            .undo_max_data_bytes = 8 * 1024,
            .wal_buffer_capacity_bytes = 32,
        },
    );
    defer runtime.deinit();

    var payload: [48]u8 = [_]u8{1} ** 48;
    try std.testing.expectError(
        error.OutOfMemory,
        runtime.wal.append(1, .insert, 7, payload[0..]),
    );
}
