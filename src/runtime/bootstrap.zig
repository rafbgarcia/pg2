//! Runtime bootstrap/composition for core pg2 subsystems.
//!
//! Responsibilities in this file:
//! - Builds buffer pool, WAL, transaction manager, undo log, and query buffers.
//! - Uses startup-only static allocation and seals runtime memory for operation.
//! - Validates memory-budget/config requirements before runtime activation.
//! - Provides bounded per-query buffer slot acquisition/release APIs.
const std = @import("std");
const io_mod = @import("../storage/io.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const wal_mod = @import("../storage/wal.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const undo_mod = @import("../mvcc/undo.zig");
const static_alloc_mod = @import("static_allocator.zig");
const disk_mod = @import("../simulator/disk.zig");
const scan_mod = @import("../executor/scan.zig");
const spill_collector_mod = @import("../executor/spill_collector.zig");

const Storage = io_mod.Storage;
const BufferPool = buffer_pool_mod.BufferPool;
const Wal = wal_mod.Wal;
const TxManager = tx_mod.TxManager;
const UndoLog = undo_mod.UndoLog;
const StaticAllocator = static_alloc_mod.StaticAllocator;
const ResultRow = scan_mod.ResultRow;
const scan_batch_size = scan_mod.scan_batch_size;
const SpillingResultCollector = spill_collector_mod.SpillingResultCollector;
const nested_scan_decode_arena_bytes: usize = 512 * 1024;
const nested_scan_match_arena_bytes: usize = 512 * 1024;

pub const BootstrapError = error{
    OutOfMemory,
    InsufficientMemoryBudget,
    InvalidConfig,
};
pub const QueryBufferError = error{
    NoQuerySlotAvailable,
    InvalidQuerySlot,
};

pub const BootstrapConfig = struct {
    buffer_pool_frames: u16 = 16,
    undo_max_entries: u32 = 1024,
    undo_max_data_bytes: u32 = 64 * 1024,
    wal_buffer_capacity_bytes: usize = 128 * 1024,
    wal_flush_threshold_bytes: usize = 64 * 1024,
    max_query_slots: u16 = 8,
    query_string_arena_bytes_per_slot: usize = 4 * 1024 * 1024,
    temp_pages_per_query_slot: u64 = 1024,
    /// Per-slot byte budget for in-memory result accumulation before spilling
    /// to temp pages. When a query's accumulated result bytes exceed this
    /// threshold, the SpillingResultCollector flushes its hot batch to disk.
    work_memory_bytes_per_slot: u64 = 4 * 1024 * 1024,
};

pub const QueryBuffers = struct {
    slot_index: u16,
    result_rows: []ResultRow,
    scratch_rows_a: []ResultRow,
    scratch_rows_b: []ResultRow,
    nested_rows: []ResultRow,
    string_arena_bytes: []u8,
    nested_decode_arena_bytes: []u8,
    nested_match_arena_bytes: []u8,
    collector: *SpillingResultCollector,
    work_memory_bytes_per_slot: u64,
};

/// Core runtime composition built in allocator init phase and sealed before
/// runtime operations.
pub const BootstrappedRuntime = struct {
    static_allocator: StaticAllocator,
    storage: Storage,
    pool: BufferPool,
    wal: Wal,
    tx_manager: TxManager,
    undo_log: UndoLog,
    query_slot_in_use: []bool,
    query_result_rows: []ResultRow,
    query_scratch_rows_a: []ResultRow,
    query_scratch_rows_b: []ResultRow,
    query_nested_rows: []ResultRow,
    query_string_arenas: []u8,
    query_nested_decode_arenas: []u8,
    query_nested_match_arenas: []u8,
    query_collectors: []SpillingResultCollector,
    max_query_slots: u16,
    work_memory_bytes_per_slot: u64,

    pub fn init(
        memory_region: []u8,
        storage: Storage,
        config: BootstrapConfig,
    ) BootstrapError!BootstrappedRuntime {
        std.debug.assert(memory_region.len > 0);
        if (config.max_query_slots == 0) return error.InvalidConfig;
        if (config.wal_buffer_capacity_bytes == 0) return error.InvalidConfig;
        if (config.buffer_pool_frames == 0) return error.InvalidConfig;
        if (config.undo_max_entries == 0) return error.InvalidConfig;
        if (config.undo_max_data_bytes == 0) return error.InvalidConfig;
        if (config.query_string_arena_bytes_per_slot == 0) return error.InvalidConfig;
        try validateMemoryBudget(memory_region, storage, config);

        var runtime: BootstrappedRuntime = undefined;
        runtime.static_allocator = StaticAllocator.init(memory_region);
        runtime.storage = storage;
        const allocator = runtime.static_allocator.allocator();

        runtime.pool = BufferPool.init(
            allocator,
            storage,
            config.buffer_pool_frames,
        ) catch return error.OutOfMemory;
        errdefer runtime.pool.deinit();

        if (config.wal_flush_threshold_bytes > 0) {
            std.debug.assert(config.wal_buffer_capacity_bytes >= config.wal_flush_threshold_bytes);
        }

        runtime.wal = Wal.init(allocator, storage);
        errdefer runtime.wal.deinit();
        runtime.wal.reserveBufferCapacity(config.wal_buffer_capacity_bytes) catch
            return error.OutOfMemory;
        runtime.wal.flush_threshold_bytes = config.wal_flush_threshold_bytes;

        runtime.undo_log = UndoLog.init(
            allocator,
            config.undo_max_entries,
            config.undo_max_data_bytes,
        ) catch return error.OutOfMemory;
        errdefer runtime.undo_log.deinit();

        runtime.max_query_slots = config.max_query_slots;
        const total_rows = std.math.mul(
            usize,
            @as(usize, config.max_query_slots),
            scan_batch_size,
        ) catch return error.InvalidConfig;
        runtime.query_slot_in_use = allocator.alloc(
            bool,
            config.max_query_slots,
        ) catch return error.OutOfMemory;
        errdefer allocator.free(runtime.query_slot_in_use);
        @memset(runtime.query_slot_in_use, false);

        runtime.query_result_rows = allocator.alloc(
            ResultRow,
            total_rows,
        ) catch return error.OutOfMemory;
        errdefer allocator.free(runtime.query_result_rows);
        runtime.query_scratch_rows_a = allocator.alloc(
            ResultRow,
            total_rows,
        ) catch return error.OutOfMemory;
        errdefer allocator.free(runtime.query_scratch_rows_a);
        runtime.query_scratch_rows_b = allocator.alloc(
            ResultRow,
            total_rows,
        ) catch return error.OutOfMemory;
        errdefer allocator.free(runtime.query_scratch_rows_b);
        runtime.query_nested_rows = allocator.alloc(
            ResultRow,
            total_rows,
        ) catch return error.OutOfMemory;
        errdefer allocator.free(runtime.query_nested_rows);
        const total_string_arena_bytes = std.math.mul(
            usize,
            @as(usize, config.max_query_slots),
            config.query_string_arena_bytes_per_slot,
        ) catch return error.InvalidConfig;
        runtime.query_string_arenas = allocator.alloc(
            u8,
            total_string_arena_bytes,
        ) catch return error.OutOfMemory;
        errdefer allocator.free(runtime.query_string_arenas);
        const total_nested_decode_arena_bytes = std.math.mul(
            usize,
            @as(usize, config.max_query_slots),
            nested_scan_decode_arena_bytes,
        ) catch return error.InvalidConfig;
        runtime.query_nested_decode_arenas = allocator.alloc(
            u8,
            total_nested_decode_arena_bytes,
        ) catch return error.OutOfMemory;
        errdefer allocator.free(runtime.query_nested_decode_arenas);
        const total_nested_match_arena_bytes = std.math.mul(
            usize,
            @as(usize, config.max_query_slots),
            nested_scan_match_arena_bytes,
        ) catch return error.InvalidConfig;
        runtime.query_nested_match_arenas = allocator.alloc(
            u8,
            total_nested_match_arena_bytes,
        ) catch return error.OutOfMemory;
        errdefer allocator.free(runtime.query_nested_match_arenas);

        runtime.query_collectors = allocator.alloc(
            SpillingResultCollector,
            config.max_query_slots,
        ) catch return error.OutOfMemory;
        errdefer allocator.free(runtime.query_collectors);

        runtime.work_memory_bytes_per_slot = config.work_memory_bytes_per_slot;

        runtime.tx_manager = TxManager.init(allocator);

        runtime.static_allocator.seal();
        return runtime;
    }

    pub fn acquireQueryBuffers(
        self: *BootstrappedRuntime,
    ) QueryBufferError!QueryBuffers {
        std.debug.assert(self.query_slot_in_use.len == self.max_query_slots);
        var slot_index: u16 = 0;
        while (slot_index < self.max_query_slots) : (slot_index += 1) {
            if (!self.query_slot_in_use[slot_index]) {
                self.query_slot_in_use[slot_index] = true;
                return .{
                    .slot_index = slot_index,
                    .result_rows = self.rowsForSlot(
                        self.query_result_rows,
                        slot_index,
                    ),
                    .scratch_rows_a = self.rowsForSlot(
                        self.query_scratch_rows_a,
                        slot_index,
                    ),
                    .scratch_rows_b = self.rowsForSlot(
                        self.query_scratch_rows_b,
                        slot_index,
                    ),
                    .nested_rows = self.rowsForSlot(
                        self.query_nested_rows,
                        slot_index,
                    ),
                    .string_arena_bytes = self.stringArenaForSlot(slot_index),
                    .nested_decode_arena_bytes = self.nestedDecodeArenaForSlot(slot_index),
                    .nested_match_arena_bytes = self.nestedMatchArenaForSlot(slot_index),
                    .collector = &self.query_collectors[slot_index],
                    .work_memory_bytes_per_slot = self.work_memory_bytes_per_slot,
                };
            }
        }
        return error.NoQuerySlotAvailable;
    }

    pub fn releaseQueryBuffers(
        self: *BootstrappedRuntime,
        slot_index: u16,
    ) QueryBufferError!void {
        std.debug.assert(self.query_slot_in_use.len == self.max_query_slots);
        if (slot_index >= self.max_query_slots) {
            return error.InvalidQuerySlot;
        }
        if (!self.query_slot_in_use[slot_index]) {
            return error.InvalidQuerySlot;
        }
        self.query_slot_in_use[slot_index] = false;
    }

    pub fn deinit(self: *BootstrappedRuntime) void {
        const allocator = self.static_allocator.allocator();
        allocator.free(self.query_collectors);
        allocator.free(self.query_nested_match_arenas);
        allocator.free(self.query_nested_decode_arenas);
        allocator.free(self.query_nested_rows);
        allocator.free(self.query_scratch_rows_b);
        allocator.free(self.query_scratch_rows_a);
        allocator.free(self.query_result_rows);
        allocator.free(self.query_string_arenas);
        allocator.free(self.query_slot_in_use);
        self.undo_log.deinit();
        self.tx_manager.deinit();
        self.wal.deinit();
        self.pool.deinit();
        self.* = undefined;
    }

    fn rowsForSlot(
        self: *BootstrappedRuntime,
        rows: []ResultRow,
        slot_index: u16,
    ) []ResultRow {
        std.debug.assert(slot_index < self.max_query_slots);
        const start = @as(usize, slot_index) * scan_batch_size;
        const end = start + scan_batch_size;
        std.debug.assert(end <= rows.len);
        return rows[start..end];
    }

    fn stringArenaForSlot(
        self: *BootstrappedRuntime,
        slot_index: u16,
    ) []u8 {
        std.debug.assert(slot_index < self.max_query_slots);
        const per_slot = self.query_string_arenas.len / self.max_query_slots;
        const start = @as(usize, slot_index) * per_slot;
        const end = start + per_slot;
        std.debug.assert(end <= self.query_string_arenas.len);
        return self.query_string_arenas[start..end];
    }

    fn nestedDecodeArenaForSlot(
        self: *BootstrappedRuntime,
        slot_index: u16,
    ) []u8 {
        std.debug.assert(slot_index < self.max_query_slots);
        const start = @as(usize, slot_index) * nested_scan_decode_arena_bytes;
        const end = start + nested_scan_decode_arena_bytes;
        std.debug.assert(end <= self.query_nested_decode_arenas.len);
        return self.query_nested_decode_arenas[start..end];
    }

    fn nestedMatchArenaForSlot(
        self: *BootstrappedRuntime,
        slot_index: u16,
    ) []u8 {
        std.debug.assert(slot_index < self.max_query_slots);
        const start = @as(usize, slot_index) * nested_scan_match_arena_bytes;
        const end = start + nested_scan_match_arena_bytes;
        std.debug.assert(end <= self.query_nested_match_arenas.len);
        return self.query_nested_match_arenas[start..end];
    }
};

fn validateMemoryBudget(
    memory_region: []u8,
    storage: Storage,
    config: BootstrapConfig,
) BootstrapError!void {
    std.debug.assert(memory_region.len > 0);
    if (config.max_query_slots == 0) return error.InvalidConfig;
    if (config.wal_buffer_capacity_bytes == 0) return error.InvalidConfig;
    if (config.buffer_pool_frames == 0) return error.InvalidConfig;
    if (config.undo_max_entries == 0) return error.InvalidConfig;
    if (config.undo_max_data_bytes == 0) return error.InvalidConfig;
    if (config.query_string_arena_bytes_per_slot == 0) return error.InvalidConfig;

    var preflight = StaticAllocator.init(memory_region);
    const allocator = preflight.allocator();

    _ = BufferPool.init(
        allocator,
        storage,
        config.buffer_pool_frames,
    ) catch return error.InsufficientMemoryBudget;

    var wal = Wal.init(allocator, storage);
    wal.reserveBufferCapacity(config.wal_buffer_capacity_bytes) catch
        return error.InsufficientMemoryBudget;

    _ = UndoLog.init(
        allocator,
        config.undo_max_entries,
        config.undo_max_data_bytes,
    ) catch return error.InsufficientMemoryBudget;

    const total_rows = std.math.mul(
        usize,
        @as(usize, config.max_query_slots),
        scan_batch_size,
    ) catch return error.InvalidConfig;
    _ = allocator.alloc(bool, config.max_query_slots) catch
        return error.InsufficientMemoryBudget;
    _ = allocator.alloc(ResultRow, total_rows) catch
        return error.InsufficientMemoryBudget;
    _ = allocator.alloc(ResultRow, total_rows) catch
        return error.InsufficientMemoryBudget;
    _ = allocator.alloc(ResultRow, total_rows) catch
        return error.InsufficientMemoryBudget;
    _ = allocator.alloc(ResultRow, total_rows) catch
        return error.InsufficientMemoryBudget;
    const total_string_arena_bytes = std.math.mul(
        usize,
        @as(usize, config.max_query_slots),
        config.query_string_arena_bytes_per_slot,
    ) catch return error.InvalidConfig;
    const total_nested_match_arena_bytes = std.math.mul(
        usize,
        @as(usize, config.max_query_slots),
        nested_scan_match_arena_bytes,
    ) catch return error.InvalidConfig;
    _ = allocator.alloc(u8, total_nested_match_arena_bytes) catch
        return error.InsufficientMemoryBudget;
    const total_nested_decode_arena_bytes = std.math.mul(
        usize,
        @as(usize, config.max_query_slots),
        nested_scan_decode_arena_bytes,
    ) catch return error.InvalidConfig;
    _ = allocator.alloc(u8, total_nested_decode_arena_bytes) catch
        return error.InsufficientMemoryBudget;
    _ = allocator.alloc(u8, total_string_arena_bytes) catch
        return error.InsufficientMemoryBudget;
    _ = allocator.alloc(SpillingResultCollector, config.max_query_slots) catch
        return error.InsufficientMemoryBudget;
}

test "bootstrap seals allocator before runtime operations" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const memory_size_bytes = 256 * 1024 * 1024;
    const backing_memory = try std.testing.allocator.alloc(
        u8,
        memory_size_bytes,
    );
    defer std.testing.allocator.free(backing_memory);
    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{
            .buffer_pool_frames = 8,
            .undo_max_entries = 128,
            .undo_max_data_bytes = 16 * 1024,
            .wal_buffer_capacity_bytes = 512,
            .wal_flush_threshold_bytes = 0,
            .max_query_slots = 1,
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

    const memory_size_bytes = 256 * 1024 * 1024;
    const backing_memory = try std.testing.allocator.alloc(
        u8,
        memory_size_bytes,
    );
    defer std.testing.allocator.free(backing_memory);
    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{
            .buffer_pool_frames = 4,
            .undo_max_entries = 64,
            .undo_max_data_bytes = 8 * 1024,
            .wal_buffer_capacity_bytes = 32,
            .wal_flush_threshold_bytes = 0,
            .max_query_slots = 1,
        },
    );
    defer runtime.deinit();

    var payload: [48]u8 = [_]u8{1} ** 48;
    try std.testing.expectError(
        error.OutOfMemory,
        runtime.wal.append(1, .insert, 7, payload[0..]),
    );
}

test "query buffer slots are bounded and reusable" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const memory_size_bytes = 512 * 1024 * 1024;
    const backing_memory = try std.testing.allocator.alloc(
        u8,
        memory_size_bytes,
    );
    defer std.testing.allocator.free(backing_memory);
    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{
            .max_query_slots = 2,
        },
    );
    defer runtime.deinit();

    var slot0 = try runtime.acquireQueryBuffers();
    var slot1 = try runtime.acquireQueryBuffers();

    try std.testing.expectEqual(@as(u16, 0), slot0.slot_index);
    try std.testing.expectEqual(@as(u16, 1), slot1.slot_index);
    try std.testing.expectError(
        error.NoQuerySlotAvailable,
        runtime.acquireQueryBuffers(),
    );

    slot0.result_rows[0] = scan_mod.ResultRow.init();
    slot1.result_rows[0] = scan_mod.ResultRow.init();

    try runtime.releaseQueryBuffers(slot0.slot_index);
    const slot0_again = try runtime.acquireQueryBuffers();
    try std.testing.expectEqual(@as(u16, 0), slot0_again.slot_index);

    try runtime.releaseQueryBuffers(slot1.slot_index);
    try runtime.releaseQueryBuffers(slot0_again.slot_index);
}

test "release query buffers rejects invalid slot" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const memory_size_bytes = 256 * 1024 * 1024;
    const backing_memory = try std.testing.allocator.alloc(
        u8,
        memory_size_bytes,
    );
    defer std.testing.allocator.free(backing_memory);
    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{
            .max_query_slots = 1,
        },
    );
    defer runtime.deinit();

    try std.testing.expectError(
        error.InvalidQuerySlot,
        runtime.releaseQueryBuffers(0),
    );
    _ = try runtime.acquireQueryBuffers();
    try std.testing.expectError(
        error.InvalidQuerySlot,
        runtime.releaseQueryBuffers(9),
    );
}

test "bootstrap rejects insufficient memory budget explicitly" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    try std.testing.expectError(
        error.InsufficientMemoryBudget,
        BootstrappedRuntime.init(
            backing_memory,
            disk.storage(),
            .{ .max_query_slots = 1 },
        ),
    );
}

test "bootstrap rejects invalid zero query-slot config" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        32 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    try std.testing.expectError(
        error.InvalidConfig,
        BootstrappedRuntime.init(
            backing_memory,
            disk.storage(),
            .{ .max_query_slots = 0 },
        ),
    );
}
