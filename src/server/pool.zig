const std = @import("std");
const bootstrap_mod = @import("../runtime/bootstrap.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const disk_mod = @import("../simulator/disk.zig");

const BootstrappedRuntime = bootstrap_mod.BootstrappedRuntime;
const QueryBuffers = bootstrap_mod.QueryBuffers;
const Snapshot = tx_mod.Snapshot;
const TxId = tx_mod.TxId;

pub const PoolError = tx_mod.TxManagerError ||
    bootstrap_mod.QueryBufferError ||
    error{
        PoolExhausted,
        InvalidPoolConn,
        PoolConnPinned,
    };

pub const PoolConn = struct {
    slot_index: u16,
    query_buffers: QueryBuffers,
    tx_id: TxId,
    snapshot: Snapshot,
    pinned: bool,
    checked_out: bool,
};

pub const ConnectionPool = struct {
    runtime: *BootstrappedRuntime,
    pool_exhausted_total: u64,

    pub fn init(runtime: *BootstrappedRuntime) ConnectionPool {
        std.debug.assert(runtime.static_allocator.isSealed());
        return .{
            .runtime = runtime,
            .pool_exhausted_total = 0,
        };
    }

    pub fn checkout(self: *ConnectionPool) PoolError!PoolConn {
        const query_buffers = self.runtime.acquireQueryBuffers() catch |err| {
            if (err == error.NoQuerySlotAvailable) {
                self.pool_exhausted_total += 1;
                return error.PoolExhausted;
            }
            return err;
        };
        errdefer releaseQuerySlotSafely(self.runtime, query_buffers.slot_index);

        const tx_id = try self.runtime.tx_manager.begin();
        errdefer self.runtime.tx_manager.abort(tx_id) catch {};

        const snapshot = try self.runtime.tx_manager.snapshot(tx_id);

        return .{
            .slot_index = query_buffers.slot_index,
            .query_buffers = query_buffers,
            .tx_id = tx_id,
            .snapshot = snapshot,
            .pinned = false,
            .checked_out = true,
        };
    }

    pub fn checkin(self: *ConnectionPool, conn: *PoolConn) PoolError!void {
        if (!conn.checked_out) return error.InvalidPoolConn;
        if (conn.pinned) return error.PoolConnPinned;

        conn.snapshot.deinit();
        try self.runtime.tx_manager.commit(conn.tx_id);
        try self.runtime.releaseQueryBuffers(conn.slot_index);

        conn.checked_out = false;
    }

    pub fn pin(self: *ConnectionPool, conn: *PoolConn) PoolError!void {
        _ = self;
        if (!conn.checked_out) return error.InvalidPoolConn;
        conn.pinned = true;
    }

    pub fn unpin(self: *ConnectionPool, conn: *PoolConn) PoolError!void {
        if (!conn.checked_out) return error.InvalidPoolConn;
        if (!conn.pinned) return error.InvalidPoolConn;
        conn.pinned = false;
        try self.checkin(conn);
    }
};

fn releaseQuerySlotSafely(runtime: *BootstrappedRuntime, slot_index: u16) void {
    runtime.releaseQueryBuffers(slot_index) catch |err| {
        std.log.err(
            "pool query slot release failed: slot={d} err={s}",
            .{ slot_index, @errorName(err) },
        );
        if (slot_index < runtime.max_query_slots) {
            runtime.query_slot_in_use[slot_index] = false;
        }
    };
}

test "checkout returns pool connection with active transaction and snapshot" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        256 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );
    defer runtime.deinit();

    var pool = ConnectionPool.init(&runtime);
    var conn = try pool.checkout();
    defer pool.checkin(&conn) catch {};

    try std.testing.expectEqual(@as(u16, 0), conn.slot_index);
    try std.testing.expect(conn.checked_out);
    try std.testing.expect(!conn.pinned);
    try std.testing.expect(conn.snapshot.tx_id == conn.tx_id);
    try std.testing.expect(runtime.tx_manager.getState(conn.tx_id).? == .active);
}

test "checkin releases slot for reuse" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        256 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );
    defer runtime.deinit();

    var pool = ConnectionPool.init(&runtime);
    var first = try pool.checkout();
    try pool.checkin(&first);

    var second = try pool.checkout();
    defer pool.checkin(&second) catch {};
    try std.testing.expectEqual(@as(u16, 0), second.slot_index);
}

test "checkout returns PoolExhausted when all slots are occupied" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        256 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );
    defer runtime.deinit();

    var pool = ConnectionPool.init(&runtime);
    var held = try pool.checkout();
    defer pool.checkin(&held) catch {};

    try std.testing.expectError(error.PoolExhausted, pool.checkout());
    try std.testing.expectEqual(@as(u64, 1), pool.pool_exhausted_total);
}

test "double checkin returns InvalidPoolConn" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        256 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );
    defer runtime.deinit();

    var pool = ConnectionPool.init(&runtime);
    var conn = try pool.checkout();
    try pool.checkin(&conn);

    try std.testing.expectError(error.InvalidPoolConn, pool.checkin(&conn));
}

test "pin and unpin keeps lease until unpin checkin" {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        256 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );
    defer runtime.deinit();

    var pool = ConnectionPool.init(&runtime);
    var conn = try pool.checkout();
    try pool.pin(&conn);
    try std.testing.expectError(error.PoolConnPinned, pool.checkin(&conn));

    try pool.unpin(&conn);
    try std.testing.expectError(error.InvalidPoolConn, pool.checkin(&conn));
}
