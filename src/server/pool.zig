//! Connection-pool lease model over runtime query slots.
//!
//! Responsibilities in this file:
//! - Manages checkout/checkin lifecycles for bounded query buffer slots.
//! - Starts transactions and snapshots per checked-out pool connection.
//! - Supports overload policies (reject/queue) with lightweight pool stats.
//! - Enforces pin/unpin semantics for connection-scoped long-lived work.
const std = @import("std");
const bootstrap_mod = @import("../runtime/bootstrap.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const disk_mod = @import("../simulator/disk.zig");
const wal_mod = @import("../storage/wal.zig");

const BootstrappedRuntime = bootstrap_mod.BootstrappedRuntime;
const QueryBuffers = bootstrap_mod.QueryBuffers;
const Snapshot = tx_mod.Snapshot;
const TxId = tx_mod.TxId;

pub const PoolError = tx_mod.TxManagerError ||
    bootstrap_mod.QueryBufferError ||
    wal_mod.WalError ||
    error{
        PoolExhausted,
        QueueTimeout,
        InvalidPoolConn,
        PoolConnPinned,
    };

pub const OverloadPolicy = enum {
    reject,
    queue,
};

pub const ConnectionPoolConfig = struct {
    overload_policy: OverloadPolicy = .reject,
    queue_wait_spins: u32 = 1024,
};

pub const PoolStats = struct {
    overload_policy: OverloadPolicy,
    pool_size: u16,
    checked_out: u16,
    pinned: u16,
    pool_exhausted_total: u64,
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
    config: ConnectionPoolConfig,
    checked_out_count: u16,
    pinned_count: u16,
    pool_exhausted_total: u64,

    pub fn init(runtime: *BootstrappedRuntime) ConnectionPool {
        return initWithConfig(runtime, .{});
    }

    pub fn initWithConfig(
        runtime: *BootstrappedRuntime,
        config: ConnectionPoolConfig,
    ) ConnectionPool {
        std.debug.assert(runtime.static_allocator.isSealed());
        return .{
            .runtime = runtime,
            .config = config,
            .checked_out_count = 0,
            .pinned_count = 0,
            .pool_exhausted_total = 0,
        };
    }

    pub fn checkout(self: *ConnectionPool) PoolError!PoolConn {
        const query_buffers = self.runtime.acquireQueryBuffers() catch |err| {
            if (err == error.NoQuerySlotAvailable) {
                self.pool_exhausted_total += 1;
                return switch (self.config.overload_policy) {
                    .reject => error.PoolExhausted,
                    .queue => self.checkoutQueued(),
                };
            }
            return err;
        };
        return self.makePoolConn(query_buffers);
    }

    pub fn checkin(self: *ConnectionPool, conn: *PoolConn) PoolError!void {
        if (!conn.checked_out) return error.InvalidPoolConn;
        if (conn.pinned) return error.PoolConnPinned;

        _ = try self.runtime.wal.commitTx(conn.tx_id);
        _ = try self.runtime.wal.flushIfNeeded();
        conn.snapshot.deinit();
        try self.runtime.tx_manager.commit(conn.tx_id);
        try self.runtime.releaseQueryBuffers(conn.slot_index);
        std.debug.assert(self.checked_out_count > 0);
        self.checked_out_count -= 1;

        conn.checked_out = false;
    }

    pub fn abortCheckin(self: *ConnectionPool, conn: *PoolConn) PoolError!void {
        if (!conn.checked_out) return error.InvalidPoolConn;
        if (conn.pinned) return error.PoolConnPinned;

        _ = try self.runtime.wal.abortTx(conn.tx_id);
        try self.runtime.wal.flush();
        conn.snapshot.deinit();
        try self.runtime.tx_manager.abort(conn.tx_id);
        try self.runtime.releaseQueryBuffers(conn.slot_index);
        std.debug.assert(self.checked_out_count > 0);
        self.checked_out_count -= 1;

        conn.checked_out = false;
    }

    pub fn pin(self: *ConnectionPool, conn: *PoolConn) PoolError!void {
        if (!conn.checked_out) return error.InvalidPoolConn;
        if (conn.pinned) return error.PoolConnPinned;
        std.debug.assert(self.pinned_count < self.checked_out_count);
        self.pinned_count += 1;
        conn.pinned = true;
    }

    pub fn unpin(self: *ConnectionPool, conn: *PoolConn) PoolError!void {
        if (!conn.checked_out) return error.InvalidPoolConn;
        if (!conn.pinned) return error.InvalidPoolConn;
        std.debug.assert(self.pinned_count > 0);
        self.pinned_count -= 1;
        conn.pinned = false;
        try self.checkin(conn);
    }

    pub fn rollbackPinned(self: *ConnectionPool, conn: *PoolConn) PoolError!void {
        if (!conn.checked_out) return error.InvalidPoolConn;
        if (!conn.pinned) return error.InvalidPoolConn;
        std.debug.assert(self.pinned_count > 0);
        self.pinned_count -= 1;
        conn.pinned = false;
        try self.abortCheckin(conn);
    }

    pub fn snapshotStats(self: *const ConnectionPool) PoolStats {
        return .{
            .overload_policy = self.config.overload_policy,
            .pool_size = self.runtime.max_query_slots,
            .checked_out = self.checked_out_count,
            .pinned = self.pinned_count,
            .pool_exhausted_total = self.pool_exhausted_total,
        };
    }

    fn checkoutQueued(self: *ConnectionPool) PoolError!PoolConn {
        var spins: u32 = 0;
        while (spins < self.config.queue_wait_spins) : (spins += 1) {
            const query_buffers = self.runtime.acquireQueryBuffers() catch |err| {
                if (err == error.NoQuerySlotAvailable) {
                    std.Thread.yield() catch {};
                    continue;
                }
                return err;
            };
            return try self.makePoolConn(query_buffers);
        }
        return error.QueueTimeout;
    }

    fn makePoolConn(
        self: *ConnectionPool,
        query_buffers: QueryBuffers,
    ) PoolError!PoolConn {
        errdefer releaseQuerySlotSafely(self.runtime, query_buffers.slot_index);

        const tx_id = try self.runtime.tx_manager.begin();
        errdefer self.runtime.tx_manager.abort(tx_id) catch {};
        _ = try self.runtime.wal.beginTx(tx_id);

        const snapshot = try self.runtime.tx_manager.snapshot(tx_id);
        std.debug.assert(self.checked_out_count < self.runtime.max_query_slots);
        self.checked_out_count += 1;

        return .{
            .slot_index = query_buffers.slot_index,
            .query_buffers = query_buffers,
            .tx_id = tx_id,
            .snapshot = snapshot,
            .pinned = false,
            .checked_out = true,
        };
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

test "abortCheckin aborts transaction and releases slot for reuse" {
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
    const tx = first.tx_id;
    try pool.abortCheckin(&first);
    try std.testing.expect(runtime.tx_manager.getState(tx).? == .aborted);

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

test "queue overload policy waits then times out when pool stays exhausted" {
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

    var pool = ConnectionPool.initWithConfig(&runtime, .{
        .overload_policy = .queue,
        .queue_wait_spins = 8,
    });
    var held = try pool.checkout();
    defer pool.checkin(&held) catch {};

    try std.testing.expectError(
        error.QueueTimeout,
        pool.checkout(),
    );
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

test "rollbackPinned aborts pinned lease and releases slot" {
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
    const tx_id = conn.tx_id;
    try pool.pin(&conn);
    try pool.rollbackPinned(&conn);

    try std.testing.expect(runtime.tx_manager.getState(tx_id).? == .aborted);
    try std.testing.expectEqual(@as(u16, 0), pool.snapshotStats().checked_out);
    try std.testing.expectEqual(@as(u16, 0), pool.snapshotStats().pinned);

    var reused = try pool.checkout();
    defer pool.checkin(&reused) catch {};
    try std.testing.expectEqual(@as(u16, 0), reused.slot_index);
}

test "snapshotStats tracks checkout pin and checkin counters" {
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
        .{ .max_query_slots = 2 },
    );
    defer runtime.deinit();

    var pool = ConnectionPool.init(&runtime);
    const initial = pool.snapshotStats();
    try std.testing.expectEqual(@as(u16, 2), initial.pool_size);
    try std.testing.expectEqual(@as(u16, 0), initial.checked_out);
    try std.testing.expectEqual(@as(u16, 0), initial.pinned);

    var conn = try pool.checkout();
    const checked_out = pool.snapshotStats();
    try std.testing.expectEqual(@as(u16, 1), checked_out.checked_out);
    try std.testing.expectEqual(@as(u16, 0), checked_out.pinned);

    try pool.pin(&conn);
    const pinned = pool.snapshotStats();
    try std.testing.expectEqual(@as(u16, 1), pinned.checked_out);
    try std.testing.expectEqual(@as(u16, 1), pinned.pinned);

    try pool.unpin(&conn);
    const released = pool.snapshotStats();
    try std.testing.expectEqual(@as(u16, 0), released.checked_out);
    try std.testing.expectEqual(@as(u16, 0), released.pinned);
}
