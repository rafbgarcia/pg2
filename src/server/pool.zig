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
const wal_mod = @import("../storage/wal.zig");
const heap_mod = @import("../storage/heap.zig");

const BootstrappedRuntime = bootstrap_mod.BootstrappedRuntime;
const QueryBuffers = bootstrap_mod.QueryBuffers;
const Snapshot = tx_mod.Snapshot;
const TxId = tx_mod.TxId;
const HeapPage = heap_mod.HeapPage;

pub const PoolError = tx_mod.TxManagerError ||
    bootstrap_mod.QueryBufferError ||
    wal_mod.WalError ||
    error{
        PoolExhausted,
        QueueTimeout,
        InvalidPoolConn,
        PoolConnPinned,
        Corruption,
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
        const oldest_active = self.runtime.tx_manager.getOldestActive();
        self.runtime.undo_log.truncate(oldest_active);
        self.runtime.tx_manager.cleanupBefore(oldest_active);
        try self.runtime.releaseQueryBuffers(conn.slot_index);
        std.debug.assert(self.checked_out_count > 0);
        self.checked_out_count -= 1;

        conn.checked_out = false;
    }

    pub fn abortCheckin(self: *ConnectionPool, conn: *PoolConn) PoolError!void {
        if (!conn.checked_out) return error.InvalidPoolConn;
        if (conn.pinned) return error.PoolConnPinned;

        try self.rollbackHeapUndoForTx(conn.tx_id);
        _ = try self.runtime.wal.abortTx(conn.tx_id);
        try self.runtime.wal.flush();
        conn.snapshot.deinit();
        try self.runtime.tx_manager.abort(conn.tx_id);
        const oldest_active = self.runtime.tx_manager.getOldestActive();
        self.runtime.undo_log.truncate(oldest_active);
        self.runtime.tx_manager.cleanupBefore(oldest_active);
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

        const snapshot = try self.runtime.tx_manager.snapshotInto(
            tx_id,
            query_buffers.snapshot_active_ids,
        );
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

    fn rollbackHeapUndoForTx(self: *ConnectionPool, tx_id: TxId) PoolError!void {
        const undo_log = &self.runtime.undo_log;
        if (undo_log.count == 0) return;

        const base = undo_log.base_index;
        var idx = base + undo_log.count;
        while (idx > base) {
            idx -= 1;
            const entry = undo_log.get(idx) orelse continue;
            if (entry.tx_id != tx_id) continue;

            const page = self.runtime.pool.pin(entry.page_id) catch |pin_err| {
                return switch (pin_err) {
                    error.AllFramesPinned => error.PoolExhausted,
                    error.OutOfMemory => error.PoolExhausted,
                    error.ChecksumMismatch => error.Corruption,
                    error.StorageRead => error.WalReadError,
                    error.StorageWrite => error.WalWriteError,
                    error.StorageFsync => error.WalFsyncError,
                    error.WalNotFlushed => error.WalWriteError,
                };
            };
            defer self.runtime.pool.unpin(entry.page_id, true);
            if (page.header.page_type != .heap) return error.Corruption;

            const start: usize = @intCast(entry.data_offset);
            const old_data = undo_log.data_buffer[start .. start + entry.data_length];
            HeapPage.restore(page, entry.slot, old_data) catch |restore_err| {
                return switch (restore_err) {
                    error.InvalidSlot => error.Corruption,
                    error.PageFull => error.Corruption,
                    error.RowTooLarge => error.Corruption,
                };
            };
        }
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
