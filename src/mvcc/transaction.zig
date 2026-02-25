//! Fixed-capacity transaction manager and snapshot visibility rules.
//!
//! Responsibilities in this file:
//! - Assigns transaction IDs and tracks active/committed/aborted states.
//! - Produces snapshots for repeatable visibility checks.
//! - Exposes oldest-active boundaries used by undo truncation policy.
//! - Enforces bounded state windows with explicit exhaustion errors.
const std = @import("std");

pub const TxId = u64;

pub const TxState = enum {
    active,
    committed,
    aborted,
};

pub const default_max_active_transactions: u16 = 256;
pub const default_max_tx_states: u32 = 65536;

pub const TxManagerConfig = struct {
    max_active_transactions: u16 = default_max_active_transactions,
    max_tx_states: u32 = default_max_tx_states,
};

/// A snapshot captures the active transaction set at the time
/// a transaction begins. Used for visibility checks.
pub const Snapshot = struct {
    tx_id: TxId,
    active_ids: []TxId,
    active_count: u16,
    min_active: TxId,
    next_tx: TxId,
    owns_active_ids: bool = false,
    allocator: ?std.mem.Allocator = null,

    pub fn deinit(self: *Snapshot) void {
        if (self.owns_active_ids) {
            self.allocator.?.free(self.active_ids);
        }
        self.* = undefined;
    }

    pub fn isVisible(self: *const Snapshot, writer_tx: TxId, state: TxState) bool {
        std.debug.assert(self.active_count <= self.active_ids.len);

        if (writer_tx == self.tx_id) return true;
        if (writer_tx >= self.next_tx) return false;

        var i: u16 = 0;
        while (i < self.active_count) : (i += 1) {
            if (self.active_ids[i] == writer_tx) return false;
        }

        return state == .committed;
    }
};

pub const TxManagerError = error{
    OutOfMemory,
    TooManyActiveTransactions,
    TxStateWindowFull,
    TransactionNotActive,
    SnapshotBufferTooSmall,
};

/// Transaction manager with fixed-capacity active and state tracking.
pub const TxManager = struct {
    allocator: std.mem.Allocator,
    max_active_transactions: u16,
    max_tx_states: u32,
    next_tx_id: TxId = 1,
    base_tx_id: TxId = 1,
    states: []TxState,
    active_list: []TxId,
    active_count: u16 = 0,
    oldest_active: TxId = 1,

    pub fn init(
        allocator: std.mem.Allocator,
        config: TxManagerConfig,
    ) TxManagerError!TxManager {
        if (config.max_active_transactions == 0) return error.TooManyActiveTransactions;
        if (config.max_tx_states == 0) return error.TxStateWindowFull;

        const states = allocator.alloc(TxState, config.max_tx_states) catch
            return error.OutOfMemory;
        errdefer allocator.free(states);
        @memset(states, .committed);

        const active_list = allocator.alloc(TxId, config.max_active_transactions) catch
            return error.OutOfMemory;
        errdefer allocator.free(active_list);
        @memset(active_list, 0);

        return .{
            .allocator = allocator,
            .max_active_transactions = config.max_active_transactions,
            .max_tx_states = config.max_tx_states,
            .states = states,
            .active_list = active_list,
        };
    }

    pub fn deinit(self: *TxManager) void {
        self.allocator.free(self.active_list);
        self.allocator.free(self.states);
        self.* = undefined;
    }

    pub fn begin(self: *TxManager) TxManagerError!TxId {
        std.debug.assert(self.active_count <= self.max_active_transactions);
        std.debug.assert(self.base_tx_id <= self.next_tx_id);

        if (self.active_count >= self.max_active_transactions) {
            return error.TooManyActiveTransactions;
        }

        const tx_id = self.next_tx_id;
        if (tx_id >= self.base_tx_id + self.max_tx_states) {
            return error.TxStateWindowFull;
        }

        self.next_tx_id += 1;
        self.active_list[self.active_count] = tx_id;
        self.active_count += 1;
        std.debug.assert(self.active_count <= self.max_active_transactions);

        const idx = self.stateSlot(tx_id);
        self.states[idx] = .active;
        self.updateOldestActive();

        return tx_id;
    }

    pub fn snapshot(
        self: *TxManager,
        tx_id: TxId,
    ) TxManagerError!Snapshot {
        const active_ids = self.allocator.alloc(TxId, self.max_active_transactions) catch
            return error.OutOfMemory;
        errdefer self.allocator.free(active_ids);
        var snap = try self.snapshotInto(tx_id, active_ids);
        snap.owns_active_ids = true;
        snap.allocator = self.allocator;
        return snap;
    }

    pub fn snapshotInto(
        self: *TxManager,
        tx_id: TxId,
        active_ids_buffer: []TxId,
    ) TxManagerError!Snapshot {
        std.debug.assert(self.active_count <= self.max_active_transactions);
        if (active_ids_buffer.len < self.max_active_transactions) {
            return error.SnapshotBufferTooSmall;
        }

        const owner_idx = self.findActiveIndex(tx_id) orelse {
            @panic("snapshot owner must be active");
        };
        _ = owner_idx;

        var snap = Snapshot{
            .tx_id = tx_id,
            .active_ids = active_ids_buffer,
            .active_count = 0,
            .min_active = tx_id,
            .next_tx = self.next_tx_id,
        };
        @memset(snap.active_ids, 0);

        var min_active: TxId = std.math.maxInt(TxId);
        var i: u16 = 0;
        while (i < self.active_count) : (i += 1) {
            const active_tx = self.active_list[i];
            if (active_tx == tx_id) continue;

            std.debug.assert(snap.active_count < self.max_active_transactions);
            snap.active_ids[snap.active_count] = active_tx;
            snap.active_count += 1;
            if (active_tx < min_active) min_active = active_tx;
        }

        if (min_active != std.math.maxInt(TxId)) {
            snap.min_active = min_active;
        }

        return snap;
    }

    /// Commit a transaction.
    ///
    /// Caller is responsible for invoking undo truncation after commit/abort,
    /// using `getOldestActive()`.
    pub fn commit(self: *TxManager, tx_id: TxId) TxManagerError!void {
        std.debug.assert(tx_id > 0);
        std.debug.assert(self.active_count <= self.max_active_transactions);
        const state = self.getState(tx_id) orelse return error.TransactionNotActive;
        if (state != .active) {
            @panic("commit requires active transaction");
        }

        const idx = self.findActiveIndex(tx_id) orelse {
            @panic("active transaction missing from active list");
        };

        self.removeActiveAt(idx);
        self.states[self.stateSlot(tx_id)] = .committed;
        self.updateOldestActive();
        std.debug.assert(self.getState(tx_id) == .committed);
    }

    /// Abort a transaction.
    ///
    /// Caller is responsible for invoking undo truncation after commit/abort,
    /// using `getOldestActive()`.
    pub fn abort(self: *TxManager, tx_id: TxId) TxManagerError!void {
        std.debug.assert(tx_id > 0);
        std.debug.assert(self.active_count <= self.max_active_transactions);
        const state = self.getState(tx_id) orelse return error.TransactionNotActive;
        if (state != .active) {
            @panic("abort requires active transaction");
        }

        const idx = self.findActiveIndex(tx_id) orelse {
            @panic("active transaction missing from active list");
        };

        self.removeActiveAt(idx);
        self.states[self.stateSlot(tx_id)] = .aborted;
        self.updateOldestActive();
        std.debug.assert(self.getState(tx_id) == .aborted);
    }

    pub fn getState(self: *const TxManager, tx_id: TxId) ?TxState {
        if (tx_id >= self.next_tx_id) return null;
        if (tx_id < self.base_tx_id) return .committed;
        if (tx_id >= self.base_tx_id + self.max_tx_states) return null;
        return self.states[self.stateSlot(tx_id)];
    }

    pub fn getOldestActive(self: *const TxManager) TxId {
        return self.oldest_active;
    }

    pub fn getActiveCount(self: *const TxManager) u16 {
        return self.active_count;
    }

    pub fn getNextTxId(self: *const TxManager) TxId {
        return self.next_tx_id;
    }

    pub fn getBaseTxId(self: *const TxManager) TxId {
        return self.base_tx_id;
    }

    /// Predict the oldest-active watermark immediately after `tx_id` commits.
    /// Requires `tx_id` to be currently active.
    pub fn oldestActiveAfterCommit(self: *const TxManager, tx_id: TxId) TxId {
        const state = self.getState(tx_id) orelse @panic("unknown tx in oldestActiveAfterCommit");
        if (state != .active) @panic("oldestActiveAfterCommit requires active tx");

        var min_other: TxId = std.math.maxInt(TxId);
        var i: u16 = 0;
        while (i < self.active_count) : (i += 1) {
            const active_tx = self.active_list[i];
            if (active_tx == tx_id) continue;
            if (active_tx < min_other) min_other = active_tx;
        }
        if (min_other != std.math.maxInt(TxId)) return min_other;
        return self.next_tx_id;
    }

    pub fn cleanupBefore(self: *TxManager, oldest_needed: TxId) void {
        std.debug.assert(oldest_needed <= self.next_tx_id);
        std.debug.assert(oldest_needed >= self.base_tx_id);

        if (oldest_needed <= self.base_tx_id) return;
        self.base_tx_id = oldest_needed;
    }

    fn stateSlot(self: *const TxManager, tx_id: TxId) usize {
        std.debug.assert(tx_id >= self.base_tx_id);
        return @intCast(tx_id % self.max_tx_states);
    }

    fn findActiveIndex(self: *const TxManager, tx_id: TxId) ?u16 {
        var i: u16 = 0;
        while (i < self.active_count) : (i += 1) {
            if (self.active_list[i] == tx_id) return i;
        }
        return null;
    }

    fn removeActiveAt(self: *TxManager, idx: u16) void {
        std.debug.assert(self.active_count > 0);
        std.debug.assert(idx < self.active_count);

        const last = self.active_count - 1;
        self.active_list[idx] = self.active_list[last];
        self.active_list[last] = 0;
        self.active_count = last;
    }

    fn updateOldestActive(self: *TxManager) void {
        std.debug.assert(self.active_count <= self.max_active_transactions);
        std.debug.assert(self.base_tx_id <= self.next_tx_id);
        if (self.active_count == 0) {
            self.oldest_active = self.next_tx_id;
            return;
        }

        var min: TxId = self.active_list[0];
        var i: u16 = 1;
        while (i < self.active_count) : (i += 1) {
            if (self.active_list[i] < min) min = self.active_list[i];
        }
        self.oldest_active = min;
        std.debug.assert(self.oldest_active < self.next_tx_id);
    }
};

// --- Tests ---

test "begin assigns sequential IDs" {
    var tm = try TxManager.init(std.testing.allocator, .{});
    defer tm.deinit();

    const t1 = try tm.begin();
    const t2 = try tm.begin();
    const t3 = try tm.begin();

    try std.testing.expectEqual(@as(TxId, 1), t1);
    try std.testing.expectEqual(@as(TxId, 2), t2);
    try std.testing.expectEqual(@as(TxId, 3), t3);
}

test "commit changes state" {
    var tm = try TxManager.init(std.testing.allocator, .{});
    defer tm.deinit();

    const t1 = try tm.begin();
    try std.testing.expectEqual(TxState.active, tm.getState(t1).?);

    try tm.commit(t1);
    try std.testing.expectEqual(TxState.committed, tm.getState(t1).?);
}

test "abort changes state" {
    var tm = try TxManager.init(std.testing.allocator, .{});
    defer tm.deinit();

    const t1 = try tm.begin();
    try tm.abort(t1);
    try std.testing.expectEqual(TxState.aborted, tm.getState(t1).?);
}

test "snapshot sees committed, not active" {
    var tm = try TxManager.init(std.testing.allocator, .{});
    defer tm.deinit();

    const t1 = try tm.begin();
    try tm.commit(t1);

    const t2 = try tm.begin();
    const t3 = try tm.begin();

    const active_ids = try std.testing.allocator.alloc(TxId, tm.max_active_transactions);
    defer std.testing.allocator.free(active_ids);
    var snap = try tm.snapshotInto(t2, active_ids);
    defer snap.deinit();

    try std.testing.expect(snap.isVisible(t1, .committed));
    try std.testing.expect(!snap.isVisible(t3, .committed));
    try std.testing.expect(snap.isVisible(t2, .active));
}

test "snapshot does not see future transactions" {
    var tm = try TxManager.init(std.testing.allocator, .{});
    defer tm.deinit();

    const t1 = try tm.begin();
    const active_ids = try std.testing.allocator.alloc(TxId, tm.max_active_transactions);
    defer std.testing.allocator.free(active_ids);
    var snap = try tm.snapshotInto(t1, active_ids);
    defer snap.deinit();

    const t2 = try tm.begin();
    try tm.commit(t2);

    try std.testing.expect(!snap.isVisible(t2, .committed));
}

test "aborted transaction not visible" {
    var tm = try TxManager.init(std.testing.allocator, .{});
    defer tm.deinit();

    const t1 = try tm.begin();
    try tm.abort(t1);

    const t2 = try tm.begin();
    const active_ids = try std.testing.allocator.alloc(TxId, tm.max_active_transactions);
    defer std.testing.allocator.free(active_ids);
    var snap = try tm.snapshotInto(t2, active_ids);
    defer snap.deinit();

    try std.testing.expect(!snap.isVisible(t1, .aborted));
}

test "oldest active tracks correctly" {
    var tm = try TxManager.init(std.testing.allocator, .{});
    defer tm.deinit();

    const t1 = try tm.begin();
    const t2 = try tm.begin();
    _ = try tm.begin();

    try std.testing.expectEqual(t1, tm.getOldestActive());

    try tm.commit(t1);
    try std.testing.expectEqual(t2, tm.getOldestActive());

    try tm.commit(t2);
    try std.testing.expectEqual(@as(TxId, 3), tm.getOldestActive());
}

test "begin fails when active limit reached" {
    var tm = try TxManager.init(std.testing.allocator, .{});
    defer tm.deinit();

    var i: u16 = 0;
    while (i < tm.max_active_transactions) : (i += 1) {
        _ = try tm.begin();
    }

    try std.testing.expectError(error.TooManyActiveTransactions, tm.begin());
}

test "cleanupBefore treats old tx states as committed" {
    var tm = try TxManager.init(std.testing.allocator, .{});
    defer tm.deinit();

    const t1 = try tm.begin();
    try tm.commit(t1);

    tm.cleanupBefore(tm.getOldestActive());
    try std.testing.expectEqual(TxState.committed, tm.getState(t1).?);
}

test "oldestActiveAfterCommit predicts watermark with and without peers" {
    var tm = try TxManager.init(std.testing.allocator, .{});
    defer tm.deinit();

    const t1 = try tm.begin();
    const t2 = try tm.begin();
    try std.testing.expectEqual(t2, tm.oldestActiveAfterCommit(t1));
    try tm.commit(t2);
    try std.testing.expectEqual(tm.next_tx_id, tm.oldestActiveAfterCommit(t1));
}

test "snapshot rejects undersized active buffer" {
    var tm = try TxManager.init(std.testing.allocator, .{
        .max_active_transactions = 8,
        .max_tx_states = 128,
    });
    defer tm.deinit();

    const tx = try tm.begin();
    var small: [4]TxId = [_]TxId{0} ** 4;
    try std.testing.expectError(
        error.SnapshotBufferTooSmall,
        tm.snapshotInto(tx, small[0..]),
    );
}
