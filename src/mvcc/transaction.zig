const std = @import("std");

pub const TxId = u64;

pub const TxState = enum {
    active,
    committed,
    aborted,
};

/// A snapshot captures the set of committed transaction IDs at the time
/// a transaction begins. Used for visibility checks in snapshot isolation.
pub const Snapshot = struct {
    /// The transaction that owns this snapshot.
    tx_id: TxId,
    /// Transaction IDs that were active (uncommitted) when this snapshot was taken.
    /// A row written by a tx in this set is NOT visible to this snapshot.
    active_set: std.AutoHashMap(TxId, void),
    /// The smallest active tx_id at snapshot time. Any tx_id < min_active
    /// that is not in active_set is guaranteed committed (or aborted).
    min_active: TxId,
    /// The next tx_id that will be assigned. Any tx_id >= next_tx is
    /// from the future and not visible.
    next_tx: TxId,

    pub fn deinit(self: *Snapshot) void {
        self.active_set.deinit();
    }

    /// Returns true if the given tx_id's writes are visible to this snapshot.
    pub fn isVisible(self: *const Snapshot, writer_tx: TxId, state: TxState) bool {
        // Our own writes are always visible.
        if (writer_tx == self.tx_id) return true;

        // Future transactions are never visible.
        if (writer_tx >= self.next_tx) return false;

        // If the writer was active when we took the snapshot, not visible.
        if (self.active_set.contains(writer_tx)) return false;

        // Otherwise, the writer committed before our snapshot — visible
        // only if it actually committed (not aborted).
        return state == .committed;
    }
};

/// Transaction Manager. Tracks active transactions, assigns IDs,
/// creates snapshots, and manages commit/abort.
pub const TxManager = struct {
    next_tx_id: TxId = 1,
    /// Maps tx_id -> state for all tracked transactions.
    tx_states: std.AutoHashMap(TxId, TxState),
    /// Set of currently active transaction IDs.
    active_txs: std.AutoHashMap(TxId, void),
    allocator: std.mem.Allocator,
    /// The oldest active tx_id (used for undo log GC).
    oldest_active: TxId = 0,

    pub fn init(allocator: std.mem.Allocator) TxManager {
        return .{
            .tx_states = std.AutoHashMap(TxId, TxState).init(allocator),
            .active_txs = std.AutoHashMap(TxId, void).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *TxManager) void {
        self.tx_states.deinit();
        self.active_txs.deinit();
    }

    /// Begin a new transaction. Returns the assigned tx_id.
    pub fn begin(self: *TxManager) !TxId {
        const tx_id = self.next_tx_id;
        self.next_tx_id += 1;

        try self.tx_states.put(tx_id, .active);
        try self.active_txs.put(tx_id, {});
        self.updateOldestActive();

        return tx_id;
    }

    /// Take a snapshot for the given transaction (must be active).
    /// The snapshot captures which transactions are currently active.
    pub fn snapshot(self: *TxManager, tx_id: TxId) !Snapshot {
        var active_set = std.AutoHashMap(TxId, void).init(self.allocator);
        var min_active: TxId = std.math.maxInt(TxId);

        var it = self.active_txs.keyIterator();
        while (it.next()) |key| {
            const active_tx = key.*;
            if (active_tx != tx_id) {
                try active_set.put(active_tx, {});
                if (active_tx < min_active) min_active = active_tx;
            }
        }

        if (min_active == std.math.maxInt(TxId)) {
            min_active = tx_id;
        }

        return Snapshot{
            .tx_id = tx_id,
            .active_set = active_set,
            .min_active = min_active,
            .next_tx = self.next_tx_id,
        };
    }

    /// Commit a transaction.
    pub fn commit(self: *TxManager, tx_id: TxId) !void {
        try self.tx_states.put(tx_id, .committed);
        _ = self.active_txs.remove(tx_id);
        self.updateOldestActive();
    }

    /// Abort a transaction.
    pub fn abort(self: *TxManager, tx_id: TxId) !void {
        try self.tx_states.put(tx_id, .aborted);
        _ = self.active_txs.remove(tx_id);
        self.updateOldestActive();
    }

    /// Get the state of a transaction.
    pub fn getState(self: *const TxManager, tx_id: TxId) ?TxState {
        return self.tx_states.get(tx_id);
    }

    /// Returns the oldest active tx_id, used for undo log GC.
    /// Undo entries older than this can be safely discarded.
    pub fn getOldestActive(self: *const TxManager) TxId {
        return self.oldest_active;
    }

    fn updateOldestActive(self: *TxManager) void {
        var min: TxId = std.math.maxInt(TxId);
        var it = self.active_txs.keyIterator();
        while (it.next()) |key| {
            if (key.* < min) min = key.*;
        }
        self.oldest_active = if (min == std.math.maxInt(TxId)) self.next_tx_id else min;
    }
};

// --- Tests ---

test "begin assigns sequential IDs" {
    var tm = TxManager.init(std.testing.allocator);
    defer tm.deinit();

    const t1 = try tm.begin();
    const t2 = try tm.begin();
    const t3 = try tm.begin();

    try std.testing.expectEqual(@as(TxId, 1), t1);
    try std.testing.expectEqual(@as(TxId, 2), t2);
    try std.testing.expectEqual(@as(TxId, 3), t3);
}

test "commit changes state" {
    var tm = TxManager.init(std.testing.allocator);
    defer tm.deinit();

    const t1 = try tm.begin();
    try std.testing.expectEqual(TxState.active, tm.getState(t1).?);

    try tm.commit(t1);
    try std.testing.expectEqual(TxState.committed, tm.getState(t1).?);
}

test "abort changes state" {
    var tm = TxManager.init(std.testing.allocator);
    defer tm.deinit();

    const t1 = try tm.begin();
    try tm.abort(t1);
    try std.testing.expectEqual(TxState.aborted, tm.getState(t1).?);
}

test "snapshot sees committed, not active" {
    var tm = TxManager.init(std.testing.allocator);
    defer tm.deinit();

    const t1 = try tm.begin();
    try tm.commit(t1);

    const t2 = try tm.begin();
    const t3 = try tm.begin(); // active when t2 snapshots

    var snap = try tm.snapshot(t2);
    defer snap.deinit();

    // t1 committed before snapshot — visible.
    try std.testing.expect(snap.isVisible(t1, .committed));
    // t3 was active at snapshot time — not visible even if committed later.
    try std.testing.expect(!snap.isVisible(t3, .committed));
    // Own writes visible.
    try std.testing.expect(snap.isVisible(t2, .active));
}

test "snapshot does not see future transactions" {
    var tm = TxManager.init(std.testing.allocator);
    defer tm.deinit();

    const t1 = try tm.begin();
    var snap = try tm.snapshot(t1);
    defer snap.deinit();

    const t2 = try tm.begin();
    try tm.commit(t2);

    // t2 started after the snapshot — not visible.
    try std.testing.expect(!snap.isVisible(t2, .committed));
}

test "aborted transaction not visible" {
    var tm = TxManager.init(std.testing.allocator);
    defer tm.deinit();

    const t1 = try tm.begin();
    try tm.abort(t1);

    const t2 = try tm.begin();
    var snap = try tm.snapshot(t2);
    defer snap.deinit();

    // t1 aborted — not visible even though it's < next_tx.
    try std.testing.expect(!snap.isVisible(t1, .aborted));
}

test "oldest active tracks correctly" {
    var tm = TxManager.init(std.testing.allocator);
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
