const std = @import("std");

pub const TxId = u64;

pub const TxState = enum {
    active,
    committed,
    aborted,
};

pub const max_active_transactions: u16 = 256;
pub const max_tx_states: u32 = 65536;

/// A snapshot captures the active transaction set at the time
/// a transaction begins. Used for visibility checks.
pub const Snapshot = struct {
    tx_id: TxId,
    active_ids: [max_active_transactions]TxId,
    active_count: u16,
    min_active: TxId,
    next_tx: TxId,

    pub fn deinit(self: *Snapshot) void {
        _ = self;
    }

    pub fn isVisible(self: *const Snapshot, writer_tx: TxId, state: TxState) bool {
        std.debug.assert(self.active_count <= max_active_transactions);

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
    TooManyActiveTransactions,
    TxStateWindowFull,
    TransactionNotActive,
};

/// Transaction manager with fixed-capacity active and state tracking.
pub const TxManager = struct {
    next_tx_id: TxId = 1,
    base_tx_id: TxId = 1,
    states: [max_tx_states]TxState,
    active_list: [max_active_transactions]TxId,
    active_count: u16 = 0,
    oldest_active: TxId = 1,

    pub fn init(allocator: std.mem.Allocator) TxManager {
        _ = allocator;
        return .{
            .states = [_]TxState{.committed} ** max_tx_states,
            .active_list = [_]TxId{0} ** max_active_transactions,
        };
    }

    pub fn deinit(self: *TxManager) void {
        _ = self;
    }

    pub fn begin(self: *TxManager) TxManagerError!TxId {
        std.debug.assert(self.active_count <= max_active_transactions);
        std.debug.assert(self.base_tx_id <= self.next_tx_id);

        if (self.active_count >= max_active_transactions) {
            return error.TooManyActiveTransactions;
        }

        const tx_id = self.next_tx_id;
        if (tx_id >= self.base_tx_id + max_tx_states) {
            return error.TxStateWindowFull;
        }

        self.next_tx_id += 1;
        self.active_list[self.active_count] = tx_id;
        self.active_count += 1;
        std.debug.assert(self.active_count <= max_active_transactions);

        const idx = self.stateSlot(tx_id);
        self.states[idx] = .active;
        self.updateOldestActive();

        return tx_id;
    }

    pub fn snapshot(self: *TxManager, tx_id: TxId) TxManagerError!Snapshot {
        std.debug.assert(self.active_count <= max_active_transactions);

        const owner_idx = self.findActiveIndex(tx_id) orelse {
            @panic("snapshot owner must be active");
        };
        _ = owner_idx;

        var snap = Snapshot{
            .tx_id = tx_id,
            .active_ids = [_]TxId{0} ** max_active_transactions,
            .active_count = 0,
            .min_active = tx_id,
            .next_tx = self.next_tx_id,
        };

        var min_active: TxId = std.math.maxInt(TxId);
        var i: u16 = 0;
        while (i < self.active_count) : (i += 1) {
            const active_tx = self.active_list[i];
            if (active_tx == tx_id) continue;

            std.debug.assert(snap.active_count < max_active_transactions);
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
        std.debug.assert(self.active_count <= max_active_transactions);
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
        std.debug.assert(self.active_count <= max_active_transactions);
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
        if (tx_id >= self.base_tx_id + max_tx_states) return null;
        return self.states[self.stateSlot(tx_id)];
    }

    pub fn getOldestActive(self: *const TxManager) TxId {
        return self.oldest_active;
    }

    pub fn cleanupBefore(self: *TxManager, oldest_needed: TxId) void {
        std.debug.assert(oldest_needed <= self.next_tx_id);
        std.debug.assert(oldest_needed >= self.base_tx_id);

        if (oldest_needed <= self.base_tx_id) return;
        self.base_tx_id = oldest_needed;
    }

    fn stateSlot(self: *const TxManager, tx_id: TxId) usize {
        std.debug.assert(tx_id >= self.base_tx_id);
        return @intCast(tx_id % max_tx_states);
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
        std.debug.assert(self.active_count <= max_active_transactions);
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
    const t3 = try tm.begin();

    var snap = try tm.snapshot(t2);
    defer snap.deinit();

    try std.testing.expect(snap.isVisible(t1, .committed));
    try std.testing.expect(!snap.isVisible(t3, .committed));
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

test "begin fails when active limit reached" {
    var tm = TxManager.init(std.testing.allocator);
    defer tm.deinit();

    var i: u16 = 0;
    while (i < max_active_transactions) : (i += 1) {
        _ = try tm.begin();
    }

    try std.testing.expectError(error.TooManyActiveTransactions, tm.begin());
}

test "cleanupBefore treats old tx states as committed" {
    var tm = TxManager.init(std.testing.allocator);
    defer tm.deinit();

    const t1 = try tm.begin();
    try tm.commit(t1);

    tm.cleanupBefore(tm.getOldestActive());
    try std.testing.expectEqual(TxState.committed, tm.getState(t1).?);
}
