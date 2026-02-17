const std = @import("std");
const tx_mod = @import("transaction.zig");

const TxId = tx_mod.TxId;

/// An entry in the undo log. Stores the previous version of a row
/// so that older snapshots can see it.
pub const UndoEntry = struct {
    /// The transaction that created this undo entry (i.e., the tx that
    /// overwrote the old version).
    tx_id: TxId,
    /// Page and slot where the row lives.
    page_id: u64,
    slot: u16,
    /// The old row data before the update.
    old_data: []const u8,
    /// Pointer to the previous undo entry for the same row (version chain).
    /// null means this is the oldest version.
    prev: ?usize,
};

/// Append-only undo log. Stores old row versions for MVCC visibility.
///
/// Each entry records the previous state of a row before an update or delete.
/// Readers follow the version chain to find a version visible to their snapshot.
///
/// Garbage collection: entries older than the oldest active transaction can be
/// safely discarded (truncate from the front).
pub const UndoLog = struct {
    entries: std.ArrayList(UndoEntry) = .{},
    allocator: std.mem.Allocator,

    /// Maps (page_id, slot) -> index of latest undo entry for that row.
    /// This is the head of the version chain.
    row_heads: std.AutoHashMap(RowKey, usize),

    const RowKey = struct { page_id: u64, slot: u16 };

    pub fn init(allocator: std.mem.Allocator) UndoLog {
        return .{
            .allocator = allocator,
            .row_heads = std.AutoHashMap(RowKey, usize).init(allocator),
        };
    }

    pub fn deinit(self: *UndoLog) void {
        // Free all owned old_data slices.
        for (self.entries.items) |entry| {
            if (entry.old_data.len > 0) {
                self.allocator.free(entry.old_data);
            }
        }
        self.entries.deinit(self.allocator);
        self.row_heads.deinit();
    }

    /// Push an undo entry. `old_data` is copied (owned by the undo log).
    /// Returns the index of the new entry.
    pub fn push(self: *UndoLog, tx_id: TxId, page_id: u64, slot: u16, old_data: []const u8) !usize {
        const key = RowKey{ .page_id = page_id, .slot = slot };

        // Link to previous entry for this row.
        const prev = self.row_heads.get(key);

        // Copy old data.
        const owned_data = try self.allocator.alloc(u8, old_data.len);
        @memcpy(owned_data, old_data);

        const idx = self.entries.items.len;
        try self.entries.append(self.allocator, .{
            .tx_id = tx_id,
            .page_id = page_id,
            .slot = slot,
            .old_data = owned_data,
            .prev = prev,
        });

        try self.row_heads.put(key, idx);
        return idx;
    }

    /// Get an undo entry by index.
    pub fn get(self: *const UndoLog, idx: usize) ?*const UndoEntry {
        if (idx >= self.entries.items.len) return null;
        return &self.entries.items[idx];
    }

    /// Get the head of the version chain for a row.
    pub fn getHead(self: *const UndoLog, page_id: u64, slot: u16) ?usize {
        return self.row_heads.get(.{ .page_id = page_id, .slot = slot });
    }

    /// Walk the version chain for a row, looking for a version visible
    /// to the given snapshot. Returns the old_data of the visible version,
    /// or null if the current heap version is already visible.
    ///
    /// Algorithm: Walk from newest undo entry to oldest. Each entry says
    /// "tx T overwrote old_data D". If T is visible to us, then T's write
    /// (the current heap or the next-newer entry's state) is what we see,
    /// so return null (use heap). If T is NOT visible, its old_data D is
    /// the state before T — but D might have been written by another
    /// invisible tx. Keep walking. If we exhaust the chain without finding
    /// a visible writer, the oldest entry's old_data is the pre-history
    /// version we should see.
    pub fn findVisible(
        self: *const UndoLog,
        page_id: u64,
        slot: u16,
        snap: *const tx_mod.Snapshot,
        tm: *const tx_mod.TxManager,
    ) ?[]const u8 {
        var idx_opt = self.getHead(page_id, slot);
        var result: ?[]const u8 = null;

        while (idx_opt) |idx| {
            const entry = self.get(idx) orelse break;
            const writer_state = tm.getState(entry.tx_id) orelse break;

            if (snap.isVisible(entry.tx_id, writer_state)) {
                // This writer is visible. The version it created (which is
                // either the current heap version or the state recorded in
                // the next-newer entry) is what we should see. Since we
                // walk newest-to-oldest, `result` holds the correct answer:
                // null means use the heap (no newer invisible write), or
                // the old_data of the most recent invisible entry.
                return result;
            }

            // This writer is NOT visible. The state before this write
            // (entry.old_data) is a candidate for what we should see.
            result = entry.old_data;
            idx_opt = entry.prev;
        }

        // Exhausted the chain — no visible writer found.
        // Return the oldest entry's old_data (the pre-history version).
        return result;
    }

    /// Returns the number of entries in the log.
    pub fn len(self: *const UndoLog) usize {
        return self.entries.items.len;
    }
};

// --- Tests ---

test "push and get undo entry" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    const idx = try log.push(1, 0, 0, "old value");
    const entry = log.get(idx).?;
    try std.testing.expectEqual(@as(TxId, 1), entry.tx_id);
    try std.testing.expectEqualSlices(u8, "old value", entry.old_data);
    try std.testing.expect(entry.prev == null);
}

test "version chain links entries" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    const idx0 = try log.push(1, 0, 0, "version 1");
    const idx1 = try log.push(2, 0, 0, "version 2");

    const entry1 = log.get(idx1).?;
    try std.testing.expectEqual(@as(?usize, idx0), entry1.prev);

    const entry0 = log.get(idx0).?;
    try std.testing.expect(entry0.prev == null);
}

test "getHead returns latest entry" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    _ = try log.push(1, 0, 0, "v1");
    const idx1 = try log.push(2, 0, 0, "v2");

    try std.testing.expectEqual(@as(?usize, idx1), log.getHead(0, 0));
}

test "different rows have independent chains" {
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    const a = try log.push(1, 0, 0, "row A");
    const b = try log.push(1, 0, 1, "row B");

    try std.testing.expectEqual(@as(?usize, a), log.getHead(0, 0));
    try std.testing.expectEqual(@as(?usize, b), log.getHead(0, 1));
}

test "findVisible returns correct version" {
    var tm = tx_mod.TxManager.init(std.testing.allocator);
    defer tm.deinit();
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // t1 inserts a row with "original".
    const t1 = try tm.begin();
    try tm.commit(t1);

    // t2 reads (takes snapshot).
    const t2 = try tm.begin();
    var snap = try tm.snapshot(t2);
    defer snap.deinit();

    // t3 updates the row (after t2's snapshot).
    const t3 = try tm.begin();
    _ = try log.push(t3, 0, 0, "original");
    try tm.commit(t3);

    // t2 should see "original" (the pre-t3 version).
    const visible = log.findVisible(0, 0, &snap, &tm);
    try std.testing.expect(visible != null);
    try std.testing.expectEqualSlices(u8, "original", visible.?);
}

test "findVisible returns null when current version is visible" {
    var tm = tx_mod.TxManager.init(std.testing.allocator);
    defer tm.deinit();
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // t1 updates a row.
    const t1 = try tm.begin();
    _ = try log.push(t1, 0, 0, "before t1");
    try tm.commit(t1);

    // t2 starts after t1 committed — t1's write is visible.
    const t2 = try tm.begin();
    var snap = try tm.snapshot(t2);
    defer snap.deinit();

    // The current heap version (written by t1) IS visible to t2.
    // So findVisible should return null (use the current heap version).
    const visible = log.findVisible(0, 0, &snap, &tm);
    try std.testing.expect(visible == null);
}

test "findVisible walks chain for deep history" {
    var tm = tx_mod.TxManager.init(std.testing.allocator);
    defer tm.deinit();
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();

    // t1 creates original.
    const t1 = try tm.begin();
    try tm.commit(t1);

    // t2 takes snapshot.
    const t2 = try tm.begin();
    var snap = try tm.snapshot(t2);
    defer snap.deinit();

    // t3 updates (after snapshot).
    const t3 = try tm.begin();
    _ = try log.push(t3, 0, 0, "v1");
    try tm.commit(t3);

    // t4 updates again (after snapshot).
    const t4 = try tm.begin();
    _ = try log.push(t4, 0, 0, "v2");
    try tm.commit(t4);

    // t2 should see "v1" — the version before t3 wrote, which is the
    // old_data stored in the t3 entry. But wait — t3's entry has "v1"
    // and t4's entry has "v2". Walking from head (t4):
    // - t4 not visible -> return "v2"? No.
    // Actually: t4's undo entry stores old_data = "v2" which is what
    // was in the heap before t4 wrote. That was t3's version.
    // t3's undo entry stores old_data = "v1" which is what was in the
    // heap before t3 wrote. That was t1's version.
    //
    // t2's snapshot: t3 and t4 are not visible. Walking from t4's entry:
    // - t4 not visible → result = "v2" (old_data before t4), continue
    // - t3 not visible → result = "v1" (old_data before t3), continue
    // - chain ends → return "v1" (the state before any invisible writer)
    const visible = log.findVisible(0, 0, &snap, &tm);
    try std.testing.expect(visible != null);
    try std.testing.expectEqualSlices(u8, "v1", visible.?);
}
