const std = @import("std");
const tx_mod = @import("transaction.zig");

const TxId = tx_mod.TxId;

pub const max_version_chain_depth: u16 = 64;
pub const max_row_size_bytes: usize = 8000;

pub const UndoEntry = struct {
    tx_id: TxId,
    page_id: u64,
    slot: u16,
    data_offset: u32,
    data_length: u16,
    prev: ?u64,
};

pub const UndoLog = struct {
    entries: []UndoEntry,
    head: u32 = 0,
    tail: u32 = 0,
    count: u32 = 0,
    base_index: u64 = 0,

    data_buffer: []u8,
    data_head: u32 = 0,
    data_tail: u32 = 0,
    data_used: u32 = 0,

    row_heads: std.AutoHashMap(RowKey, u64),
    allocator: std.mem.Allocator,

    const RowKey = struct {
        page_id: u64,
        slot: u16,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        max_entries: u32,
        max_data_bytes: u32,
    ) !UndoLog {
        std.debug.assert(max_entries > 0);
        std.debug.assert(max_data_bytes > 0);

        const entries = try allocator.alloc(UndoEntry, max_entries);
        const data_buffer = try allocator.alloc(u8, max_data_bytes);
        errdefer allocator.free(data_buffer);
        errdefer allocator.free(entries);

        var row_heads = std.AutoHashMap(RowKey, u64).init(allocator);
        try row_heads.ensureTotalCapacity(max_entries);

        const log: UndoLog = .{
            .entries = entries,
            .data_buffer = data_buffer,
            .row_heads = row_heads,
            .allocator = allocator,
        };
        log.assertInvariants();
        return log;
    }

    pub fn deinit(self: *UndoLog) void {
        self.row_heads.deinit();
        self.allocator.free(self.data_buffer);
        self.allocator.free(self.entries);
        self.* = undefined;
    }

    pub fn push(
        self: *UndoLog,
        tx_id: TxId,
        page_id: u64,
        slot: u16,
        old_data: []const u8,
    ) error{UndoLogFull}!u64 {
        std.debug.assert(tx_id > 0);
        std.debug.assert(old_data.len > 0);
        std.debug.assert(old_data.len <= max_row_size_bytes);
        std.debug.assert(old_data.len <= std.math.maxInt(u16));
        self.assertInvariants();

        if (self.count >= self.entries.len) return error.UndoLogFull;

        const data_offset = try self.reserveData(@intCast(old_data.len));
        const key = RowKey{ .page_id = page_id, .slot = slot };
        const prev = self.getHead(page_id, slot);
        const logical_idx = self.base_index + self.count;

        const physical = self.head;
        self.entries[physical] = .{
            .tx_id = tx_id,
            .page_id = page_id,
            .slot = slot,
            .data_offset = data_offset,
            .data_length = @intCast(old_data.len),
            .prev = prev,
        };

        const dst_start: usize = @intCast(data_offset);
        const dst_end = dst_start + old_data.len;
        @memcpy(self.data_buffer[dst_start..dst_end], old_data);

        if (self.count == 0) {
            self.data_tail = data_offset;
        }

        self.head = self.nextEntryPos(self.head);
        self.count += 1;

        self.row_heads.put(key, logical_idx) catch {
            @panic("row_heads capacity exceeded");
        };
        self.assertInvariants();
        return logical_idx;
    }

    pub fn get(self: *const UndoLog, idx: u64) ?*const UndoEntry {
        const physical = self.logicalToPhysical(idx) orelse return null;
        return &self.entries[physical];
    }

    pub fn getHead(self: *const UndoLog, page_id: u64, slot: u16) ?u64 {
        const key = RowKey{ .page_id = page_id, .slot = slot };
        const idx = self.row_heads.get(key) orelse return null;
        if (idx < self.base_index) return null;
        if (idx >= self.base_index + self.count) return null;
        return idx;
    }

    pub fn findVisible(
        self: *const UndoLog,
        page_id: u64,
        slot: u16,
        snap: *const tx_mod.Snapshot,
        tm: *const tx_mod.TxManager,
    ) ?[]const u8 {
        var idx_opt = self.getHead(page_id, slot);
        var result: ?[]const u8 = null;
        var depth: u16 = 0;

        while (idx_opt) |idx| {
            depth += 1;
            if (depth > max_version_chain_depth) {
                @panic("undo version chain exceeded max_version_chain_depth");
            }

            const entry = self.get(idx) orelse break;
            const writer_state = tm.getState(entry.tx_id) orelse {
                @panic("undo chain references unknown transaction state");
            };

            if (snap.isVisible(entry.tx_id, writer_state)) {
                return result;
            }

            result = self.entryData(entry);
            idx_opt = entry.prev;
        }

        return result;
    }

    pub fn truncate(self: *UndoLog, oldest_needed_tx_id: TxId) void {
        self.assertInvariants();
        while (self.count > 0) {
            const entry = &self.entries[self.tail];
            if (entry.tx_id >= oldest_needed_tx_id) break;

            std.debug.assert(self.data_used >= entry.data_length);
            self.data_used -= entry.data_length;
            self.tail = self.nextEntryPos(self.tail);
            self.count -= 1;
            self.base_index += 1;

            if (self.count == 0) {
                self.head = 0;
                self.tail = 0;
                self.data_head = 0;
                self.data_tail = 0;
                self.data_used = 0;
            } else {
                self.data_tail = self.entries[self.tail].data_offset;
            }
        }

        self.assertInvariants();
    }

    pub fn len(self: *const UndoLog) u32 {
        return self.count;
    }

    fn reserveData(self: *UndoLog, length: u16) error{UndoLogFull}!u32 {
        std.debug.assert(length > 0);
        self.assertInvariants();
        const len32: u32 = length;
        const cap: u32 = @intCast(self.data_buffer.len);

        if (len32 > cap) return error.UndoLogFull;
        if (self.data_used + len32 > cap) return error.UndoLogFull;

        if (self.data_head >= self.data_tail) {
            const space_to_end = cap - self.data_head;
            if (len32 <= space_to_end) {
                const offset = self.data_head;
                self.data_head += len32;
                if (self.data_head == cap) self.data_head = 0;
                self.data_used += len32;
                return offset;
            }

            if (len32 > self.data_tail) return error.UndoLogFull;
            self.data_head = 0;
        } else {
            const between = self.data_tail - self.data_head;
            if (len32 > between) return error.UndoLogFull;
        }

        const offset = self.data_head;
        self.data_head += len32;
        self.data_used += len32;
        return offset;
    }

    fn nextEntryPos(self: *const UndoLog, pos: u32) u32 {
        const cap: u32 = @intCast(self.entries.len);
        std.debug.assert(cap > 0);
        std.debug.assert(pos < cap);
        const next = pos + 1;
        if (next == cap) return 0;
        return next;
    }

    fn logicalToPhysical(self: *const UndoLog, idx: u64) ?u32 {
        self.assertInvariants();
        if (idx < self.base_index) return null;

        const relative = idx - self.base_index;
        if (relative >= self.count) return null;

        const cap: u32 = @intCast(self.entries.len);
        const rel32: u32 = @intCast(relative);
        return (self.tail + rel32) % cap;
    }

    fn entryData(self: *const UndoLog, entry: *const UndoEntry) []const u8 {
        const start: usize = @intCast(entry.data_offset);
        const data_len: usize = entry.data_length;
        std.debug.assert(start + data_len <= self.data_buffer.len);
        return self.data_buffer[start .. start + data_len];
    }

    fn assertInvariants(self: *const UndoLog) void {
        std.debug.assert(self.entries.len > 0);
        std.debug.assert(self.data_buffer.len > 0);
        std.debug.assert(self.count <= self.entries.len);
        std.debug.assert(self.data_used <= self.data_buffer.len);

        const entries_cap: u32 = @intCast(self.entries.len);
        std.debug.assert(self.head < entries_cap);
        std.debug.assert(self.tail < entries_cap);

        const data_cap: u32 = @intCast(self.data_buffer.len);
        std.debug.assert(self.data_head < data_cap);
        std.debug.assert(self.data_tail < data_cap);
    }
};

// --- Tests ---

test "push and get undo entry" {
    var log = try UndoLog.init(std.testing.allocator, 16, 4096);
    defer log.deinit();

    const idx = try log.push(1, 0, 0, "old value");
    const entry = log.get(idx).?;
    try std.testing.expectEqual(@as(TxId, 1), entry.tx_id);
    try std.testing.expectEqualSlices(u8, "old value", log.entryData(entry));
    try std.testing.expect(entry.prev == null);
}

test "version chain links entries" {
    var log = try UndoLog.init(std.testing.allocator, 16, 4096);
    defer log.deinit();

    const idx0 = try log.push(1, 0, 0, "version 1");
    const idx1 = try log.push(2, 0, 0, "version 2");

    const entry1 = log.get(idx1).?;
    try std.testing.expectEqual(@as(?u64, idx0), entry1.prev);

    const entry0 = log.get(idx0).?;
    try std.testing.expect(entry0.prev == null);
}

test "getHead returns latest entry" {
    var log = try UndoLog.init(std.testing.allocator, 16, 4096);
    defer log.deinit();

    _ = try log.push(1, 0, 0, "v1");
    const idx1 = try log.push(2, 0, 0, "v2");

    try std.testing.expectEqual(@as(?u64, idx1), log.getHead(0, 0));
}

test "different rows have independent chains" {
    var log = try UndoLog.init(std.testing.allocator, 16, 4096);
    defer log.deinit();

    const a = try log.push(1, 0, 0, "row A");
    const b = try log.push(1, 0, 1, "row B");

    try std.testing.expectEqual(@as(?u64, a), log.getHead(0, 0));
    try std.testing.expectEqual(@as(?u64, b), log.getHead(0, 1));
}

test "findVisible returns correct version" {
    var tm = tx_mod.TxManager.init(std.testing.allocator);
    defer tm.deinit();
    var log = try UndoLog.init(std.testing.allocator, 64, 4096);
    defer log.deinit();

    const t1 = try tm.begin();
    try tm.commit(t1);

    const t2 = try tm.begin();
    var snap = try tm.snapshot(t2);
    defer snap.deinit();

    const t3 = try tm.begin();
    _ = try log.push(t3, 0, 0, "original");
    try tm.commit(t3);

    const visible = log.findVisible(0, 0, &snap, &tm);
    try std.testing.expect(visible != null);
    try std.testing.expectEqualSlices(u8, "original", visible.?);
}

test "findVisible returns null when current version is visible" {
    var tm = tx_mod.TxManager.init(std.testing.allocator);
    defer tm.deinit();
    var log = try UndoLog.init(std.testing.allocator, 64, 4096);
    defer log.deinit();

    const t1 = try tm.begin();
    _ = try log.push(t1, 0, 0, "before t1");
    try tm.commit(t1);

    const t2 = try tm.begin();
    var snap = try tm.snapshot(t2);
    defer snap.deinit();

    const visible = log.findVisible(0, 0, &snap, &tm);
    try std.testing.expect(visible == null);
}

test "findVisible walks chain for deep history" {
    var tm = tx_mod.TxManager.init(std.testing.allocator);
    defer tm.deinit();
    var log = try UndoLog.init(std.testing.allocator, 64, 4096);
    defer log.deinit();

    const t1 = try tm.begin();
    try tm.commit(t1);

    const t2 = try tm.begin();
    var snap = try tm.snapshot(t2);
    defer snap.deinit();

    const t3 = try tm.begin();
    _ = try log.push(t3, 0, 0, "v1");
    try tm.commit(t3);

    const t4 = try tm.begin();
    _ = try log.push(t4, 0, 0, "v2");
    try tm.commit(t4);

    const visible = log.findVisible(0, 0, &snap, &tm);
    try std.testing.expect(visible != null);
    try std.testing.expectEqualSlices(u8, "v1", visible.?);
}

test "findVisible skips aborted head and returns latest committed version" {
    var tm = tx_mod.TxManager.init(std.testing.allocator);
    defer tm.deinit();
    var log = try UndoLog.init(std.testing.allocator, 64, 4096);
    defer log.deinit();

    const t1 = try tm.begin();
    _ = try log.push(t1, 0, 0, "v1");
    try tm.commit(t1);

    const t2 = try tm.begin();
    _ = try log.push(t2, 0, 0, "v2");
    try tm.abort(t2);

    const t3 = try tm.begin();
    var snap = try tm.snapshot(t3);
    defer snap.deinit();

    const visible = log.findVisible(0, 0, &snap, &tm);
    try std.testing.expect(visible != null);
    try std.testing.expectEqualSlices(u8, "v2", visible.?);
}

test "findVisible skips active head and returns latest committed version" {
    var tm = tx_mod.TxManager.init(std.testing.allocator);
    defer tm.deinit();
    var log = try UndoLog.init(std.testing.allocator, 64, 4096);
    defer log.deinit();

    const t1 = try tm.begin();
    _ = try log.push(t1, 0, 0, "v1");
    try tm.commit(t1);

    const t2 = try tm.begin();
    _ = try log.push(t2, 0, 0, "v2");

    const t3 = try tm.begin();
    var snap = try tm.snapshot(t3);
    defer snap.deinit();

    const visible = log.findVisible(0, 0, &snap, &tm);
    try std.testing.expect(visible != null);
    try std.testing.expectEqualSlices(u8, "v2", visible.?);
}

test "push returns UndoLogFull when entry ring fills" {
    var log = try UndoLog.init(std.testing.allocator, 2, 128);
    defer log.deinit();

    _ = try log.push(1, 0, 0, "a");
    _ = try log.push(2, 0, 1, "b");
    try std.testing.expectError(error.UndoLogFull, log.push(3, 0, 2, "c"));
}

test "truncate frees capacity for new pushes" {
    var log = try UndoLog.init(std.testing.allocator, 2, 128);
    defer log.deinit();

    _ = try log.push(1, 0, 0, "a");
    _ = try log.push(2, 0, 1, "b");
    try std.testing.expectEqual(@as(u32, 2), log.len());

    log.truncate(2);
    try std.testing.expectEqual(@as(u32, 1), log.len());

    _ = try log.push(3, 0, 2, "c");
    try std.testing.expectEqual(@as(u32, 2), log.len());
}

test "stale row head returns null after truncation" {
    var log = try UndoLog.init(std.testing.allocator, 4, 128);
    defer log.deinit();

    _ = try log.push(1, 10, 2, "x");
    log.truncate(2);

    try std.testing.expect(log.getHead(10, 2) == null);
}
