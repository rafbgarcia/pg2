//! Transaction-aware queues for deferred storage reclamation work.
//!
//! Responsibilities:
//! - Tracks heap slots that became tombstoned inside transactions.
//! - Preserves commit/abort semantics (pending vs committed queue entries).
//! - Exposes deterministic FIFO dequeue gated by `oldest_active`.
const std = @import("std");

pub const SlotReclaimError = error{
    InvalidEntry,
    QueueFull,
    QueueEmpty,
    DuplicateEntry,
};

pub const IndexReclaimError = error{
    InvalidEntry,
    QueueFull,
    QueueEmpty,
    DuplicateEntry,
    KeyTooLarge,
};

pub const slot_reclaim_queue_capacity: usize = 4096;
pub const index_reclaim_queue_capacity: usize = 4096;
pub const index_reclaim_key_bytes_capacity: usize = 512 * 1024;

pub const SlotReclaimEntryState = enum(u8) {
    pending,
    committed,
};

pub const SlotReclaimEntry = struct {
    page_id: u64,
    slot: u16,
    deleting_tx_id: u64,
    state: SlotReclaimEntryState,
};

pub const SlotReclaimQueue = struct {
    entries: [slot_reclaim_queue_capacity]SlotReclaimEntry = [_]SlotReclaimEntry{
        .{
            .page_id = 0,
            .slot = 0,
            .deleting_tx_id = 0,
            .state = .pending,
        },
    } ** slot_reclaim_queue_capacity,
    head: usize = 0,
    tail: usize = 0,
    len: usize = 0,

    pub fn enqueue(self: *SlotReclaimQueue, deleting_tx_id: u64, page_id: u64, slot: u16) SlotReclaimError!void {
        if (deleting_tx_id == 0 or page_id == 0) return error.InvalidEntry;
        if (self.contains(page_id, slot)) return error.DuplicateEntry;
        if (self.len >= slot_reclaim_queue_capacity) return error.QueueFull;
        self.entries[self.tail] = .{
            .page_id = page_id,
            .slot = slot,
            .deleting_tx_id = deleting_tx_id,
            .state = .pending,
        };
        self.tail = (self.tail + 1) % slot_reclaim_queue_capacity;
        self.len += 1;
    }

    pub fn commitTx(self: *SlotReclaimQueue, tx_id: u64) void {
        if (tx_id == 0 or self.len == 0) return;
        var idx = self.head;
        var remaining = self.len;
        while (remaining > 0) : (remaining -= 1) {
            if (self.entries[idx].deleting_tx_id == tx_id and
                self.entries[idx].state == .pending)
            {
                self.entries[idx].state = .committed;
            }
            idx = (idx + 1) % slot_reclaim_queue_capacity;
        }
    }

    pub fn abortTx(self: *SlotReclaimQueue, tx_id: u64) void {
        if (tx_id == 0 or self.len == 0) return;
        var new_entries: [slot_reclaim_queue_capacity]SlotReclaimEntry = [_]SlotReclaimEntry{
            .{
                .page_id = 0,
                .slot = 0,
                .deleting_tx_id = 0,
                .state = .pending,
            },
        } ** slot_reclaim_queue_capacity;
        var new_len: usize = 0;
        var idx = self.head;
        var remaining = self.len;
        while (remaining > 0) : (remaining -= 1) {
            const entry = self.entries[idx];
            if (!(entry.deleting_tx_id == tx_id and entry.state == .pending)) {
                new_entries[new_len] = entry;
                new_len += 1;
            }
            idx = (idx + 1) % slot_reclaim_queue_capacity;
        }
        self.entries = new_entries;
        self.head = 0;
        self.len = new_len;
        self.tail = new_len % slot_reclaim_queue_capacity;
    }

    pub fn dequeueReclaimable(self: *SlotReclaimQueue, oldest_active: u64) SlotReclaimError!?SlotReclaimEntry {
        if (self.len == 0) return error.QueueEmpty;
        const entry = self.entries[self.head];
        if (entry.state != .committed) return null;
        if (entry.deleting_tx_id >= oldest_active) return null;
        self.entries[self.head] = .{
            .page_id = 0,
            .slot = 0,
            .deleting_tx_id = 0,
            .state = .pending,
        };
        self.head = (self.head + 1) % slot_reclaim_queue_capacity;
        self.len -= 1;
        return entry;
    }

    pub fn isEmpty(self: *const SlotReclaimQueue) bool {
        return self.len == 0;
    }

    pub fn contains(self: *const SlotReclaimQueue, page_id: u64, slot: u16) bool {
        var idx = self.head;
        var remaining = self.len;
        while (remaining > 0) : (remaining -= 1) {
            const entry = self.entries[idx];
            if (entry.page_id == page_id and entry.slot == slot) return true;
            idx = (idx + 1) % slot_reclaim_queue_capacity;
        }
        return false;
    }
};

pub const IndexReclaimEntryState = enum(u8) {
    pending,
    committed,
};

pub const IndexReclaimEntry = struct {
    model_id: u16,
    index_id: u16,
    page_id: u64,
    slot: u16,
    deleting_tx_id: u64,
    key_offset: u32,
    key_len: u16,
    state: IndexReclaimEntryState,
};

pub const IndexReclaimQueue = struct {
    entries: [index_reclaim_queue_capacity]IndexReclaimEntry = [_]IndexReclaimEntry{
        .{
            .model_id = 0,
            .index_id = 0,
            .page_id = 0,
            .slot = 0,
            .deleting_tx_id = 0,
            .key_offset = 0,
            .key_len = 0,
            .state = .pending,
        },
    } ** index_reclaim_queue_capacity,
    key_bytes: [index_reclaim_key_bytes_capacity]u8 = [_]u8{0} ** index_reclaim_key_bytes_capacity,
    key_bytes_len: usize = 0,
    head: usize = 0,
    tail: usize = 0,
    len: usize = 0,

    pub fn enqueue(
        self: *IndexReclaimQueue,
        deleting_tx_id: u64,
        model_id: u16,
        index_id: u16,
        page_id: u64,
        slot: u16,
        key: []const u8,
    ) IndexReclaimError!void {
        if (deleting_tx_id == 0 or page_id == 0) return error.InvalidEntry;
        if (key.len == 0 or key.len > std.math.maxInt(u16)) return error.KeyTooLarge;
        if (self.contains(page_id, slot, index_id, key)) return error.DuplicateEntry;
        if (self.len >= index_reclaim_queue_capacity) return error.QueueFull;
        if (self.key_bytes_len + key.len > self.key_bytes.len) {
            self.compact();
            if (self.key_bytes_len + key.len > self.key_bytes.len) return error.QueueFull;
        }

        const key_offset: u32 = @intCast(self.key_bytes_len);
        @memcpy(self.key_bytes[self.key_bytes_len .. self.key_bytes_len + key.len], key);
        self.key_bytes_len += key.len;

        const entry: IndexReclaimEntry = .{
            .model_id = model_id,
            .index_id = index_id,
            .page_id = page_id,
            .slot = slot,
            .deleting_tx_id = deleting_tx_id,
            .key_offset = key_offset,
            .key_len = @intCast(key.len),
            .state = .pending,
        };
        self.entries[self.tail] = entry;
        self.tail = (self.tail + 1) % index_reclaim_queue_capacity;
        self.len += 1;
    }

    pub fn commitTx(self: *IndexReclaimQueue, tx_id: u64) void {
        if (tx_id == 0 or self.len == 0) return;
        var idx = self.head;
        var remaining = self.len;
        while (remaining > 0) : (remaining -= 1) {
            if (self.entries[idx].deleting_tx_id == tx_id and
                self.entries[idx].state == .pending)
            {
                self.entries[idx].state = .committed;
            }
            idx = (idx + 1) % index_reclaim_queue_capacity;
        }
    }

    pub fn abortTx(self: *IndexReclaimQueue, tx_id: u64) void {
        if (tx_id == 0 or self.len == 0) return;
        var new_entries: [index_reclaim_queue_capacity]IndexReclaimEntry = [_]IndexReclaimEntry{
            .{
                .model_id = 0,
                .index_id = 0,
                .page_id = 0,
                .slot = 0,
                .deleting_tx_id = 0,
                .key_offset = 0,
                .key_len = 0,
                .state = .pending,
            },
        } ** index_reclaim_queue_capacity;
        var new_key_bytes: [index_reclaim_key_bytes_capacity]u8 = [_]u8{0} ** index_reclaim_key_bytes_capacity;
        var new_key_len: usize = 0;
        var new_len: usize = 0;
        var idx = self.head;
        var remaining = self.len;
        while (remaining > 0) : (remaining -= 1) {
            const entry = self.entries[idx];
            if (!(entry.deleting_tx_id == tx_id and entry.state == .pending)) {
                const key = self.keySlice(&entry);
                if (new_key_len + key.len > new_key_bytes.len) {
                    // Fail closed by dropping pending compaction work for this tx.
                    return;
                }
                @memcpy(new_key_bytes[new_key_len .. new_key_len + key.len], key);
                var new_entry = entry;
                new_entry.key_offset = @intCast(new_key_len);
                new_entries[new_len] = new_entry;
                new_key_len += key.len;
                new_len += 1;
            }
            idx = (idx + 1) % index_reclaim_queue_capacity;
        }
        self.entries = new_entries;
        self.key_bytes = new_key_bytes;
        self.key_bytes_len = new_key_len;
        self.head = 0;
        self.len = new_len;
        self.tail = new_len % index_reclaim_queue_capacity;
    }

    pub fn dequeueReclaimableForRow(
        self: *IndexReclaimQueue,
        oldest_active: u64,
        page_id: u64,
        slot: u16,
    ) IndexReclaimError!?IndexReclaimEntry {
        if (self.len == 0) return error.QueueEmpty;
        const entry = self.entries[self.head];
        if (entry.state != .committed) return null;
        if (entry.deleting_tx_id >= oldest_active) return null;
        if (entry.page_id != page_id or entry.slot != slot) return null;
        const out = entry;
        self.head = (self.head + 1) % index_reclaim_queue_capacity;
        self.len -= 1;
        return out;
    }

    pub fn isEmpty(self: *const IndexReclaimQueue) bool {
        return self.len == 0;
    }

    pub fn contains(
        self: *const IndexReclaimQueue,
        page_id: u64,
        slot: u16,
        index_id: u16,
        key: []const u8,
    ) bool {
        var idx = self.head;
        var remaining = self.len;
        while (remaining > 0) : (remaining -= 1) {
            const entry = self.entries[idx];
            if (entry.page_id == page_id and
                entry.slot == slot and
                entry.index_id == index_id and
                std.mem.eql(u8, self.keySlice(&entry), key))
            {
                return true;
            }
            idx = (idx + 1) % index_reclaim_queue_capacity;
        }
        return false;
    }

    pub fn keySlice(self: *const IndexReclaimQueue, entry: *const IndexReclaimEntry) []const u8 {
        const start: usize = @intCast(entry.key_offset);
        const end = start + entry.key_len;
        if (start > self.key_bytes_len or end > self.key_bytes_len) return self.key_bytes[0..0];
        return self.key_bytes[start..end];
    }

    fn compact(self: *IndexReclaimQueue) void {
        var new_entries: [index_reclaim_queue_capacity]IndexReclaimEntry = [_]IndexReclaimEntry{
            .{
                .model_id = 0,
                .index_id = 0,
                .page_id = 0,
                .slot = 0,
                .deleting_tx_id = 0,
                .key_offset = 0,
                .key_len = 0,
                .state = .pending,
            },
        } ** index_reclaim_queue_capacity;
        var new_key_bytes: [index_reclaim_key_bytes_capacity]u8 = [_]u8{0} ** index_reclaim_key_bytes_capacity;
        var new_key_len: usize = 0;
        var new_len: usize = 0;
        var idx = self.head;
        var remaining = self.len;
        while (remaining > 0) : (remaining -= 1) {
            const entry = self.entries[idx];
            const key = self.keySlice(&entry);
            if (new_key_len + key.len > new_key_bytes.len) {
                // Fail closed by resetting to an empty queue.
                self.entries = [_]IndexReclaimEntry{
                    .{
                        .model_id = 0,
                        .index_id = 0,
                        .page_id = 0,
                        .slot = 0,
                        .deleting_tx_id = 0,
                        .key_offset = 0,
                        .key_len = 0,
                        .state = .pending,
                    },
                } ** index_reclaim_queue_capacity;
                self.key_bytes = [_]u8{0} ** index_reclaim_key_bytes_capacity;
                self.key_bytes_len = 0;
                self.head = 0;
                self.tail = 0;
                self.len = 0;
                return;
            }
            @memcpy(new_key_bytes[new_key_len .. new_key_len + key.len], key);
            var new_entry = entry;
            new_entry.key_offset = @intCast(new_key_len);
            new_entries[new_len] = new_entry;
            new_key_len += key.len;
            new_len += 1;
            idx = (idx + 1) % index_reclaim_queue_capacity;
        }
        self.entries = new_entries;
        self.key_bytes = new_key_bytes;
        self.key_bytes_len = new_key_len;
        self.head = 0;
        self.len = new_len;
        self.tail = new_len % index_reclaim_queue_capacity;
    }
};

test "slot reclaim queue commit+dequeue requires oldest_active advance" {
    var queue: SlotReclaimQueue = .{};
    try queue.enqueue(10, 100, 2);
    queue.commitTx(10);
    try std.testing.expect((try queue.dequeueReclaimable(10)) == null);
    const entry = (try queue.dequeueReclaimable(11)).?;
    try std.testing.expectEqual(@as(u64, 100), entry.page_id);
    try std.testing.expectEqual(@as(u16, 2), entry.slot);
    try std.testing.expect(queue.isEmpty());
}

test "slot reclaim queue abort removes only pending for tx" {
    var queue: SlotReclaimQueue = .{};
    try queue.enqueue(7, 200, 1);
    try queue.enqueue(8, 201, 2);
    queue.commitTx(8);
    queue.abortTx(7);
    try std.testing.expectEqual(@as(usize, 1), queue.len);
    const entry = (try queue.dequeueReclaimable(9)).?;
    try std.testing.expectEqual(@as(u64, 201), entry.page_id);
}

test "index reclaim queue dequeue is gated by tx visibility and row identity" {
    var queue: IndexReclaimQueue = .{};
    try queue.enqueue(4, 1, 2, 100, 3, "abc");
    queue.commitTx(4);
    try std.testing.expect((try queue.dequeueReclaimableForRow(4, 100, 3)) == null);
    try std.testing.expect((try queue.dequeueReclaimableForRow(5, 101, 3)) == null);
    const entry = (try queue.dequeueReclaimableForRow(5, 100, 3)).?;
    try std.testing.expectEqual(@as(u16, 1), entry.model_id);
    try std.testing.expectEqual(@as(u16, 2), entry.index_id);
    try std.testing.expectEqualStrings("abc", queue.keySlice(&entry));
    try std.testing.expect(queue.isEmpty());
}
