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

pub const slot_reclaim_queue_capacity: usize = 4096;

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
