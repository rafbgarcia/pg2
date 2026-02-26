//! Asynchronous advisor metric sink.
//!
//! Critical-path contract:
//! - Statement path may only enqueue into a bounded in-memory queue.
//! - Queue overflow drops advisor metrics (never blocks query execution).
//! - Background thread performs file I/O and batching.
const std = @import("std");
const metrics_mod = @import("metrics.zig");

pub const MetricRecord = metrics_mod.MetricRecord;
const MetricsFileWriter = metrics_mod.MetricsFileWriter;

pub const queue_capacity: usize = 4096;
const batch_capacity: usize = 128;
const idle_sleep_ns: u64 = 50 * std.time.ns_per_ms;

pub const Sink = struct {
    mutex: std.Thread.Mutex = .{},
    queue: [queue_capacity]MetricRecord = undefined,
    head: usize = 0,
    tail: usize = 0,
    count: usize = 0,
    dropped_total: u64 = 0,
    shutdown_requested: bool = false,
    thread: ?std.Thread = null,
    writer: ?MetricsFileWriter = null,

    pub fn init() Sink {
        return .{};
    }

    pub fn start(self: *Sink, root_dir: *std.fs.Dir) !void {
        std.debug.assert(self.thread == null);
        self.writer = try MetricsFileWriter.open(root_dir);
        self.thread = try std.Thread.spawn(.{}, writerMain, .{self});
    }

    pub fn deinit(self: *Sink) void {
        if (self.thread) |thread| {
            self.mutex.lock();
            self.shutdown_requested = true;
            self.mutex.unlock();
            thread.join();
            self.thread = null;
        }
        if (self.writer) |*writer| {
            writer.deinit();
            self.writer = null;
        }
    }

    /// Non-blocking enqueue for request/statement path.
    /// Returns false when queue is full and the metric was dropped.
    pub fn enqueue(self: *Sink, record: *const MetricRecord) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.count == queue_capacity) {
            self.dropped_total += 1;
            return false;
        }

        self.queue[self.tail] = record.*;
        self.tail = (self.tail + 1) % queue_capacity;
        self.count += 1;
        return true;
    }

    pub fn droppedTotal(self: *Sink) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.dropped_total;
    }

    fn writerMain(self: *Sink) void {
        var batch: [batch_capacity]MetricRecord = undefined;

        while (true) {
            const drained = self.dequeueBatch(&batch);
            if (drained > 0) {
                if (self.writer) |*writer| {
                    writer.appendBatch(batch[0..drained]) catch |err| {
                        std.log.warn("advisor sink flush failed: {s}", .{@errorName(err)});
                        std.Thread.sleep(idle_sleep_ns);
                    };
                }
                continue;
            }

            if (self.shouldStop()) break;
            std.Thread.sleep(idle_sleep_ns);
        }

        while (true) {
            const drained = self.dequeueBatch(&batch);
            if (drained == 0) break;
            if (self.writer) |*writer| {
                writer.appendBatch(batch[0..drained]) catch |err| {
                    std.log.warn("advisor sink final flush failed: {s}", .{@errorName(err)});
                    break;
                };
            }
        }
    }

    fn dequeueBatch(self: *Sink, out: *[batch_capacity]MetricRecord) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.count == 0) return 0;

        var drained: usize = 0;
        while (drained < batch_capacity and self.count > 0) : (drained += 1) {
            out[drained] = self.queue[self.head];
            self.head = (self.head + 1) % queue_capacity;
            self.count -= 1;
        }
        return drained;
    }

    fn shouldStop(self: *Sink) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.shutdown_requested and self.count == 0;
    }
};

test "sink flushes enqueued metrics asynchronously" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var sink = Sink.init();
    try sink.start(&tmp.dir);

    var i: u32 = 0;
    while (i < 32) : (i += 1) {
        const ok = sink.enqueue(&.{
            .timestamp_unix_ns = i,
            .operation_kind = .select,
            .rows_scanned = 100,
            .rows_matched = 10,
            .has_predicate_filter = true,
        });
        try std.testing.expect(ok);
    }

    std.Thread.sleep(200 * std.time.ns_per_ms);
    sink.deinit();

    const records = try metrics_mod.readAll(std.testing.allocator, &tmp.dir);
    defer std.testing.allocator.free(records);
    try std.testing.expect(records.len >= 32);
}

test "sink drops metrics when queue is full" {
    var sink = Sink.init();

    var i: usize = 0;
    while (i < queue_capacity) : (i += 1) {
        const ok = sink.enqueue(&.{});
        try std.testing.expect(ok);
    }

    const overflow_ok = sink.enqueue(&.{});
    try std.testing.expect(!overflow_ok);
    try std.testing.expectEqual(@as(u64, 1), sink.droppedTotal());
}
