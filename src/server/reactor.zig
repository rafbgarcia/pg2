//! Bounded server reactor for multiplexed connection progress.
const std = @import("std");
const io_mod = @import("../storage/io.zig");
const session_mod = @import("session.zig");
const transport_mod = @import("transport.zig");

const Acceptor = transport_mod.Acceptor;
const Connection = transport_mod.Connection;
const Clock = io_mod.Clock;

pub const Dispatcher = struct {
    pub const DispatchResult = struct {
        response_len: usize,
        pin_transition: session_mod.PinTransition = .none,
    };

    ctx: *anyopaque,
    dispatch: *const fn (
        ctx: *anyopaque,
        session_id: u16,
        request: []const u8,
        out: []u8,
    ) session_mod.SessionError!DispatchResult,
    cleanupSession: *const fn (ctx: *anyopaque, session_id: u16) void,
};

pub fn ServerReactor(
    comptime max_sessions: usize,
    comptime request_buf_bytes: usize,
    comptime response_buf_bytes: usize,
) type {
    return struct {
        const Self = @This();
        const max_queue_capacity = max_sessions;

        pub const Config = struct {
            clock: Clock,
            queue_timeout_ticks: u64 = 30_000_000_000,
            max_queued_requests: usize = max_sessions,
            max_inflight: usize = 1,
        };

        const QueueState = enum(u8) {
            none,
            ready,
            dispatch,
        };

        const SessionSlot = struct {
            in_use: bool = false,
            connection: Connection = undefined,
            request_buf: [request_buf_bytes]u8 = undefined,
            request_len: usize = 0,
            has_request: bool = false,
            response_buf: [response_buf_bytes]u8 = undefined,
            response_len: usize = 0,
            has_response: bool = false,
            queue_state: QueueState = .none,
            queue_generation: u32 = 0,
            enqueue_tick: u64 = 0,
            pin_active: bool = false,
            pin_started_tick: u64 = 0,
        };

        const QueueEntry = struct {
            session_id: u16,
        };

        const TimeoutEntry = struct {
            session_id: u16,
            deadline: u64,
            generation: u32,
            sequence: u64,
        };

        const WorkerResult = union(enum) {
            none,
            ok: Dispatcher.DispatchResult,
            err: session_mod.SessionError,
        };

        const WorkerSlot = struct {
            thread: ?std.Thread = null,
            mutex: std.Thread.Mutex = .{},
            cond: std.Thread.Condition = .{},
            stop: bool = false,
            has_job: bool = false,
            running: bool = false,
            job_session_id: u16 = 0,
            job_request_len: usize = 0,
            result_session_id: u16 = 0,
            result: WorkerResult = .none,
            request_buf: [request_buf_bytes]u8 = undefined,
            response_buf: [response_buf_bytes]u8 = undefined,
        };

        const RingQueue = struct {
            buf: [max_queue_capacity]QueueEntry = undefined,
            head: usize = 0,
            len: usize = 0,

            fn isEmpty(self: *const RingQueue) bool {
                return self.len == 0;
            }

            fn push(self: *RingQueue, value: QueueEntry) bool {
                if (self.len >= max_queue_capacity) return false;
                const tail = (self.head + self.len) % max_queue_capacity;
                self.buf[tail] = value;
                self.len += 1;
                return true;
            }

            fn pop(self: *RingQueue) ?QueueEntry {
                if (self.isEmpty()) return null;
                const value = self.buf[self.head];
                self.head = (self.head + 1) % max_queue_capacity;
                self.len -= 1;
                return value;
            }
        };

        const TimeoutHeap = struct {
            buf: [max_queue_capacity]TimeoutEntry = undefined,
            len: usize = 0,

            fn peek(self: *const TimeoutHeap) ?TimeoutEntry {
                if (self.len == 0) return null;
                return self.buf[0];
            }

            fn push(self: *TimeoutHeap, value: TimeoutEntry) bool {
                if (self.len >= max_queue_capacity) return false;
                var i = self.len;
                self.buf[i] = value;
                self.len += 1;
                while (i > 0) {
                    const parent = (i - 1) / 2;
                    if (!timeoutLessThan(self.buf[i], self.buf[parent])) break;
                    const tmp = self.buf[parent];
                    self.buf[parent] = self.buf[i];
                    self.buf[i] = tmp;
                    i = parent;
                }
                return true;
            }

            fn pop(self: *TimeoutHeap) ?TimeoutEntry {
                if (self.len == 0) return null;
                const result = self.buf[0];
                self.len -= 1;
                if (self.len > 0) {
                    self.buf[0] = self.buf[self.len];
                    var i: usize = 0;
                    while (true) {
                        const left = (i * 2) + 1;
                        if (left >= self.len) break;
                        const right = left + 1;
                        var best = left;
                        if (right < self.len and timeoutLessThan(self.buf[right], self.buf[left])) {
                            best = right;
                        }
                        if (!timeoutLessThan(self.buf[best], self.buf[i])) break;
                        const tmp = self.buf[i];
                        self.buf[i] = self.buf[best];
                        self.buf[best] = tmp;
                        i = best;
                    }
                }
                return result;
            }

            fn timeoutLessThan(a: TimeoutEntry, b: TimeoutEntry) bool {
                if (a.deadline < b.deadline) return true;
                if (a.deadline > b.deadline) return false;
                return a.sequence < b.sequence;
            }
        };

        pub const ReactorError = session_mod.SessionError ||
            transport_mod.AcceptError ||
            transport_mod.ConnectionError ||
            error{WorkerStartFailed};

        pub const Stats = struct {
            queue_depth: usize,
            workers_busy: usize,
            pool_pinned: usize,
            requests_enqueued_total: u64,
            requests_dispatched_total: u64,
            requests_completed_total: u64,
            queue_full_total: u64,
            queue_timeout_total: u64,
            max_queue_wait_ticks: u64,
            max_pin_wait_ticks: u64,
            max_pin_duration_ticks: u64,
        };

        dispatcher: Dispatcher,
        clock: Clock,
        queue_timeout_ticks: u64,
        max_queued_requests: usize,
        max_inflight: usize,
        sessions: [max_sessions]SessionSlot = [_]SessionSlot{.{}} ** max_sessions,
        workers: [max_sessions]WorkerSlot = [_]WorkerSlot{.{}} ** max_sessions,
        ready_queue: RingQueue = .{},
        dispatch_queue: RingQueue = .{},
        timeout_heap: TimeoutHeap = .{},
        timeout_sequence: u64 = 0,
        workers_busy: usize = 0,
        read_cursor: usize = 0,
        write_cursor: usize = 0,
        requests_enqueued_total: u64 = 0,
        requests_dispatched_total: u64 = 0,
        requests_completed_total: u64 = 0,
        queue_full_total: u64 = 0,
        queue_timeout_total: u64 = 0,
        max_queue_wait_ticks: u64 = 0,
        pool_pinned: usize = 0,
        max_pin_wait_ticks: u64 = 0,
        max_pin_duration_ticks: u64 = 0,

        pub fn init(dispatcher: Dispatcher, config: Config) Self {
            std.debug.assert(max_sessions > 0);
            std.debug.assert(max_sessions <= std.math.maxInt(u16));
            std.debug.assert(request_buf_bytes > 0);
            std.debug.assert(response_buf_bytes > 0);
            std.debug.assert(config.max_queued_requests > 0);
            std.debug.assert(config.max_queued_requests <= max_sessions);
            std.debug.assert(config.max_inflight > 0);
            std.debug.assert(config.max_inflight <= max_sessions);
            return .{
                .dispatcher = dispatcher,
                .clock = config.clock,
                .queue_timeout_ticks = config.queue_timeout_ticks,
                .max_queued_requests = config.max_queued_requests,
                .max_inflight = config.max_inflight,
            };
        }

        pub fn deinit(self: *Self) void {
            self.stopWorkers();
            var i: usize = 0;
            while (i < self.sessions.len) : (i += 1) {
                if (self.sessions[i].in_use) self.closeSlot(i);
            }
        }

        pub fn step(self: *Self, acceptor: Acceptor) ReactorError!void {
            try self.acceptPending(acceptor);
            try self.collectWorkerCompletion();
            try self.flushPendingWrites();
            try self.pollReads();
            self.expireTimedOutRequests(self.clock.now());
            self.promoteReadyToDispatch();
            try self.startDispatchAvailable();
            try self.collectWorkerCompletion();
            try self.flushPendingWrites();
        }

        pub fn activeSessions(self: *const Self) usize {
            var count: usize = 0;
            var i: usize = 0;
            while (i < self.sessions.len) : (i += 1) {
                if (self.sessions[i].in_use) count += 1;
            }
            return count;
        }

        pub fn stats(self: *const Self) Stats {
            return .{
                .queue_depth = self.ready_queue.len + self.dispatch_queue.len,
                .workers_busy = self.workers_busy,
                .pool_pinned = self.pool_pinned,
                .requests_enqueued_total = self.requests_enqueued_total,
                .requests_dispatched_total = self.requests_dispatched_total,
                .requests_completed_total = self.requests_completed_total,
                .queue_full_total = self.queue_full_total,
                .queue_timeout_total = self.queue_timeout_total,
                .max_queue_wait_ticks = self.max_queue_wait_ticks,
                .max_pin_wait_ticks = self.max_pin_wait_ticks,
                .max_pin_duration_ticks = self.max_pin_duration_ticks,
            };
        }

        fn acceptPending(self: *Self, acceptor: Acceptor) transport_mod.AcceptError!void {
            while (true) {
                const conn_opt = try acceptor.accept();
                const conn = conn_opt orelse return;
                if (!self.tryAddSession(conn)) {
                    conn.close();
                }
            }
        }

        fn tryAddSession(self: *Self, connection: Connection) bool {
            var i: usize = 0;
            while (i < self.sessions.len) : (i += 1) {
                if (self.sessions[i].in_use) continue;
                self.sessions[i] = .{
                    .in_use = true,
                    .connection = connection,
                };
                return true;
            }
            return false;
        }

        fn pollReads(self: *Self) transport_mod.ConnectionError!void {
            var visited: usize = 0;
            const now = self.clock.now();
            while (visited < self.sessions.len) : (visited += 1) {
                const i = (self.read_cursor + visited) % self.sessions.len;
                const slot = &self.sessions[i];
                if (!slot.in_use) continue;
                if (slot.has_request or slot.has_response) continue;

                const request_opt = slot.connection.readRequest(slot.request_buf[0..]) catch |err| switch (err) {
                    error.WouldBlock => continue,
                    else => {
                        self.closeSlot(i);
                        continue;
                    },
                };
                const request = request_opt orelse {
                    self.closeSlot(i);
                    continue;
                };
                slot.request_len = request.len;
                slot.has_request = true;
                self.admitToReadyQueue(i, now);
            }
            self.read_cursor = (self.read_cursor + 1) % self.sessions.len;
        }

        fn admitToReadyQueue(self: *Self, i: usize, now: u64) void {
            const slot = &self.sessions[i];
            std.debug.assert(slot.in_use);
            std.debug.assert(slot.has_request);
            if (slot.queue_state != .none) return;
            const sid: u16 = @intCast(i);

            if (self.ready_queue.len >= self.max_queued_requests or self.timeout_heap.len >= self.max_queued_requests) {
                self.queue_full_total += 1;
                self.respondOverload(i, "QueueFull");
                return;
            }

            slot.queue_generation +%= 1;
            slot.enqueue_tick = now;
            slot.queue_state = .ready;
            _ = self.ready_queue.push(.{ .session_id = sid });
            const deadline = std.math.add(u64, now, self.queue_timeout_ticks) catch std.math.maxInt(u64);
            _ = self.timeout_heap.push(.{
                .session_id = sid,
                .deadline = deadline,
                .generation = slot.queue_generation,
                .sequence = self.timeout_sequence,
            });
            self.timeout_sequence +%= 1;
            self.requests_enqueued_total += 1;
        }

        fn expireTimedOutRequests(self: *Self, now: u64) void {
            while (self.timeout_heap.peek()) |top| {
                if (top.deadline > now) return;
                const entry = self.timeout_heap.pop().?;
                const i: usize = @intCast(entry.session_id);
                const slot = &self.sessions[i];
                if (!slot.in_use) continue;
                if (!slot.has_request) continue;
                if (slot.queue_state != .ready) continue;
                if (slot.queue_generation != entry.generation) continue;

                self.queue_timeout_total += 1;
                self.respondOverload(i, "QueueTimeout");
            }
        }

        fn promoteReadyToDispatch(self: *Self) void {
            while (self.dispatch_queue.len + self.workers_busy < self.max_inflight) {
                const entry = self.ready_queue.pop() orelse return;
                const i: usize = @intCast(entry.session_id);
                const slot = &self.sessions[i];
                if (!slot.in_use) continue;
                if (!slot.has_request) continue;
                if (slot.queue_state != .ready) continue;
                slot.queue_state = .dispatch;
                _ = self.dispatch_queue.push(entry);
                return;
            }
        }

        fn startDispatchAvailable(self: *Self) ReactorError!void {
            while (self.workers_busy < self.max_inflight) {
                const worker_index = self.findIdleWorker() orelse return;
                const started = try self.startDispatchOne(worker_index);
                if (!started) return;
            }
        }

        fn startDispatchOne(self: *Self, worker_index: usize) ReactorError!bool {
            if (self.workers_busy >= self.max_inflight) return false;
            while (true) {
                const entry = self.dispatch_queue.pop() orelse return false;
                const i: usize = @intCast(entry.session_id);
                const slot = &self.sessions[i];
                if (!slot.in_use) continue;
                if (!slot.has_request or slot.has_response) continue;
                if (slot.queue_state != .dispatch) continue;

                try self.ensureWorkerStarted(worker_index);

                const worker = &self.workers[worker_index];
                worker.mutex.lock();
                if (worker.running or worker.has_job or worker.result != .none) {
                    worker.mutex.unlock();
                    continue;
                }

                std.debug.assert(slot.request_len <= worker.request_buf.len);
                @memcpy(
                    worker.request_buf[0..slot.request_len],
                    slot.request_buf[0..slot.request_len],
                );

                worker.job_session_id = @intCast(i);
                worker.job_request_len = slot.request_len;
                worker.has_job = true;
                worker.running = true;
                worker.cond.signal();
                worker.mutex.unlock();

                self.workers_busy += 1;
                self.requests_dispatched_total += 1;
                const wait_ticks = self.clock.now() - slot.enqueue_tick;
                if (wait_ticks > self.max_queue_wait_ticks) {
                    self.max_queue_wait_ticks = wait_ticks;
                }
                if (slot.pin_active and wait_ticks > self.max_pin_wait_ticks) {
                    self.max_pin_wait_ticks = wait_ticks;
                }
                return true;
            }
        }

        fn collectWorkerCompletion(self: *Self) ReactorError!void {
            if (self.workers_busy == 0) return;

            var worker_index: usize = 0;
            while (worker_index < self.max_inflight) : (worker_index += 1) {
                const worker = &self.workers[worker_index];
                worker.mutex.lock();
                const result = worker.result;
                const sid = worker.result_session_id;
                if (result == .none) {
                    worker.mutex.unlock();
                    continue;
                }
                worker.result = .none;
                worker.mutex.unlock();

                std.debug.assert(self.workers_busy > 0);
                self.workers_busy -= 1;

                const i: usize = @intCast(sid);
                const slot = &self.sessions[i];
                if (!slot.in_use or !slot.has_request or slot.queue_state != .dispatch) {
                    continue;
                }

                switch (result) {
                    .ok => |dispatch_result| {
                        const response_len = dispatch_result.response_len;
                        std.debug.assert(response_len <= slot.response_buf.len);
                        @memcpy(
                            slot.response_buf[0..response_len],
                            worker.response_buf[0..response_len],
                        );
                        const now = self.clock.now();
                        switch (dispatch_result.pin_transition) {
                            .none => {},
                            .began => {
                                if (!slot.pin_active) {
                                    slot.pin_active = true;
                                    slot.pin_started_tick = now;
                                    self.pool_pinned += 1;
                                }
                            },
                            .ended => {
                                if (slot.pin_active) {
                                    const duration = now - slot.pin_started_tick;
                                    if (duration > self.max_pin_duration_ticks) {
                                        self.max_pin_duration_ticks = duration;
                                    }
                                    slot.pin_active = false;
                                    slot.pin_started_tick = 0;
                                    std.debug.assert(self.pool_pinned > 0);
                                    self.pool_pinned -= 1;
                                }
                            },
                        }
                        slot.has_request = false;
                        slot.request_len = 0;
                        slot.has_response = true;
                        slot.response_len = response_len;
                        slot.queue_state = .none;
                        self.requests_completed_total += 1;
                    },
                    .err => |err| {
                        self.closeSlot(i);
                        return err;
                    },
                    .none => unreachable,
                }
            }
        }

        fn ensureWorkerStarted(self: *Self, worker_index: usize) error{WorkerStartFailed}!void {
            const worker = &self.workers[worker_index];
            if (worker.thread != null) return;
            worker.thread = std.Thread.spawn(
                .{},
                workerMain,
                .{ self, worker_index },
            ) catch return error.WorkerStartFailed;
        }

        fn stopWorkers(self: *Self) void {
            var i: usize = 0;
            while (i < self.max_inflight) : (i += 1) {
                const worker = &self.workers[i];
                if (worker.thread) |thread| {
                    worker.mutex.lock();
                    worker.stop = true;
                    worker.cond.signal();
                    worker.mutex.unlock();
                    thread.join();
                    worker.thread = null;
                }
            }
        }

        fn workerMain(self: *Self, worker_index: usize) void {
            const worker = &self.workers[worker_index];
            while (true) {
                worker.mutex.lock();
                while (!worker.has_job and !worker.stop) {
                    worker.cond.wait(&worker.mutex);
                }
                if (worker.stop and !worker.has_job) {
                    worker.mutex.unlock();
                    return;
                }

                const sid = worker.job_session_id;
                const request_len = worker.job_request_len;
                worker.has_job = false;
                worker.mutex.unlock();

                const result = self.dispatcher.dispatch(
                    self.dispatcher.ctx,
                    sid,
                    worker.request_buf[0..request_len],
                    worker.response_buf[0..],
                ) catch |err| {
                    worker.mutex.lock();
                    worker.result_session_id = sid;
                    worker.result = .{ .err = err };
                    worker.running = false;
                    worker.mutex.unlock();
                    continue;
                };

                worker.mutex.lock();
                worker.result_session_id = sid;
                worker.result = .{ .ok = result };
                worker.running = false;
                worker.mutex.unlock();
            }
        }

        fn findIdleWorker(self: *Self) ?usize {
            var worker_index: usize = 0;
            while (worker_index < self.max_inflight) : (worker_index += 1) {
                const worker = &self.workers[worker_index];
                worker.mutex.lock();
                const idle = !worker.running and !worker.has_job and worker.result == .none;
                worker.mutex.unlock();
                if (idle) return worker_index;
            }
            return null;
        }

        fn flushPendingWrites(self: *Self) transport_mod.ConnectionError!void {
            var visited: usize = 0;
            while (visited < self.sessions.len) : (visited += 1) {
                const i = (self.write_cursor + visited) % self.sessions.len;
                const slot = &self.sessions[i];
                if (!slot.in_use or !slot.has_response) continue;

                slot.connection.writeResponse(slot.response_buf[0..slot.response_len]) catch |err| switch (err) {
                    error.WouldBlock => continue,
                    else => {
                        self.closeSlot(i);
                        continue;
                    },
                };
                slot.has_response = false;
                slot.response_len = 0;
            }
            self.write_cursor = (self.write_cursor + 1) % self.sessions.len;
        }

        fn respondOverload(self: *Self, i: usize, code: []const u8) void {
            const slot = &self.sessions[i];
            if (!slot.in_use) return;
            const msg = std.fmt.bufPrint(
                slot.response_buf[0..],
                "ERR class=overload code={s}\n",
                .{code},
            ) catch {
                self.closeSlot(i);
                return;
            };
            slot.has_request = false;
            slot.request_len = 0;
            slot.has_response = true;
            slot.response_len = msg.len;
            slot.queue_state = .none;
        }

        fn closeSlot(self: *Self, i: usize) void {
            const slot = &self.sessions[i];
            if (!slot.in_use) return;
            if (slot.pin_active) {
                self.dispatcher.cleanupSession(
                    self.dispatcher.ctx,
                    @intCast(i),
                );
                const duration = self.clock.now() - slot.pin_started_tick;
                if (duration > self.max_pin_duration_ticks) {
                    self.max_pin_duration_ticks = duration;
                }
                slot.pin_active = false;
                slot.pin_started_tick = 0;
                std.debug.assert(self.pool_pinned > 0);
                self.pool_pinned -= 1;
            }
            slot.connection.close();
            slot.* = .{};
        }
    };
}

test "reactor tracks two simultaneous sessions and dispatches both requests" {
    const Reactor = ServerReactor(4, 256, 256);
    const response = "OK\n";
    const DispatchCtx = struct {
        calls: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        fn dispatch(
            ctx_ptr: *anyopaque,
            _: u16,
            _: []const u8,
            out: []u8,
        ) session_mod.SessionError!Dispatcher.DispatchResult {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            if (response.len > out.len) return error.ResponseTooLarge;
            @memcpy(out[0..response.len], response);
            _ = ctx.calls.fetchAdd(1, .seq_cst);
            return .{ .response_len = response.len };
        }

        fn cleanupSession(_: *anyopaque, _: u16) void {}
    };

    const TestClock = struct {
        tick: u64 = 0,

        fn clock(self: *@This()) Clock {
            return .{
                .ptr = @ptrCast(self),
                .vtable = &vtable,
            };
        }

        const vtable = Clock.VTable{
            .now = &now,
        };

        fn now(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.tick;
        }

        fn advance(self: *@This(), by: u64) void {
            self.tick += by;
        }
    };

    const TestConnection = struct {
        request: []const u8,
        served: bool = false,
        read_would_block_budget: u8 = 0,
        read_would_block_count: usize = 0,
        write_would_block_budget: u8 = 0,
        write_would_block_count: usize = 0,
        closed: bool = false,
        close_calls: usize = 0,
        writes: usize = 0,
        last_response: [64]u8 = undefined,
        last_response_len: usize = 0,

        fn connection(self: *@This()) Connection {
            return .{
                .ptr = @ptrCast(self),
                .vtable = &vtable,
            };
        }

        const vtable = Connection.VTable{
            .readRequest = &readRequest,
            .writeResponse = &writeResponse,
            .close = &close,
        };

        fn readRequest(ptr: *anyopaque, out: []u8) transport_mod.ConnectionError!?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.closed) return null;
            if (!self.served) {
                if (self.read_would_block_budget > 0) {
                    self.read_would_block_budget -= 1;
                    self.read_would_block_count += 1;
                    return error.WouldBlock;
                }
                if (self.request.len > out.len) return error.RequestTooLarge;
                @memcpy(out[0..self.request.len], self.request);
                self.served = true;
                return out[0..self.request.len];
            }
            return error.WouldBlock;
        }

        fn writeResponse(ptr: *anyopaque, data: []const u8) transport_mod.ConnectionError!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.closed) return error.WriteFailed;
            if (self.write_would_block_budget > 0) {
                self.write_would_block_budget -= 1;
                self.write_would_block_count += 1;
                return error.WouldBlock;
            }
            if (data.len > self.last_response.len) return error.ResponseTooLarge;
            @memcpy(self.last_response[0..data.len], data);
            self.last_response_len = data.len;
            self.writes += 1;
        }

        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closed = true;
            self.close_calls += 1;
        }
    };

    const TestAcceptor = struct {
        connections: []Connection,
        index: usize = 0,

        fn acceptor(self: *@This()) Acceptor {
            return .{
                .ptr = @ptrCast(self),
                .vtable = &vtable,
            };
        }

        const vtable = Acceptor.VTable{
            .accept = &accept,
        };

        fn accept(ptr: *anyopaque) transport_mod.AcceptError!?Connection {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.index >= self.connections.len) return null;
            const conn = self.connections[self.index];
            self.index += 1;
            return conn;
        }
    };

    var ctx = DispatchCtx{};
    var clock = TestClock{};
    var reactor = Reactor.init(.{
        .ctx = &ctx,
        .dispatch = &DispatchCtx.dispatch,
        .cleanupSession = &DispatchCtx.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 128,
    });
    defer reactor.deinit();

    var conn_a = TestConnection{
        .request = "User {}",
        .read_would_block_budget = 1,
    };
    var conn_b = TestConnection{
        .request = "User {}",
        .read_would_block_budget = 2,
        .write_would_block_budget = 1,
    };
    var conns = [_]Connection{
        conn_a.connection(),
        conn_b.connection(),
    };
    var acceptor = TestAcceptor{
        .connections = conns[0..],
    };

    var steps: usize = 0;
    while (steps < 64) : (steps += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        clock.advance(1);
        if (conn_a.writes == 1 and conn_b.writes == 1) break;
    }

    try std.testing.expectEqual(@as(usize, 2), reactor.activeSessions());
    try std.testing.expectEqual(@as(usize, 2), ctx.calls.load(.seq_cst));
    try std.testing.expectEqual(@as(usize, 1), conn_a.writes);
    try std.testing.expectEqual(@as(usize, 1), conn_b.writes);
    try std.testing.expectEqualStrings("OK\n", conn_a.last_response[0..conn_a.last_response_len]);
    try std.testing.expectEqualStrings("OK\n", conn_b.last_response[0..conn_b.last_response_len]);
    try std.testing.expect(!conn_a.closed);
    try std.testing.expect(!conn_b.closed);
}
