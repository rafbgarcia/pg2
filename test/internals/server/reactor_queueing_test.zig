//! Deterministic scheduler queueing contracts for server reactor.
const std = @import("std");
const pg2 = @import("pg2");

const reactor_mod = pg2.server.reactor;
const diagnostics_mod = pg2.server.diagnostics;
const session_mod = pg2.server.session;
const transport_mod = pg2.server.transport;
const io_mod = pg2.storage.io;

const Acceptor = transport_mod.Acceptor;
const Connection = transport_mod.Connection;
const DispatchResult = reactor_mod.Dispatcher.DispatchResult;
const RuntimeInspectStats = diagnostics_mod.RuntimeInspectStats;

const ManualClock = struct {
    tick: u64 = 0,

    fn clock(self: *@This()) io_mod.Clock {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = io_mod.Clock.VTable{
        .now = &now,
    };

    fn now(ptr: *anyopaque) u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.tick;
    }

    fn advance(self: *@This(), ticks: u64) void {
        self.tick += ticks;
    }
};

const ScriptedConnection = struct {
    request: []const u8,
    request_served: bool = false,
    closed: bool = false,
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
        if (self.request_served) return error.WouldBlock;
        if (self.request.len > out.len) return error.RequestTooLarge;
        @memcpy(out[0..self.request.len], self.request);
        self.request_served = true;
        return out[0..self.request.len];
    }

    fn writeResponse(ptr: *anyopaque, data: []const u8) transport_mod.ConnectionError!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.closed) return error.WriteFailed;
        if (data.len > self.last_response.len) return error.ResponseTooLarge;
        @memcpy(self.last_response[0..data.len], data);
        self.last_response_len = data.len;
        self.writes += 1;
    }

    fn close(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.closed = true;
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

const TraceDispatch = struct {
    calls: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    order_len: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    order_mutex: std.Thread.Mutex = .{},
    order: [8]u8 = [_]u8{0} ** 8,

    fn dispatch(
        ctx_ptr: *anyopaque,
        _: u16,
        request: []const u8,
        _: RuntimeInspectStats,
        out: []u8,
    ) session_mod.SessionError!DispatchResult {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (request.len < 2) return error.ResponseTooLarge;
        self.order_mutex.lock();
        defer self.order_mutex.unlock();
        const idx = self.order_len.load(.seq_cst);
        self.order[idx] = request[1];
        self.order_len.store(idx + 1, .seq_cst);
        _ = self.calls.fetchAdd(1, .seq_cst);
        const response = "OK\n";
        if (response.len > out.len) return error.ResponseTooLarge;
        @memcpy(out[0..response.len], response);
        return .{ .response_len = response.len };
    }

    fn cleanupSession(_: *anyopaque, _: u16) void {}
};

const BlockingDispatch = struct {
    calls: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    gate_mutex: std.Thread.Mutex = .{},
    gate_cond: std.Thread.Condition = .{},
    release: bool = false,

    fn dispatch(
        ctx_ptr: *anyopaque,
        _: u16,
        _: []const u8,
        _: RuntimeInspectStats,
        out: []u8,
    ) session_mod.SessionError!DispatchResult {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        _ = self.calls.fetchAdd(1, .seq_cst);
        self.gate_mutex.lock();
        defer self.gate_mutex.unlock();
        while (!self.release) {
            self.gate_cond.wait(&self.gate_mutex);
        }
        const response = "OK\n";
        if (response.len > out.len) return error.ResponseTooLarge;
        @memcpy(out[0..response.len], response);
        return .{ .response_len = response.len };
    }

    fn unblock(self: *@This()) void {
        self.gate_mutex.lock();
        self.release = true;
        self.gate_cond.signal();
        self.gate_mutex.unlock();
    }

    fn cleanupSession(_: *anyopaque, _: u16) void {}
};

const MultiGateDispatch = struct {
    calls: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    dispatch_order_len: usize = 0,
    completion_order_len: usize = 0,
    dispatch_order: [8]u8 = [_]u8{0} ** 8,
    completion_order: [8]u8 = [_]u8{0} ** 8,
    gate_mask: u8 = 0,
    gate_mutex: std.Thread.Mutex = .{},
    gate_cond: std.Thread.Condition = .{},

    fn dispatch(
        ctx_ptr: *anyopaque,
        _: u16,
        request: []const u8,
        _: RuntimeInspectStats,
        out: []u8,
    ) session_mod.SessionError!DispatchResult {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (request.len < 2) return error.ResponseTooLarge;
        const tag = request[1];
        const idx = tagToIndex(tag) catch return error.ResponseTooLarge;
        const bit = @as(u8, 1) << idx;

        self.gate_mutex.lock();
        self.dispatch_order[self.dispatch_order_len] = tag;
        self.dispatch_order_len += 1;
        _ = self.calls.fetchAdd(1, .seq_cst);
        while ((self.gate_mask & bit) == 0) {
            self.gate_cond.wait(&self.gate_mutex);
        }
        self.completion_order[self.completion_order_len] = tag;
        self.completion_order_len += 1;
        self.gate_mutex.unlock();

        const response = "OK\n";
        if (response.len > out.len) return error.ResponseTooLarge;
        @memcpy(out[0..response.len], response);
        return .{ .response_len = response.len };
    }

    fn release(self: *@This(), tag: u8) !void {
        const idx = try tagToIndex(tag);
        self.gate_mutex.lock();
        self.gate_mask |= @as(u8, 1) << idx;
        self.gate_cond.broadcast();
        self.gate_mutex.unlock();
    }

    fn releaseAll(self: *@This()) void {
        self.gate_mutex.lock();
        self.gate_mask = std.math.maxInt(u8);
        self.gate_cond.broadcast();
        self.gate_mutex.unlock();
    }

    fn tagToIndex(tag: u8) error{InvalidTag}!u3 {
        return switch (tag) {
            '0' => 0,
            '1' => 1,
            '2' => 2,
            '3' => 3,
            else => error.InvalidTag,
        };
    }

    fn cleanupSession(_: *anyopaque, _: u16) void {}
};

test "reactor emits QueueFull when queue admission capacity is saturated" {
    const Reactor = reactor_mod.ServerReactor(3, 64, 64);

    var dispatch_ctx = TraceDispatch{};
    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &TraceDispatch.dispatch,
        .cleanupSession = &TraceDispatch.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 100,
        .max_queued_requests = 2,
    });
    defer reactor.deinit();

    var conn_a = ScriptedConnection{ .request = "a0" };
    var conn_b = ScriptedConnection{ .request = "b1" };
    var conn_c = ScriptedConnection{ .request = "c2" };
    var conns = [_]Connection{ conn_a.connection(), conn_b.connection(), conn_c.connection() };
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    try reactor.step(acceptor.acceptor());

    try std.testing.expectEqual(@as(usize, 1), conn_c.writes);
    try std.testing.expectEqualStrings(
        "ERR class=overload code=QueueFull\n",
        conn_c.last_response[0..conn_c.last_response_len],
    );

    const stats = reactor.stats();
    try std.testing.expectEqual(@as(u64, 1), stats.queue_full_total);
}

test "reactor emits QueueTimeout exactly at deadline before dispatch" {
    const Reactor = reactor_mod.ServerReactor(3, 64, 64);

    var dispatch_ctx = TraceDispatch{};
    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &TraceDispatch.dispatch,
        .cleanupSession = &TraceDispatch.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 1,
        .max_queued_requests = 3,
    });
    defer reactor.deinit();

    var conn_a = ScriptedConnection{ .request = "a0" };
    var conn_b = ScriptedConnection{ .request = "b1" };
    var conn_c = ScriptedConnection{ .request = "c2" };
    var conns = [_]Connection{ conn_a.connection(), conn_b.connection(), conn_c.connection() };
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    try reactor.step(acceptor.acceptor());
    clock.advance(1);
    try reactor.step(acceptor.acceptor());
    var polls: usize = 0;
    while (polls < 4096) : (polls += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        if (conn_a.writes == 1 and conn_b.writes == 1 and conn_c.writes == 1) break;
    }

    try std.testing.expectEqual(@as(usize, 1), dispatch_ctx.calls.load(.seq_cst));
    try std.testing.expectEqual(@as(usize, 1), conn_a.writes);
    try std.testing.expectEqualStrings("OK\n", conn_a.last_response[0..conn_a.last_response_len]);

    try std.testing.expectEqual(@as(usize, 1), conn_b.writes);
    try std.testing.expectEqualStrings(
        "ERR class=overload code=QueueTimeout\n",
        conn_b.last_response[0..conn_b.last_response_len],
    );
    try std.testing.expectEqual(@as(usize, 1), conn_c.writes);
    try std.testing.expectEqualStrings(
        "ERR class=overload code=QueueTimeout\n",
        conn_c.last_response[0..conn_c.last_response_len],
    );

    const stats = reactor.stats();
    try std.testing.expectEqual(@as(u64, 2), stats.queue_timeout_total);
}

test "reactor dispatches queued sessions in round-robin fair order" {
    const Reactor = reactor_mod.ServerReactor(4, 64, 64);

    var dispatch_ctx = TraceDispatch{};
    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &TraceDispatch.dispatch,
        .cleanupSession = &TraceDispatch.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 100,
        .max_queued_requests = 4,
    });
    defer reactor.deinit();

    var conn_a = ScriptedConnection{ .request = "a0" };
    var conn_b = ScriptedConnection{ .request = "b1" };
    var conn_c = ScriptedConnection{ .request = "c2" };
    var conn_d = ScriptedConnection{ .request = "d3" };
    var conns = [_]Connection{
        conn_a.connection(),
        conn_b.connection(),
        conn_c.connection(),
        conn_d.connection(),
    };
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    var steps: usize = 0;
    while (steps < 128) : (steps += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        clock.advance(1);
        if (dispatch_ctx.calls.load(.seq_cst) == 4) break;
    }

    try std.testing.expectEqual(@as(usize, 4), dispatch_ctx.calls.load(.seq_cst));
    try std.testing.expectEqual(@as(u8, '0'), dispatch_ctx.order[0]);
    try std.testing.expectEqual(@as(u8, '1'), dispatch_ctx.order[1]);
    try std.testing.expectEqual(@as(u8, '2'), dispatch_ctx.order[2]);
    try std.testing.expectEqual(@as(u8, '3'), dispatch_ctx.order[3]);
}

test "reactor keeps progressing reads timeouts and writes while worker is busy" {
    const Reactor = reactor_mod.ServerReactor(2, 64, 64);

    var dispatch_ctx = BlockingDispatch{};
    defer dispatch_ctx.unblock();
    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &BlockingDispatch.dispatch,
        .cleanupSession = &BlockingDispatch.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 2,
        .max_queued_requests = 2,
    });
    defer reactor.deinit();

    var conn_a = ScriptedConnection{ .request = "a0" };
    var conn_b = ScriptedConnection{ .request = "b1" };
    var conns = [_]Connection{ conn_a.connection(), conn_b.connection() };
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    try reactor.step(acceptor.acceptor());
    const after_first = reactor.stats();
    try std.testing.expectEqual(@as(usize, 1), after_first.workers_busy);

    clock.advance(1);
    try reactor.step(acceptor.acceptor());
    const before_timeout = reactor.stats();
    try std.testing.expectEqual(@as(usize, 1), before_timeout.workers_busy);

    clock.advance(1);
    try reactor.step(acceptor.acceptor());
    try std.testing.expectEqual(@as(usize, 1), conn_b.writes);
    try std.testing.expectEqualStrings(
        "ERR class=overload code=QueueTimeout\n",
        conn_b.last_response[0..conn_b.last_response_len],
    );

    dispatch_ctx.unblock();
    var polls: usize = 0;
    while (polls < 64) : (polls += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        if (conn_a.writes == 1) break;
    }

    try std.testing.expectEqual(@as(usize, 1), dispatch_ctx.calls.load(.seq_cst));
    try std.testing.expectEqual(@as(usize, 1), conn_a.writes);
    try std.testing.expectEqualStrings("OK\n", conn_a.last_response[0..conn_a.last_response_len]);
}

test "reactor preserves deterministic mixed completion ordering with max_inflight=2" {
    const Reactor = reactor_mod.ServerReactor(3, 64, 64);

    var dispatch_ctx = MultiGateDispatch{};
    defer dispatch_ctx.releaseAll();
    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &MultiGateDispatch.dispatch,
        .cleanupSession = &MultiGateDispatch.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 100,
        .max_queued_requests = 3,
        .max_inflight = 2,
    });
    defer reactor.deinit();

    var conn_a = ScriptedConnection{ .request = "a0" };
    var conn_b = ScriptedConnection{ .request = "b1" };
    var conn_c = ScriptedConnection{ .request = "c2" };
    var conns = [_]Connection{
        conn_a.connection(),
        conn_b.connection(),
        conn_c.connection(),
    };
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    var spins: usize = 0;
    while (spins < 128) : (spins += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        if (dispatch_ctx.calls.load(.seq_cst) == 2) break;
    }

    const started_stats = reactor.stats();
    try std.testing.expectEqual(@as(usize, 2), started_stats.workers_busy);
    try std.testing.expectEqual(@as(usize, 0), conn_a.writes);
    try std.testing.expectEqual(@as(usize, 0), conn_b.writes);
    try std.testing.expectEqual(@as(usize, 0), conn_c.writes);

    try dispatch_ctx.release('1');

    spins = 0;
    while (spins < 128) : (spins += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        if (conn_b.writes == 1) break;
    }
    try std.testing.expectEqual(@as(usize, 1), conn_b.writes);
    try std.testing.expectEqual(@as(usize, 0), conn_a.writes);
    try std.testing.expectEqual(@as(usize, 0), conn_c.writes);

    try dispatch_ctx.release('0');
    spins = 0;
    while (spins < 128) : (spins += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        if (conn_a.writes == 1 and dispatch_ctx.calls.load(.seq_cst) == 3) break;
    }
    try std.testing.expectEqual(@as(usize, 1), conn_a.writes);
    try std.testing.expectEqual(@as(usize, 0), conn_c.writes);

    try dispatch_ctx.release('2');
    spins = 0;
    while (spins < 128) : (spins += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        if (conn_c.writes == 1) break;
    }

    try std.testing.expectEqual(@as(usize, 3), dispatch_ctx.calls.load(.seq_cst));
    try std.testing.expectEqual(@as(usize, 3), dispatch_ctx.dispatch_order_len);
    try std.testing.expectEqual(@as(u8, '0'), dispatch_ctx.dispatch_order[0]);
    try std.testing.expectEqual(@as(u8, '1'), dispatch_ctx.dispatch_order[1]);
    try std.testing.expectEqual(@as(u8, '2'), dispatch_ctx.dispatch_order[2]);

    try std.testing.expectEqual(@as(usize, 3), dispatch_ctx.completion_order_len);
    try std.testing.expectEqual(@as(u8, '1'), dispatch_ctx.completion_order[0]);
    try std.testing.expectEqual(@as(u8, '0'), dispatch_ctx.completion_order[1]);
    try std.testing.expectEqual(@as(u8, '2'), dispatch_ctx.completion_order[2]);
}
