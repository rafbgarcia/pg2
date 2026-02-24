//! Feature coverage for concurrent session progress through reactor + session dispatch.
const std = @import("std");
const pg2 = @import("pg2");
const feature = @import("../test_env_test.zig");

const reactor_mod = pg2.server.reactor;
const session_mod = pg2.server.session;
const pool_mod = pg2.server.pool;
const transport_mod = pg2.server.transport;
const io_mod = pg2.storage.io;

const Acceptor = transport_mod.Acceptor;
const Connection = transport_mod.Connection;
const DispatchResult = reactor_mod.Dispatcher.DispatchResult;

const request_a = "ConcUser |> where(id == 1) { id name }";
const request_b = "ConcUser |> where(id == 2) { id name }";
const request_c = "ConcUser |> sort(id asc) { id }";

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
    served: bool = false,
    closed: bool = false,
    writes: usize = 0,
    last_response: [256]u8 = undefined,
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
        if (self.served) return error.WouldBlock;
        if (self.request.len > out.len) return error.RequestTooLarge;
        @memcpy(out[0..self.request.len], self.request);
        self.served = true;
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

const GatedSessionDispatch = struct {
    session: *session_mod.Session,
    pool: *pool_mod.ConnectionPool,
    pin_states: [3]session_mod.SessionPinState = [_]session_mod.SessionPinState{.{}} ** 3,
    calls: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    started_order: [8]u8 = [_]u8{0} ** 8,
    started_len: usize = 0,
    completed_order: [8]u8 = [_]u8{0} ** 8,
    completed_len: usize = 0,
    released_mask: u8 = 0,
    gate_mutex: std.Thread.Mutex = .{},
    gate_cond: std.Thread.Condition = .{},

    fn dispatch(
        ctx_ptr: *anyopaque,
        session_id: u16,
        request: []const u8,
        out: []u8,
    ) session_mod.SessionError!DispatchResult {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        const tag = tagForRequest(request) catch return error.ResponseTooLarge;

        self.gate_mutex.lock();
        self.started_order[self.started_len] = tag;
        self.started_len += 1;
        _ = self.calls.fetchAdd(1, .seq_cst);
        while (isGated(tag) and !self.isReleased(tag)) {
            self.gate_cond.wait(&self.gate_mutex);
        }
        self.gate_mutex.unlock();

        const session_result = try self.session.dispatchRequestForSession(
            self.pool,
            &self.pin_states[session_id],
            request,
            out,
        );

        self.gate_mutex.lock();
        self.completed_order[self.completed_len] = tag;
        self.completed_len += 1;
        self.gate_mutex.unlock();
        return .{
            .response_len = session_result.bytes_written,
            .pin_transition = session_result.pin_transition,
        };
    }

    fn release(self: *@This(), tag: u8) !void {
        if (!isGated(tag)) return;
        self.gate_mutex.lock();
        self.released_mask |= maskForTag(tag) catch {
            self.gate_mutex.unlock();
            return error.InvalidSyntax;
        };
        self.gate_cond.broadcast();
        self.gate_mutex.unlock();
    }

    fn releaseAll(self: *@This()) void {
        self.gate_mutex.lock();
        self.released_mask = std.math.maxInt(u8);
        self.gate_cond.broadcast();
        self.gate_mutex.unlock();
    }

    fn isReleased(self: *@This(), tag: u8) bool {
        const mask = maskForTag(tag) catch return true;
        return (self.released_mask & mask) != 0;
    }

    fn isGated(tag: u8) bool {
        return tag == 'A' or tag == 'B';
    }

    fn maskForTag(tag: u8) error{InvalidTag}!u8 {
        return switch (tag) {
            'A' => 1 << 0,
            'B' => 1 << 1,
            else => error.InvalidTag,
        };
    }

    fn tagForRequest(request: []const u8) error{InvalidRequest}!u8 {
        if (std.mem.eql(u8, request, request_a)) return 'A';
        if (std.mem.eql(u8, request, request_b)) return 'B';
        if (std.mem.eql(u8, request, request_c)) return 'C';
        return error.InvalidRequest;
    }

    fn cleanupSession(ptr: *anyopaque, session_id: u16) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.session.cleanupPinnedSession(
            self.pool,
            &self.pin_states[session_id],
        );
    }
};

test "feature server concurrency progresses fast session while one worker remains blocked" {
    var env: feature.FeatureEnv = undefined;
    try env.initWithConfigAndMemory(
        .{ .max_query_slots = 2 },
        128 * 1024 * 1024,
    );
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\ConcUser {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );

    _ = try executor.run("ConcUser |> insert(id = 1, name = \"Alice\") {}");
    _ = try executor.run("ConcUser |> insert(id = 2, name = \"Bob\") {}");

    const Reactor = reactor_mod.ServerReactor(3, 128, 256);
    var dispatch_ctx = GatedSessionDispatch{
        .session = &executor.session,
        .pool = &executor.pool,
    };
    defer dispatch_ctx.releaseAll();

    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &GatedSessionDispatch.dispatch,
        .cleanupSession = &GatedSessionDispatch.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 1024,
        .max_queued_requests = 3,
        .max_inflight = 2,
    });
    defer reactor.deinit();

    var conn_a = ScriptedConnection{ .request = request_a };
    var conn_b = ScriptedConnection{ .request = request_b };
    var conn_c = ScriptedConnection{ .request = request_c };
    var conns = [_]Connection{
        conn_a.connection(),
        conn_b.connection(),
        conn_c.connection(),
    };
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    var steps: usize = 0;
    while (steps < 4096) : (steps += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        clock.advance(1);
        const stats = reactor.stats();
        if (dispatch_ctx.calls.load(.seq_cst) == 2 and stats.workers_busy == 2) break;
    }

    try std.testing.expectEqual(@as(usize, 0), conn_a.writes);
    try std.testing.expectEqual(@as(usize, 0), conn_b.writes);
    try std.testing.expectEqual(@as(usize, 0), conn_c.writes);

    try dispatch_ctx.release('B');
    steps = 0;
    while (steps < 4096) : (steps += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        clock.advance(1);
        if (conn_b.writes == 1 and conn_c.writes == 1) break;
    }

    try std.testing.expectEqual(@as(usize, 1), conn_b.writes);
    try std.testing.expectEqual(@as(usize, 1), conn_c.writes);
    try std.testing.expectEqual(@as(usize, 0), conn_a.writes);
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n2,Bob\n",
        conn_b.last_response[0..conn_b.last_response_len],
    );
    try std.testing.expectEqualStrings(
        "OK returned_rows=2 inserted_rows=0 updated_rows=0 deleted_rows=0\n1\n2\n",
        conn_c.last_response[0..conn_c.last_response_len],
    );

    try dispatch_ctx.release('A');
    steps = 0;
    while (steps < 4096) : (steps += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        clock.advance(1);
        if (conn_a.writes == 1) break;
    }

    try std.testing.expectEqual(@as(usize, 1), conn_a.writes);
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,Alice\n",
        conn_a.last_response[0..conn_a.last_response_len],
    );
    try std.testing.expectEqual(@as(usize, 3), dispatch_ctx.calls.load(.seq_cst));
    try std.testing.expectEqual(@as(usize, 3), dispatch_ctx.started_len);
    const first = dispatch_ctx.started_order[0];
    const second = dispatch_ctx.started_order[1];
    const first_two_are_a_b =
        (first == 'A' and second == 'B') or
        (first == 'B' and second == 'A');
    try std.testing.expect(first_two_are_a_b);
    try std.testing.expectEqual(@as(u8, 'C'), dispatch_ctx.started_order[2]);
}
