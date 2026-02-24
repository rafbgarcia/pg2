//! Feature coverage for explicit tx pinning under interleaved queue pressure.
const std = @import("std");
const pg2 = @import("pg2");
const feature = @import("../test_env_test.zig");

const reactor_mod = pg2.server.reactor;
const diagnostics_mod = pg2.server.diagnostics;
const session_mod = pg2.server.session;
const pool_mod = pg2.server.pool;
const transport_mod = pg2.server.transport;
const io_mod = pg2.storage.io;

const Acceptor = transport_mod.Acceptor;
const Connection = transport_mod.Connection;
const DispatchResult = reactor_mod.Dispatcher.DispatchResult;
const RuntimeInspectStats = diagnostics_mod.RuntimeInspectStats;

const request_a_begin = "BEGIN";
const request_a_insert = "TxUser |> insert(id = 3, name = \"Cara\") {}";
const request_a_commit = "COMMIT";
const request_b_block = "TxUser |> where(id == 1) { id name }";
const request_c_read = "TxUser |> where(id == 2) { id name }";

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
    requests: []const []const u8,
    request_index: usize = 0,
    closed: bool = false,
    writes: usize = 0,
    response_log: [4][256]u8 = undefined,
    response_lens: [4]usize = [_]usize{0} ** 4,

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
        if (self.request_index >= self.requests.len) return error.WouldBlock;
        const request = self.requests[self.request_index];
        self.request_index += 1;
        if (request.len > out.len) return error.RequestTooLarge;
        @memcpy(out[0..request.len], request);
        return out[0..request.len];
    }

    fn writeResponse(ptr: *anyopaque, data: []const u8) transport_mod.ConnectionError!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.closed) return error.WriteFailed;
        if (self.writes >= self.response_log.len) return error.ResponseTooLarge;
        if (data.len > self.response_log[self.writes].len) return error.ResponseTooLarge;
        @memcpy(self.response_log[self.writes][0..data.len], data);
        self.response_lens[self.writes] = data.len;
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

const GatedTxDispatch = struct {
    session: *session_mod.Session,
    pool: *pool_mod.ConnectionPool,
    pin_states: [5]session_mod.SessionPinState = [_]session_mod.SessionPinState{.{}} ** 5,
    gate_mutex: std.Thread.Mutex = .{},
    gate_cond: std.Thread.Condition = .{},
    unblock_b: bool = false,

    fn dispatch(
        ptr: *anyopaque,
        session_id: u16,
        request: []const u8,
        runtime_inspect_stats: RuntimeInspectStats,
        out: []u8,
    ) session_mod.SessionError!DispatchResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (std.mem.eql(u8, request, request_b_block)) {
            self.gate_mutex.lock();
            while (!self.unblock_b) {
                self.gate_cond.wait(&self.gate_mutex);
            }
            self.gate_mutex.unlock();
        }
        const result = try self.session.dispatchRequestForSession(
            self.pool,
            &self.pin_states[session_id],
            request,
            runtime_inspect_stats,
            out,
        );
        return .{
            .response_len = result.bytes_written,
            .pin_transition = result.pin_transition,
        };
    }

    fn releaseBlockedB(self: *@This()) void {
        self.gate_mutex.lock();
        self.unblock_b = true;
        self.gate_cond.broadcast();
        self.gate_mutex.unlock();
    }

    fn cleanupSession(ptr: *anyopaque, session_id: u16) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.session.cleanupPinnedSession(
            self.pool,
            &self.pin_states[session_id],
        );
    }
};

test "feature interleaved pinned transactions under queue pressure keep pool leak-free" {
    var env: feature.FeatureEnv = undefined;
    try env.initWithConfigAndMemory(
        .{ .max_query_slots = 2 },
        128 * 1024 * 1024,
    );
    defer env.deinit();

    const executor = &env.executor;
    try executor.applyDefinitions(
        \\TxUser {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\}
    );
    _ = try executor.run("TxUser |> insert(id = 1, name = \"Alice\") {}");
    _ = try executor.run("TxUser |> insert(id = 2, name = \"Bob\") {}");

    const Reactor = reactor_mod.ServerReactor(5, 128, 256);
    var dispatch_ctx = GatedTxDispatch{
        .session = &executor.session,
        .pool = &executor.pool,
    };
    defer dispatch_ctx.releaseBlockedB();

    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &GatedTxDispatch.dispatch,
        .cleanupSession = &GatedTxDispatch.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 2048,
        .max_queued_requests = 5,
        .max_inflight = 1,
    });
    defer reactor.deinit();

    var conn_a = ScriptedConnection{
        .requests = &[_][]const u8{
            request_a_begin,
            request_a_insert,
            request_a_commit,
        },
    };
    var conn_b = ScriptedConnection{
        .requests = &[_][]const u8{
            request_b_block,
        },
    };
    var conn_c = ScriptedConnection{
        .requests = &[_][]const u8{
            request_c_read,
        },
    };
    var conns = [_]Connection{
        conn_a.connection(),
        conn_b.connection(),
        conn_c.connection(),
    };
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    var steps: usize = 0;
    while (steps < 256) : (steps += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        clock.advance(1);
        if (conn_a.writes >= 1 and reactor.stats().pool_pinned == 1) break;
    }
    try std.testing.expect(conn_a.writes >= 1);
    try std.testing.expectEqualStrings(
        "OK tx=BEGIN\n",
        conn_a.response_log[0][0..conn_a.response_lens[0]],
    );

    steps = 0;
    while (steps < 128) : (steps += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        clock.advance(1);
    }

    dispatch_ctx.releaseBlockedB();

    steps = 0;
    while (steps < 8192) : (steps += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        clock.advance(1);
        if (conn_a.writes == 3 and conn_b.writes == 1 and conn_c.writes == 1) break;
    }

    try std.testing.expectEqual(@as(usize, 3), conn_a.writes);
    try std.testing.expectEqualStrings(
        "OK returned_rows=0 inserted_rows=1 updated_rows=0 deleted_rows=0\n",
        conn_a.response_log[1][0..conn_a.response_lens[1]],
    );
    try std.testing.expectEqualStrings(
        "OK tx=COMMIT\n",
        conn_a.response_log[2][0..conn_a.response_lens[2]],
    );
    try std.testing.expectEqual(@as(usize, 1), conn_b.writes);
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n1,Alice\n",
        conn_b.response_log[0][0..conn_b.response_lens[0]],
    );
    try std.testing.expectEqual(@as(usize, 1), conn_c.writes);
    try std.testing.expectEqualStrings(
        "OK returned_rows=1 inserted_rows=0 updated_rows=0 deleted_rows=0\n2,Bob\n",
        conn_c.response_log[0][0..conn_c.response_lens[0]],
    );

    const stats = reactor.stats();
    try std.testing.expectEqual(@as(usize, 0), stats.pool_pinned);
    try std.testing.expect(stats.max_pin_wait_ticks > 0);
    try std.testing.expect(stats.max_pin_duration_ticks > 0);
    try std.testing.expect(stats.requests_enqueued_total >= stats.requests_dispatched_total);
    try std.testing.expect(stats.requests_dispatched_total >= stats.requests_completed_total);
    try std.testing.expect(stats.queue_depth <= 5);

    const pool_stats = executor.pool.snapshotStats();
    try std.testing.expectEqual(@as(u16, 0), pool_stats.pinned);
    try std.testing.expectEqual(@as(u16, 0), pool_stats.checked_out);
    try std.testing.expect(pool_stats.checked_out <= pool_stats.pool_size);
}
