//! Deterministic internals coverage for reactor/session transaction pin cleanup.
const std = @import("std");
const pg2 = @import("pg2");

const bootstrap_mod = pg2.runtime.bootstrap;
const catalog_mod = pg2.catalog.meta;
const io_mod = pg2.storage.io;
const reactor_mod = pg2.server.reactor;
const session_mod = pg2.server.session;
const pool_mod = pg2.server.pool;
const transport_mod = pg2.server.transport;

const Acceptor = transport_mod.Acceptor;
const Connection = transport_mod.Connection;
const DispatchResult = reactor_mod.Dispatcher.DispatchResult;
const TxId = pg2.mvcc.transaction.TxId;

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
    disconnected: bool = false,
    disconnect_after_requests: bool = true,
    closed: bool = false,
    writes: usize = 0,
    write_attempts: usize = 0,
    fail_write_attempt: ?usize = null,
    last_response: [128]u8 = undefined,
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
        if (self.request_index < self.requests.len) {
            const req = self.requests[self.request_index];
            self.request_index += 1;
            if (req.len > out.len) return error.RequestTooLarge;
            @memcpy(out[0..req.len], req);
            return out[0..req.len];
        }
        if (self.disconnect_after_requests and !self.disconnected) {
            self.disconnected = true;
            return null;
        }
        return error.WouldBlock;
    }

    fn writeResponse(ptr: *anyopaque, data: []const u8) transport_mod.ConnectionError!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.closed) return error.WriteFailed;
        self.write_attempts += 1;
        if (self.fail_write_attempt) |attempt| {
            if (self.write_attempts == attempt) return error.WriteFailed;
        }
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

test "reactor disconnect cleanup rolls back pinned tx and releases slot" {
    const Reactor = reactor_mod.ServerReactor(2, 128, 128);

    var disk = pg2.simulator.disk.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        64 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try bootstrap_mod.BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );
    defer runtime.deinit();

    var catalog = catalog_mod.Catalog{};
    var session = session_mod.Session.init(&runtime, &catalog);
    var pool = pool_mod.ConnectionPool.init(&runtime);

    const DispatchCtx = struct {
        session: *session_mod.Session,
        pool: *pool_mod.ConnectionPool,
        pin_states: [2]session_mod.SessionPinState =
            [_]session_mod.SessionPinState{.{}} ** 2,
        cleanup_calls: usize = 0,
        last_tx_id: ?TxId = null,

        fn dispatch(
            ptr: *anyopaque,
            session_id: u16,
            request: []const u8,
            out: []u8,
        ) session_mod.SessionError!DispatchResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const result = try self.session.dispatchRequestForSession(
                self.pool,
                &self.pin_states[session_id],
                request,
                out,
            );
            return .{
                .response_len = result.bytes_written,
                .pin_transition = result.pin_transition,
            };
        }

        fn cleanupSession(ptr: *anyopaque, session_id: u16) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.pin_states[session_id].active) {
                self.last_tx_id = self.pin_states[session_id].pool_conn.tx_id;
            }
            self.cleanup_calls += 1;
            self.session.cleanupPinnedSession(
                self.pool,
                &self.pin_states[session_id],
            );
        }
    };

    var dispatch_ctx = DispatchCtx{
        .session = &session,
        .pool = &pool,
    };

    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &DispatchCtx.dispatch,
        .cleanupSession = &DispatchCtx.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 100,
        .max_queued_requests = 2,
        .max_inflight = 1,
    });
    defer reactor.deinit();

    var conn = ScriptedConnection{
        .requests = &[_][]const u8{
            "BEGIN",
        },
    };
    var conns = [_]Connection{conn.connection()};
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    var steps: usize = 0;
    while (steps < 1024) : (steps += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        clock.advance(1);
        if (conn.closed and conn.writes == 1) break;
    }

    try std.testing.expectEqual(@as(usize, 1), conn.writes);
    try std.testing.expectEqualStrings(
        "OK tx=BEGIN\n",
        conn.last_response[0..conn.last_response_len],
    );
    try std.testing.expect(conn.closed);
    try std.testing.expectEqual(@as(usize, 1), dispatch_ctx.cleanup_calls);

    const pool_stats = pool.snapshotStats();
    try std.testing.expectEqual(@as(u16, 0), pool_stats.checked_out);
    try std.testing.expectEqual(@as(u16, 0), pool_stats.pinned);

    const stats = reactor.stats();
    try std.testing.expectEqual(@as(usize, 0), stats.pool_pinned);
    try std.testing.expect(stats.requests_enqueued_total >= stats.requests_dispatched_total);
    try std.testing.expect(stats.requests_dispatched_total >= stats.requests_completed_total);

    const tx_id = dispatch_ctx.last_tx_id orelse return error.TestUnexpectedResult;
    try std.testing.expect(runtime.tx_manager.getState(tx_id).? == .aborted);

    var reused = try pool.checkout();
    defer pool.checkin(&reused) catch {};
    try std.testing.expectEqual(@as(u16, 0), reused.slot_index);
}

test "reactor write failure after COMMIT response does not leak pinned slot" {
    const Reactor = reactor_mod.ServerReactor(2, 128, 128);

    var disk = pg2.simulator.disk.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        64 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try bootstrap_mod.BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );
    defer runtime.deinit();

    var catalog = catalog_mod.Catalog{};
    var session = session_mod.Session.init(&runtime, &catalog);
    var pool = pool_mod.ConnectionPool.init(&runtime);

    const DispatchCtx = struct {
        session: *session_mod.Session,
        pool: *pool_mod.ConnectionPool,
        pin_states: [2]session_mod.SessionPinState =
            [_]session_mod.SessionPinState{.{}} ** 2,
        cleanup_calls: usize = 0,
        observed_tx_id: ?TxId = null,

        fn dispatch(
            ptr: *anyopaque,
            session_id: u16,
            request: []const u8,
            out: []u8,
        ) session_mod.SessionError!DispatchResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const result = try self.session.dispatchRequestForSession(
                self.pool,
                &self.pin_states[session_id],
                request,
                out,
            );
            if (self.pin_states[session_id].active) {
                self.observed_tx_id = self.pin_states[session_id].pool_conn.tx_id;
            }
            return .{
                .response_len = result.bytes_written,
                .pin_transition = result.pin_transition,
            };
        }

        fn cleanupSession(ptr: *anyopaque, session_id: u16) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cleanup_calls += 1;
            self.session.cleanupPinnedSession(
                self.pool,
                &self.pin_states[session_id],
            );
        }
    };

    var dispatch_ctx = DispatchCtx{
        .session = &session,
        .pool = &pool,
    };

    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &DispatchCtx.dispatch,
        .cleanupSession = &DispatchCtx.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 100,
        .max_queued_requests = 2,
        .max_inflight = 1,
    });
    defer reactor.deinit();

    var conn = ScriptedConnection{
        .requests = &[_][]const u8{
            "BEGIN",
            "COMMIT",
        },
        .disconnect_after_requests = false,
        .fail_write_attempt = 2,
    };
    var conns = [_]Connection{conn.connection()};
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    var steps: usize = 0;
    while (steps < 2048) : (steps += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        clock.advance(1);
        if (conn.closed) break;
    }

    try std.testing.expect(conn.closed);
    try std.testing.expectEqual(@as(usize, 1), conn.writes);
    try std.testing.expectEqual(@as(usize, 2), conn.write_attempts);
    try std.testing.expectEqual(@as(usize, 0), dispatch_ctx.cleanup_calls);

    const tx_id = dispatch_ctx.observed_tx_id orelse return error.TestUnexpectedResult;
    try std.testing.expect(runtime.tx_manager.getState(tx_id).? == .committed);

    const pool_stats = pool.snapshotStats();
    try std.testing.expectEqual(@as(u16, 0), pool_stats.checked_out);
    try std.testing.expectEqual(@as(u16, 0), pool_stats.pinned);

    const stats = reactor.stats();
    try std.testing.expectEqual(@as(usize, 0), stats.pool_pinned);
}

test "reactor write failure after ROLLBACK response does not leak pinned slot" {
    const Reactor = reactor_mod.ServerReactor(2, 128, 128);

    var disk = pg2.simulator.disk.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        64 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try bootstrap_mod.BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );
    defer runtime.deinit();

    var catalog = catalog_mod.Catalog{};
    var session = session_mod.Session.init(&runtime, &catalog);
    var pool = pool_mod.ConnectionPool.init(&runtime);

    const DispatchCtx = struct {
        session: *session_mod.Session,
        pool: *pool_mod.ConnectionPool,
        pin_states: [2]session_mod.SessionPinState =
            [_]session_mod.SessionPinState{.{}} ** 2,
        cleanup_calls: usize = 0,
        observed_tx_id: ?TxId = null,

        fn dispatch(
            ptr: *anyopaque,
            session_id: u16,
            request: []const u8,
            out: []u8,
        ) session_mod.SessionError!DispatchResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const result = try self.session.dispatchRequestForSession(
                self.pool,
                &self.pin_states[session_id],
                request,
                out,
            );
            if (self.pin_states[session_id].active) {
                self.observed_tx_id = self.pin_states[session_id].pool_conn.tx_id;
            }
            return .{
                .response_len = result.bytes_written,
                .pin_transition = result.pin_transition,
            };
        }

        fn cleanupSession(ptr: *anyopaque, session_id: u16) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cleanup_calls += 1;
            self.session.cleanupPinnedSession(
                self.pool,
                &self.pin_states[session_id],
            );
        }
    };

    var dispatch_ctx = DispatchCtx{
        .session = &session,
        .pool = &pool,
    };

    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &DispatchCtx.dispatch,
        .cleanupSession = &DispatchCtx.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 100,
        .max_queued_requests = 2,
        .max_inflight = 1,
    });
    defer reactor.deinit();

    var conn = ScriptedConnection{
        .requests = &[_][]const u8{
            "BEGIN",
            "ROLLBACK",
        },
        .disconnect_after_requests = false,
        .fail_write_attempt = 2,
    };
    var conns = [_]Connection{conn.connection()};
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    var steps: usize = 0;
    while (steps < 2048) : (steps += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        clock.advance(1);
        if (conn.closed) break;
    }

    try std.testing.expect(conn.closed);
    try std.testing.expectEqual(@as(usize, 1), conn.writes);
    try std.testing.expectEqual(@as(usize, 2), conn.write_attempts);
    try std.testing.expectEqual(@as(usize, 0), dispatch_ctx.cleanup_calls);

    const tx_id = dispatch_ctx.observed_tx_id orelse return error.TestUnexpectedResult;
    try std.testing.expect(runtime.tx_manager.getState(tx_id).? == .aborted);

    const pool_stats = pool.snapshotStats();
    try std.testing.expectEqual(@as(u16, 0), pool_stats.checked_out);
    try std.testing.expectEqual(@as(u16, 0), pool_stats.pinned);

    const stats = reactor.stats();
    try std.testing.expectEqual(@as(usize, 0), stats.pool_pinned);
}
