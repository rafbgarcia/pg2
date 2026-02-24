//! Bounded server reactor for multiplexed connection progress.
const std = @import("std");
const session_mod = @import("session.zig");
const transport_mod = @import("transport.zig");

const Acceptor = transport_mod.Acceptor;
const Connection = transport_mod.Connection;

pub const Dispatcher = struct {
    ctx: *anyopaque,
    dispatch: *const fn (
        ctx: *anyopaque,
        request: []const u8,
        out: []u8,
    ) session_mod.SessionError!usize,
};

pub fn ServerReactor(
    comptime max_sessions: usize,
    comptime request_buf_bytes: usize,
    comptime response_buf_bytes: usize,
) type {
    return struct {
        const Self = @This();
        const SessionSlot = struct {
            in_use: bool = false,
            connection: Connection = undefined,
            request_buf: [request_buf_bytes]u8 = undefined,
            request_len: usize = 0,
            has_request: bool = false,
            response_buf: [response_buf_bytes]u8 = undefined,
            response_len: usize = 0,
            has_response: bool = false,
        };

        pub const ReactorError = session_mod.SessionError ||
            transport_mod.AcceptError ||
            transport_mod.ConnectionError;

        dispatcher: Dispatcher,
        sessions: [max_sessions]SessionSlot = [_]SessionSlot{.{}} ** max_sessions,
        read_cursor: usize = 0,
        dispatch_cursor: usize = 0,
        write_cursor: usize = 0,

        pub fn init(dispatcher: Dispatcher) Self {
            std.debug.assert(max_sessions > 0);
            std.debug.assert(request_buf_bytes > 0);
            std.debug.assert(response_buf_bytes > 0);
            return .{
                .dispatcher = dispatcher,
            };
        }

        pub fn deinit(self: *Self) void {
            var i: usize = 0;
            while (i < self.sessions.len) : (i += 1) {
                if (self.sessions[i].in_use) self.closeSlot(i);
            }
        }

        pub fn step(self: *Self, acceptor: Acceptor) ReactorError!void {
            try self.acceptPending(acceptor);
            try self.flushPendingWrites();
            try self.pollReads();
            try self.dispatchOne();
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
            while (visited < self.sessions.len) : (visited += 1) {
                const i = (self.read_cursor + visited) % self.sessions.len;
                var slot = &self.sessions[i];
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
            }
            self.read_cursor = (self.read_cursor + 1) % self.sessions.len;
        }

        fn dispatchOne(self: *Self) session_mod.SessionError!void {
            var visited: usize = 0;
            while (visited < self.sessions.len) : (visited += 1) {
                const i = (self.dispatch_cursor + visited) % self.sessions.len;
                var slot = &self.sessions[i];
                if (!slot.in_use) continue;
                if (!slot.has_request or slot.has_response) continue;

                const response_len = try self.dispatcher.dispatch(
                    self.dispatcher.ctx,
                    slot.request_buf[0..slot.request_len],
                    slot.response_buf[0..],
                );
                slot.has_request = false;
                slot.request_len = 0;
                slot.has_response = true;
                slot.response_len = response_len;
                self.dispatch_cursor = (i + 1) % self.sessions.len;
                return;
            }
            self.dispatch_cursor = (self.dispatch_cursor + 1) % self.sessions.len;
        }

        fn flushPendingWrites(self: *Self) transport_mod.ConnectionError!void {
            var visited: usize = 0;
            while (visited < self.sessions.len) : (visited += 1) {
                const i = (self.write_cursor + visited) % self.sessions.len;
                var slot = &self.sessions[i];
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

        fn closeSlot(self: *Self, i: usize) void {
            var slot = &self.sessions[i];
            if (!slot.in_use) return;
            slot.connection.close();
            slot.* = .{};
        }
    };
}

test "reactor tracks two simultaneous sessions and dispatches both requests" {
    const Reactor = ServerReactor(4, 256, 256);
    const response = "OK\n";
    const DispatchCtx = struct {
        calls: usize = 0,
        fn dispatch(ctx_ptr: *anyopaque, _: []const u8, out: []u8) session_mod.SessionError!usize {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            if (response.len > out.len) return error.ResponseTooLarge;
            @memcpy(out[0..response.len], response);
            ctx.calls += 1;
            return response.len;
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
    var reactor = Reactor.init(.{
        .ctx = &ctx,
        .dispatch = &DispatchCtx.dispatch,
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
    while (steps < 32) : (steps += 1) {
        try reactor.step(acceptor.acceptor());
        if (conn_a.writes == 1 and conn_b.writes == 1) break;
    }

    try std.testing.expectEqual(@as(usize, 2), reactor.activeSessions());
    try std.testing.expectEqual(@as(usize, 2), ctx.calls);
    try std.testing.expectEqual(@as(usize, 1), conn_a.writes);
    try std.testing.expectEqual(@as(usize, 1), conn_b.writes);
    try std.testing.expectEqualStrings("OK\n", conn_a.last_response[0..conn_a.last_response_len]);
    try std.testing.expectEqualStrings("OK\n", conn_b.last_response[0..conn_b.last_response_len]);
    try std.testing.expect(!conn_a.closed);
    try std.testing.expect(!conn_b.closed);
}
