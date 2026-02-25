//! Deterministic scheduler queueing contracts for server reactor.
const std = @import("std");
const pg2 = @import("pg2");

pub const reactor_mod = pg2.server.reactor;
pub const diagnostics_mod = pg2.server.diagnostics;
pub const session_mod = pg2.server.session;
pub const transport_mod = pg2.server.transport;
pub const io_mod = pg2.storage.io;

pub const Acceptor = transport_mod.Acceptor;
pub const Connection = transport_mod.Connection;
pub const DispatchResult = reactor_mod.Dispatcher.DispatchResult;
pub const RuntimeInspectStats = diagnostics_mod.RuntimeInspectStats;

pub const ManualClock = struct {
    tick: u64 = 0,

    pub fn clock(self: *@This()) io_mod.Clock {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = io_mod.Clock.VTable{
        .now = &now,
    };

    pub fn now(ptr: *anyopaque) u64 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.tick;
    }

    pub fn advance(self: *@This(), ticks: u64) void {
        self.tick += ticks;
    }
};

pub const ScriptedConnection = struct {
    request: []const u8,
    request_served: bool = false,
    closed: bool = false,
    writes: usize = 0,
    last_response: [64]u8 = undefined,
    last_response_len: usize = 0,

    pub fn connection(self: *@This()) Connection {
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

    pub fn readRequest(ptr: *anyopaque, out: []u8) transport_mod.ConnectionError!?[]const u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.closed) return null;
        if (self.request_served) return error.WouldBlock;
        if (self.request.len > out.len) return error.RequestTooLarge;
        @memcpy(out[0..self.request.len], self.request);
        self.request_served = true;
        return out[0..self.request.len];
    }

    pub fn writeResponse(ptr: *anyopaque, data: []const u8) transport_mod.ConnectionError!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.closed) return error.WriteFailed;
        if (data.len > self.last_response.len) return error.ResponseTooLarge;
        @memcpy(self.last_response[0..data.len], data);
        self.last_response_len = data.len;
        self.writes += 1;
    }

    pub fn close(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.closed = true;
    }
};

pub const TestAcceptor = struct {
    connections: []Connection,
    index: usize = 0,

    pub fn acceptor(self: *@This()) Acceptor {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Acceptor.VTable{
        .accept = &accept,
    };

    pub fn accept(ptr: *anyopaque) transport_mod.AcceptError!?Connection {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.index >= self.connections.len) return null;
        const conn = self.connections[self.index];
        self.index += 1;
        return conn;
    }
};

pub const TraceDispatch = struct {
    calls: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    order_len: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    order_mutex: std.Thread.Mutex = .{},
    order: [8]u8 = [_]u8{0} ** 8,

    pub fn dispatch(
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

    pub fn cleanupSession(_: *anyopaque, _: u16) void {}
};

pub const BlockingDispatch = struct {
    calls: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    gate_mutex: std.Thread.Mutex = .{},
    gate_cond: std.Thread.Condition = .{},
    release: bool = false,

    pub fn dispatch(
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

    pub fn unblock(self: *@This()) void {
        self.gate_mutex.lock();
        self.release = true;
        self.gate_cond.signal();
        self.gate_mutex.unlock();
    }

    pub fn cleanupSession(_: *anyopaque, _: u16) void {}
};

pub const MultiGateDispatch = struct {
    calls: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    dispatch_order_len: usize = 0,
    completion_order_len: usize = 0,
    dispatch_order: [8]u8 = [_]u8{0} ** 8,
    completion_order: [8]u8 = [_]u8{0} ** 8,
    gate_mask: u8 = 0,
    gate_mutex: std.Thread.Mutex = .{},
    gate_cond: std.Thread.Condition = .{},

    pub fn dispatch(
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

    pub fn release(self: *@This(), tag: u8) !void {
        const idx = try tagToIndex(tag);
        self.gate_mutex.lock();
        self.gate_mask |= @as(u8, 1) << idx;
        self.gate_cond.broadcast();
        self.gate_mutex.unlock();
    }

    pub fn releaseAll(self: *@This()) void {
        self.gate_mutex.lock();
        self.gate_mask = std.math.maxInt(u8);
        self.gate_cond.broadcast();
        self.gate_mutex.unlock();
    }

    pub fn tagToIndex(tag: u8) error{InvalidTag}!u3 {
        return switch (tag) {
            '0' => 0,
            '1' => 1,
            '2' => 2,
            '3' => 3,
            else => error.InvalidTag,
        };
    }

    pub fn cleanupSession(_: *anyopaque, _: u16) void {}
};

