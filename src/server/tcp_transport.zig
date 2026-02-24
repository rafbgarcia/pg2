//! TCP transport backend implementing server transport interfaces.
//!
//! Responsibilities in this file:
//! - Accepts TCP connections and exposes them as `transport.Connection`.
//! - Implements newline-delimited request framing and response writes.
//! - Handles connection lifecycle and platform-safe cleanup for tests/runtime.
const std = @import("std");
const builtin = @import("builtin");
const transport_mod = @import("transport.zig");

const Acceptor = transport_mod.Acceptor;
const Connection = transport_mod.Connection;
const AcceptError = transport_mod.AcceptError;
const ConnectionError = transport_mod.ConnectionError;
const max_request_bytes = 4096;
const max_response_bytes = 4096;

pub const TcpAcceptor = struct {
    server: std.net.Server,
    active_connection: ?TcpConnection = null,

    pub fn listen(
        address: std.net.Address,
        options: std.net.Address.ListenOptions,
    ) !TcpAcceptor {
        return .{
            .server = try address.listen(options),
            .active_connection = null,
        };
    }

    pub fn deinit(self: *TcpAcceptor) void {
        if (self.active_connection) |*active| {
            if (!active.closed) {
                active.stream.close();
            }
            self.active_connection = null;
        }
        self.server.deinit();
    }

    pub fn boundAddress(self: *const TcpAcceptor) std.net.Address {
        return self.server.listen_address;
    }

    pub fn acceptor(self: *TcpAcceptor) Acceptor {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &acceptor_vtable,
        };
    }

    const acceptor_vtable = Acceptor.VTable{
        .accept = &acceptImpl,
    };

    fn acceptImpl(ptr: *anyopaque) AcceptError!?Connection {
        const self: *TcpAcceptor = @ptrCast(@alignCast(ptr));

        if (self.active_connection) |active| {
            if (!active.closed) return error.AcceptFailed;
            self.active_connection = null;
        }

        const accepted = self.server.accept() catch return error.AcceptFailed;
        self.active_connection = .{
            .stream = accepted.stream,
        };
        return self.active_connection.?.connection();
    }
};

const TcpConnection = struct {
    stream: std.net.Stream,
    closed: bool = false,
    request_buf: [max_request_bytes]u8 = undefined,
    request_len: usize = 0,
    pending_write_buf: [max_response_bytes]u8 = undefined,
    pending_write_len: usize = 0,
    pending_write_sent: usize = 0,

    fn connection(self: *TcpConnection) Connection {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Connection.VTable{
        .readRequest = &readRequestImpl,
        .writeResponse = &writeResponseImpl,
        .close = &closeImpl,
    };

    fn readRequestImpl(
        ptr: *anyopaque,
        out: []u8,
    ) ConnectionError!?[]const u8 {
        const self: *TcpConnection = @ptrCast(@alignCast(ptr));
        if (self.closed) return null;

        var byte_buf: [1]u8 = undefined;

        while (true) {
            const n = self.stream.read(byte_buf[0..]) catch |err| switch (err) {
                error.WouldBlock => return error.WouldBlock,
                else => return error.ReadFailed,
            };
            if (n == 0) {
                closeImpl(ptr);
                if (self.request_len == 0) return null;
                if (self.request_len > out.len) return error.RequestTooLarge;
                @memcpy(out[0..self.request_len], self.request_buf[0..self.request_len]);
                const line = trimLineEnding(out[0..self.request_len]);
                self.request_len = 0;
                return line;
            }

            const byte = byte_buf[0];
            if (byte == '\n') {
                if (self.request_len > out.len) return error.RequestTooLarge;
                @memcpy(out[0..self.request_len], self.request_buf[0..self.request_len]);
                const line = trimLineEnding(out[0..self.request_len]);
                self.request_len = 0;
                return line;
            }

            if (self.request_len >= self.request_buf.len) return error.RequestTooLarge;
            self.request_buf[self.request_len] = byte;
            self.request_len += 1;
        }
    }

    fn writeResponseImpl(
        ptr: *anyopaque,
        data: []const u8,
    ) ConnectionError!void {
        const self: *TcpConnection = @ptrCast(@alignCast(ptr));
        if (self.closed) return error.WriteFailed;

        if (self.pending_write_len == 0) {
            if (data.len > self.pending_write_buf.len) return error.ResponseTooLarge;
            @memcpy(self.pending_write_buf[0..data.len], data);
            self.pending_write_len = data.len;
            self.pending_write_sent = 0;
        } else {
            const pending = self.pending_write_buf[0..self.pending_write_len];
            if (!std.mem.eql(u8, pending, data)) return error.WriteFailed;
        }

        while (self.pending_write_sent < self.pending_write_len) {
            const sent = std.posix.send(
                self.stream.handle,
                self.pending_write_buf[self.pending_write_sent..self.pending_write_len],
                0,
            ) catch |err| switch (err) {
                error.WouldBlock => return error.WouldBlock,
                else => return error.WriteFailed,
            };
            if (sent == 0) return error.WriteFailed;
            self.pending_write_sent += sent;
        }

        self.pending_write_len = 0;
        self.pending_write_sent = 0;
    }

    fn closeImpl(ptr: *anyopaque) void {
        const self: *TcpConnection = @ptrCast(@alignCast(ptr));
        if (self.closed) return;
        self.stream.close();
        self.closed = true;
        self.request_len = 0;
        self.pending_write_len = 0;
        self.pending_write_sent = 0;
    }
};

fn trimLineEnding(line: []const u8) []const u8 {
    if (line.len > 0 and line[line.len - 1] == '\r') {
        return line[0 .. line.len - 1];
    }
    return line;
}

test "tcp transport accepts and reads newline-delimited request" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var listener = try TcpAcceptor.listen(
        try std.net.Address.parseIp("127.0.0.1", 0),
        .{ .reuse_address = true },
    );
    defer listener.deinit();

    const address = listener.boundAddress();

    const ClientThread = struct {
        fn run(addr: std.net.Address) !void {
            const stream = try std.net.tcpConnectToAddress(addr);
            defer stream.close();
            try stream.writeAll("User\r\n");
        }
    };

    const thread = try std.Thread.spawn(.{}, ClientThread.run, .{address});
    defer thread.join();

    const conn = (try listener.acceptor().accept()).?;
    var request_buf: [64]u8 = undefined;
    const request = (try conn.readRequest(request_buf[0..])).?;
    try std.testing.expectEqualStrings("User", request);
    try std.testing.expect((try conn.readRequest(request_buf[0..])) == null);
    conn.close();
}
