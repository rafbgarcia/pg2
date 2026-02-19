const std = @import("std");
const builtin = @import("builtin");
const transport_mod = @import("transport.zig");

const Acceptor = transport_mod.Acceptor;
const Connection = transport_mod.Connection;
const AcceptError = transport_mod.AcceptError;
const ConnectionError = transport_mod.ConnectionError;

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

    fn connection(self: *TcpConnection) Connection {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = Connection.VTable{
        .readRequest = &readRequestImpl,
        .writeResponse = &writeResponseImpl,
    };

    fn readRequestImpl(
        ptr: *anyopaque,
        out: []u8,
    ) ConnectionError!?[]const u8 {
        const self: *TcpConnection = @ptrCast(@alignCast(ptr));
        if (self.closed) return null;

        var used: usize = 0;
        var byte_buf: [1]u8 = undefined;

        while (true) {
            const n = self.stream.read(byte_buf[0..]) catch return error.ReadFailed;
            if (n == 0) {
                self.stream.close();
                self.closed = true;
                if (used == 0) return null;
                return trimLineEnding(out[0..used]);
            }

            const byte = byte_buf[0];
            if (byte == '\n') {
                return trimLineEnding(out[0..used]);
            }

            if (used >= out.len) return error.RequestTooLarge;
            out[used] = byte;
            used += 1;
        }
    }

    fn writeResponseImpl(
        ptr: *anyopaque,
        data: []const u8,
    ) ConnectionError!void {
        const self: *TcpConnection = @ptrCast(@alignCast(ptr));
        if (self.closed) return error.WriteFailed;
        self.stream.writeAll(data) catch return error.WriteFailed;
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
}
