const std = @import("std");
const builtin = @import("builtin");
const transport_mod = @import("transport.zig");

const Acceptor = transport_mod.Acceptor;
const Connection = transport_mod.Connection;
const AcceptError = transport_mod.AcceptError;
const ConnectionError = transport_mod.ConnectionError;

pub const ListenError = error{
    UnsupportedPlatform,
    IoUringUnavailable,
    ListenFailed,
};

pub const IoUringAcceptor = struct {
    server: std.net.Server,
    ring: if (builtin.os.tag == .linux) std.os.linux.IoUring else void,
    accept_addr: std.posix.sockaddr,
    accept_addr_len: std.posix.socklen_t,
    active_connection: ?IoUringConnection = null,

    pub fn listen(
        address: std.net.Address,
        options: std.net.Address.ListenOptions,
    ) ListenError!IoUringAcceptor {
        if (builtin.os.tag != .linux) return error.UnsupportedPlatform;

        var server = address.listen(options) catch return error.ListenFailed;
        errdefer server.deinit();

        const ring = std.os.linux.IoUring.init(16, 0) catch
            return error.IoUringUnavailable;
        errdefer {
            var ring_deinit = ring;
            ring_deinit.deinit();
        }

        return .{
            .server = server,
            .ring = ring,
            .accept_addr = undefined,
            .accept_addr_len = @sizeOf(std.posix.sockaddr),
            .active_connection = null,
        };
    }

    pub fn deinit(self: *IoUringAcceptor) void {
        if (self.active_connection) |*active| {
            if (!active.closed) {
                active.stream.close();
            }
            self.active_connection = null;
        }
        if (builtin.os.tag == .linux) {
            self.ring.deinit();
        }
        self.server.deinit();
    }

    pub fn boundAddress(self: *const IoUringAcceptor) std.net.Address {
        return self.server.listen_address;
    }

    pub fn acceptor(self: *IoUringAcceptor) Acceptor {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &acceptor_vtable,
        };
    }

    const acceptor_vtable = Acceptor.VTable{
        .accept = &acceptImpl,
    };

    fn acceptImpl(ptr: *anyopaque) AcceptError!?Connection {
        const self: *IoUringAcceptor = @ptrCast(@alignCast(ptr));
        if (builtin.os.tag != .linux) return error.AcceptFailed;

        if (self.active_connection) |active| {
            if (!active.closed) return error.AcceptFailed;
            self.active_connection = null;
        }

        self.accept_addr_len = @sizeOf(std.posix.sockaddr);
        _ = self.ring.accept(
            0xA11CEACCE5700001,
            self.server.stream.handle,
            &self.accept_addr,
            &self.accept_addr_len,
            std.posix.SOCK.CLOEXEC,
        ) catch return error.AcceptFailed;
        _ = self.ring.submit_and_wait(1) catch return error.AcceptFailed;
        const cqe = self.ring.copy_cqe() catch return error.AcceptFailed;
        if (cqe.user_data != 0xA11CEACCE5700001) return error.AcceptFailed;
        if (cqe.err() != .SUCCESS) return error.AcceptFailed;
        if (cqe.res <= 0) return error.AcceptFailed;

        const accepted_fd: std.posix.fd_t = @intCast(cqe.res);
        self.active_connection = .{
            .stream = .{ .handle = accepted_fd },
            .ring = &self.ring,
        };
        return self.active_connection.?.connection();
    }
};

const IoUringConnection = struct {
    stream: std.net.Stream,
    ring: *std.os.linux.IoUring,
    closed: bool = false,
    recv_buf: [1024]u8 = undefined,
    recv_start: usize = 0,
    recv_end: usize = 0,

    fn connection(self: *IoUringConnection) Connection {
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
        const self: *IoUringConnection = @ptrCast(@alignCast(ptr));
        if (self.closed) return null;

        var used: usize = 0;
        while (true) {
            if (self.recv_start == self.recv_end) {
                const n = try recvIntoPending(self);
                if (n == 0) {
                    self.stream.close();
                    self.closed = true;
                    if (used == 0) return null;
                    return trimLineEnding(out[0..used]);
                }
            }

            const byte = self.recv_buf[self.recv_start];
            self.recv_start += 1;
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
        const self: *IoUringConnection = @ptrCast(@alignCast(ptr));
        if (self.closed) return error.WriteFailed;

        var sent: usize = 0;
        while (sent < data.len) {
            const chunk_sent = try sendFrom(self, data[sent..]);
            if (chunk_sent == 0) return error.WriteFailed;
            sent += chunk_sent;
        }
    }

    fn recvIntoPending(self: *IoUringConnection) ConnectionError!usize {
        _ = self.ring.recv(
            0xA11CE5700002,
            self.stream.handle,
            .{ .buffer = self.recv_buf[0..] },
            0,
        ) catch return error.ReadFailed;
        _ = self.ring.submit_and_wait(1) catch return error.ReadFailed;
        const cqe = self.ring.copy_cqe() catch return error.ReadFailed;
        if (cqe.user_data != 0xA11CE5700002) return error.ReadFailed;
        if (cqe.err() != .SUCCESS) return error.ReadFailed;
        if (cqe.res < 0) return error.ReadFailed;

        const bytes_read: usize = @intCast(cqe.res);
        self.recv_start = 0;
        self.recv_end = bytes_read;
        return bytes_read;
    }

    fn sendFrom(self: *IoUringConnection, data: []const u8) ConnectionError!usize {
        _ = self.ring.send(
            0xA11CE5700003,
            self.stream.handle,
            data,
            0,
        ) catch return error.WriteFailed;
        _ = self.ring.submit_and_wait(1) catch return error.WriteFailed;
        const cqe = self.ring.copy_cqe() catch return error.WriteFailed;
        if (cqe.user_data != 0xA11CE5700003) return error.WriteFailed;
        if (cqe.err() != .SUCCESS) return error.WriteFailed;
        if (cqe.res < 0) return error.WriteFailed;
        return @intCast(cqe.res);
    }
};

fn trimLineEnding(line: []const u8) []const u8 {
    if (line.len > 0 and line[line.len - 1] == '\r') {
        return line[0 .. line.len - 1];
    }
    return line;
}

test "io_uring transport accepts and reads newline-delimited request" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var listener = IoUringAcceptor.listen(
        try std.net.Address.parseIp("127.0.0.1", 0),
        .{ .reuse_address = true },
    ) catch |err| switch (err) {
        error.IoUringUnavailable => return error.SkipZigTest,
        else => return err,
    };
    defer listener.deinit();

    const address = listener.boundAddress();

    const ClientThread = struct {
        fn run(addr: std.net.Address) !void {
            const stream = try std.net.tcpConnectToAddress(addr);
            defer stream.close();
            try stream.writeAll("User\r\n");

            var response_buf: [64]u8 = undefined;
            var used: usize = 0;
            while (used < response_buf.len) {
                const n = try stream.read(response_buf[used..]);
                if (n == 0) break;
                used += n;
                if (std.mem.indexOfScalar(u8, response_buf[0..used], '\n') != null) {
                    break;
                }
            }
            try std.testing.expectEqualStrings("OK\n", response_buf[0..used]);
        }
    };

    const thread = try std.Thread.spawn(.{}, ClientThread.run, .{address});
    defer thread.join();

    const conn = (try listener.acceptor().accept()).?;
    var request_buf: [64]u8 = undefined;
    const request = (try conn.readRequest(request_buf[0..])).?;
    try std.testing.expectEqualStrings("User", request);
    try conn.writeResponse("OK\n");
    try std.testing.expect((try conn.readRequest(request_buf[0..])) == null);
}
