//! Transport-agnostic server connection interfaces.
//!
//! Responsibilities in this file:
//! - Defines `Connection` request/response framing contract.
//! - Defines `Acceptor` contract for pending-connection retrieval.
//! - Provides a stable abstraction shared by TCP and io_uring backends.
const std = @import("std");

pub const AcceptError = error{
    AcceptFailed,
};

pub const ConnectionError = error{
    WouldBlock,
    ReadFailed,
    WriteFailed,
    RequestTooLarge,
    ResponseTooLarge,
};

/// Transport-agnostic accepted connection.
///
/// `readRequest` returns the next complete request frame copied into `out`,
/// or `null` when the peer closed the connection.
pub const Connection = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        readRequest: *const fn (
            ptr: *anyopaque,
            out: []u8,
        ) ConnectionError!?[]const u8,
        writeResponse: *const fn (
            ptr: *anyopaque,
            data: []const u8,
        ) ConnectionError!void,
        close: *const fn (ptr: *anyopaque) void,
    };

    pub fn readRequest(
        self: Connection,
        out: []u8,
    ) ConnectionError!?[]const u8 {
        return self.vtable.readRequest(self.ptr, out);
    }

    pub fn writeResponse(
        self: Connection,
        data: []const u8,
    ) ConnectionError!void {
        return self.vtable.writeResponse(self.ptr, data);
    }

    pub fn close(self: Connection) void {
        self.vtable.close(self.ptr);
    }
};

/// Transport accept loop abstraction.
///
/// `accept` returns `null` when there are currently no more pending
/// connections to serve.
pub const Acceptor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        accept: *const fn (ptr: *anyopaque) AcceptError!?Connection,
    };

    pub fn accept(self: Acceptor) AcceptError!?Connection {
        return self.vtable.accept(self.ptr);
    }
};

test "connection interface roundtrip with test doubles" {
    const TestConn = struct {
        request: []const u8,
        response: [64]u8 = undefined,
        response_len: usize = 0,
        served: bool = false,
        closed: bool = false,

        fn conn(self: *@This()) Connection {
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

        fn readRequest(
            ptr: *anyopaque,
            out: []u8,
        ) ConnectionError!?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.served) return null;
            if (self.request.len > out.len) return error.RequestTooLarge;
            @memcpy(out[0..self.request.len], self.request);
            self.served = true;
            return out[0..self.request.len];
        }

        fn writeResponse(
            ptr: *anyopaque,
            data: []const u8,
        ) ConnectionError!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (data.len > self.response.len) return error.ResponseTooLarge;
            @memcpy(self.response[0..data.len], data);
            self.response_len = data.len;
        }

        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closed = true;
        }
    };

    var test_conn = TestConn{ .request = "ping" };
    const conn = test_conn.conn();
    var request_buf: [16]u8 = undefined;
    const req = (try conn.readRequest(request_buf[0..])).?;
    try std.testing.expectEqualStrings("ping", req);

    try conn.writeResponse("pong");
    try std.testing.expectEqualStrings(
        "pong",
        test_conn.response[0..test_conn.response_len],
    );
    conn.close();
    try std.testing.expect(test_conn.closed);
    try std.testing.expect((try conn.readRequest(request_buf[0..])) == null);
}
