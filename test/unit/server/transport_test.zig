const std = @import("std");
const pg2 = @import("pg2");

const transport_mod = pg2.server.transport;
const Connection = transport_mod.Connection;
const ConnectionError = transport_mod.ConnectionError;

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
