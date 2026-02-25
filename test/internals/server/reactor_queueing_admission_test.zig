//! Deterministic scheduler queue admission/timeout contracts for server reactor.
const std = @import("std");
const helper = @import("reactor_queueing_helpers.zig");

const ReactorMod = helper.reactor_mod;
const Connection = helper.Connection;
const ManualClock = helper.ManualClock;
const ScriptedConnection = helper.ScriptedConnection;
const TestAcceptor = helper.TestAcceptor;
const TraceDispatch = helper.TraceDispatch;

test "reactor emits QueueFull when queue admission capacity is saturated" {
    const Reactor = ReactorMod.ServerReactor(3, 64, 64);

    var dispatch_ctx = TraceDispatch{};
    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &TraceDispatch.dispatch,
        .cleanupSession = &TraceDispatch.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 100,
        .max_queued_requests = 2,
    });
    defer reactor.deinit();

    var conn_a = ScriptedConnection{ .request = "a0" };
    var conn_b = ScriptedConnection{ .request = "b1" };
    var conn_c = ScriptedConnection{ .request = "c2" };
    var conns = [_]Connection{ conn_a.connection(), conn_b.connection(), conn_c.connection() };
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    try reactor.step(acceptor.acceptor());

    try std.testing.expectEqual(@as(usize, 1), conn_c.writes);
    try std.testing.expectEqualStrings(
        "ERR class=overload code=QueueFull\n",
        conn_c.last_response[0..conn_c.last_response_len],
    );

    const stats = reactor.stats();
    try std.testing.expectEqual(@as(u64, 1), stats.queue_full_total);
}

test "reactor emits QueueTimeout exactly at deadline before dispatch" {
    const Reactor = ReactorMod.ServerReactor(3, 64, 64);

    var dispatch_ctx = TraceDispatch{};
    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &TraceDispatch.dispatch,
        .cleanupSession = &TraceDispatch.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 1,
        .max_queued_requests = 3,
    });
    defer reactor.deinit();

    var conn_a = ScriptedConnection{ .request = "a0" };
    var conn_b = ScriptedConnection{ .request = "b1" };
    var conn_c = ScriptedConnection{ .request = "c2" };
    var conns = [_]Connection{ conn_a.connection(), conn_b.connection(), conn_c.connection() };
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    try reactor.step(acceptor.acceptor());
    clock.advance(1);
    try reactor.step(acceptor.acceptor());
    var polls: usize = 0;
    while (polls < 4096) : (polls += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        if (conn_a.writes == 1 and conn_b.writes == 1 and conn_c.writes == 1) break;
    }

    try std.testing.expectEqual(@as(usize, 1), dispatch_ctx.calls.load(.seq_cst));
    try std.testing.expectEqual(@as(usize, 1), conn_a.writes);
    try std.testing.expectEqualStrings("OK\n", conn_a.last_response[0..conn_a.last_response_len]);

    try std.testing.expectEqual(@as(usize, 1), conn_b.writes);
    try std.testing.expectEqualStrings(
        "ERR class=overload code=QueueTimeout\n",
        conn_b.last_response[0..conn_b.last_response_len],
    );
    try std.testing.expectEqual(@as(usize, 1), conn_c.writes);
    try std.testing.expectEqualStrings(
        "ERR class=overload code=QueueTimeout\n",
        conn_c.last_response[0..conn_c.last_response_len],
    );

    const stats = reactor.stats();
    try std.testing.expectEqual(@as(u64, 2), stats.queue_timeout_total);
}
