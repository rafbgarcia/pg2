//! Deterministic scheduler fairness/progress contracts for server reactor.
const std = @import("std");
const helper = @import("reactor_queueing_helpers.zig");

const ReactorMod = helper.reactor_mod;
const Connection = helper.Connection;
const ManualClock = helper.ManualClock;
const ScriptedConnection = helper.ScriptedConnection;
const TestAcceptor = helper.TestAcceptor;
const TraceDispatch = helper.TraceDispatch;
const BlockingDispatch = helper.BlockingDispatch;

test "reactor dispatches queued sessions in round-robin fair order" {
    const Reactor = ReactorMod.ServerReactor(4, 64, 64);

    var dispatch_ctx = TraceDispatch{};
    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &TraceDispatch.dispatch,
        .cleanupSession = &TraceDispatch.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 100,
        .max_queued_requests = 4,
    });
    defer reactor.deinit();

    var conn_a = ScriptedConnection{ .request = "a0" };
    var conn_b = ScriptedConnection{ .request = "b1" };
    var conn_c = ScriptedConnection{ .request = "c2" };
    var conn_d = ScriptedConnection{ .request = "d3" };
    var conns = [_]Connection{
        conn_a.connection(),
        conn_b.connection(),
        conn_c.connection(),
        conn_d.connection(),
    };
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    var steps: usize = 0;
    while (steps < 128) : (steps += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        clock.advance(1);
        if (dispatch_ctx.calls.load(.seq_cst) == 4) break;
    }

    try std.testing.expectEqual(@as(usize, 4), dispatch_ctx.calls.load(.seq_cst));
    try std.testing.expectEqual(@as(u8, '0'), dispatch_ctx.order[0]);
    try std.testing.expectEqual(@as(u8, '1'), dispatch_ctx.order[1]);
    try std.testing.expectEqual(@as(u8, '2'), dispatch_ctx.order[2]);
    try std.testing.expectEqual(@as(u8, '3'), dispatch_ctx.order[3]);
}

test "reactor keeps progressing reads timeouts and writes while worker is busy" {
    const Reactor = ReactorMod.ServerReactor(2, 64, 64);

    var dispatch_ctx = BlockingDispatch{};
    defer dispatch_ctx.unblock();
    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &BlockingDispatch.dispatch,
        .cleanupSession = &BlockingDispatch.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 2,
        .max_queued_requests = 2,
    });
    defer reactor.deinit();

    var conn_a = ScriptedConnection{ .request = "a0" };
    var conn_b = ScriptedConnection{ .request = "b1" };
    var conns = [_]Connection{ conn_a.connection(), conn_b.connection() };
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    try reactor.step(acceptor.acceptor());
    const after_first = reactor.stats();
    try std.testing.expectEqual(@as(usize, 1), after_first.workers_busy);

    clock.advance(1);
    try reactor.step(acceptor.acceptor());
    const before_timeout = reactor.stats();
    try std.testing.expectEqual(@as(usize, 1), before_timeout.workers_busy);

    clock.advance(1);
    try reactor.step(acceptor.acceptor());
    try std.testing.expectEqual(@as(usize, 1), conn_b.writes);
    try std.testing.expectEqualStrings(
        "ERR class=overload code=QueueTimeout\n",
        conn_b.last_response[0..conn_b.last_response_len],
    );

    dispatch_ctx.unblock();
    var polls: usize = 0;
    while (polls < 64) : (polls += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        if (conn_a.writes == 1) break;
    }

    try std.testing.expectEqual(@as(usize, 1), dispatch_ctx.calls.load(.seq_cst));
    try std.testing.expectEqual(@as(usize, 1), conn_a.writes);
    try std.testing.expectEqualStrings("OK\n", conn_a.last_response[0..conn_a.last_response_len]);
}
