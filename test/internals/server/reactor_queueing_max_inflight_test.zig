//! Deterministic max_inflight ordering contracts for server reactor.
const std = @import("std");
const helper = @import("reactor_queueing_helpers.zig");

const ReactorMod = helper.reactor_mod;
const Connection = helper.Connection;
const ManualClock = helper.ManualClock;
const ScriptedConnection = helper.ScriptedConnection;
const TestAcceptor = helper.TestAcceptor;
const MultiGateDispatch = helper.MultiGateDispatch;

test "reactor preserves deterministic mixed completion ordering with max_inflight=2" {
    const Reactor = ReactorMod.ServerReactor(3, 64, 64);

    var dispatch_ctx = MultiGateDispatch{};
    defer dispatch_ctx.releaseAll();
    var clock = ManualClock{};
    var reactor = Reactor.init(.{
        .ctx = &dispatch_ctx,
        .dispatch = &MultiGateDispatch.dispatch,
        .cleanupSession = &MultiGateDispatch.cleanupSession,
    }, .{
        .clock = clock.clock(),
        .queue_timeout_ticks = 100,
        .max_queued_requests = 3,
        .max_inflight = 2,
    });
    defer reactor.deinit();

    var conn_a = ScriptedConnection{ .request = "a0" };
    var conn_b = ScriptedConnection{ .request = "b1" };
    var conn_c = ScriptedConnection{ .request = "c2" };
    var conns = [_]Connection{
        conn_a.connection(),
        conn_b.connection(),
        conn_c.connection(),
    };
    var acceptor = TestAcceptor{ .connections = conns[0..] };

    var spins: usize = 0;
    while (spins < 128) : (spins += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        if (dispatch_ctx.calls.load(.seq_cst) == 2) break;
    }

    const started_stats = reactor.stats();
    try std.testing.expectEqual(@as(usize, 2), started_stats.workers_busy);
    try std.testing.expectEqual(@as(usize, 0), conn_a.writes);
    try std.testing.expectEqual(@as(usize, 0), conn_b.writes);
    try std.testing.expectEqual(@as(usize, 0), conn_c.writes);

    try dispatch_ctx.release('1');

    spins = 0;
    while (spins < 128) : (spins += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        if (conn_b.writes == 1) break;
    }
    try std.testing.expectEqual(@as(usize, 1), conn_b.writes);
    try std.testing.expectEqual(@as(usize, 0), conn_a.writes);
    try std.testing.expectEqual(@as(usize, 0), conn_c.writes);

    try dispatch_ctx.release('0');
    spins = 0;
    while (spins < 128) : (spins += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        if (conn_a.writes == 1 and dispatch_ctx.calls.load(.seq_cst) == 3) break;
    }
    try std.testing.expectEqual(@as(usize, 1), conn_a.writes);
    try std.testing.expectEqual(@as(usize, 0), conn_c.writes);

    try dispatch_ctx.release('2');
    spins = 0;
    while (spins < 128) : (spins += 1) {
        try reactor.step(acceptor.acceptor());
        std.Thread.yield() catch {};
        if (conn_c.writes == 1) break;
    }

    try std.testing.expectEqual(@as(usize, 3), dispatch_ctx.calls.load(.seq_cst));
    try std.testing.expectEqual(@as(usize, 3), dispatch_ctx.dispatch_order_len);
    try std.testing.expectEqual(@as(u8, '0'), dispatch_ctx.dispatch_order[0]);
    try std.testing.expectEqual(@as(u8, '1'), dispatch_ctx.dispatch_order[1]);
    try std.testing.expectEqual(@as(u8, '2'), dispatch_ctx.dispatch_order[2]);

    try std.testing.expectEqual(@as(usize, 3), dispatch_ctx.completion_order_len);
    try std.testing.expectEqual(@as(u8, '1'), dispatch_ctx.completion_order[0]);
    try std.testing.expectEqual(@as(u8, '0'), dispatch_ctx.completion_order[1]);
    try std.testing.expectEqual(@as(u8, '2'), dispatch_ctx.completion_order[2]);
}
