const std = @import("std");
const pg2 = @import("pg2");

const BootstrappedRuntime = pg2.runtime.bootstrap.BootstrappedRuntime;
const ConnectionPool = pg2.server.pool.ConnectionPool;
const disk_mod = pg2.simulator.disk;

const RuntimeFixture = struct {
    disk: *disk_mod.SimulatedDisk,
    backing_memory: []u8,
    runtime: BootstrappedRuntime,

    fn init(max_query_slots: u16) !RuntimeFixture {
        const disk = try std.testing.allocator.create(disk_mod.SimulatedDisk);
        errdefer std.testing.allocator.destroy(disk);
        disk.* = disk_mod.SimulatedDisk.init(std.testing.allocator);
        errdefer disk.deinit();

        const backing_memory = try std.testing.allocator.alloc(
            u8,
            256 * 1024 * 1024,
        );
        errdefer std.testing.allocator.free(backing_memory);

        const runtime = try BootstrappedRuntime.init(
            backing_memory,
            disk.storage(),
            .{ .max_query_slots = max_query_slots },
        );

        return .{
            .disk = disk,
            .backing_memory = backing_memory,
            .runtime = runtime,
        };
    }

    fn deinit(self: *RuntimeFixture) void {
        self.runtime.deinit();
        std.testing.allocator.free(self.backing_memory);
        self.disk.deinit();
        std.testing.allocator.destroy(self.disk);
    }
};

test "checkout returns pool connection with active transaction and snapshot" {
    var fixture = try RuntimeFixture.init(1);
    defer fixture.deinit();

    var pool = ConnectionPool.init(&fixture.runtime);
    var conn = try pool.checkout();
    defer pool.checkin(&conn) catch {};

    try std.testing.expectEqual(@as(u16, 0), conn.slot_index);
    try std.testing.expect(conn.checked_out);
    try std.testing.expect(!conn.pinned);
    try std.testing.expect(conn.snapshot.tx_id == conn.tx_id);
    try std.testing.expect(fixture.runtime.tx_manager.getState(conn.tx_id).? == .active);
}

test "checkin releases slot for reuse" {
    var fixture = try RuntimeFixture.init(1);
    defer fixture.deinit();

    var pool = ConnectionPool.init(&fixture.runtime);
    var first = try pool.checkout();
    try pool.checkin(&first);

    var second = try pool.checkout();
    defer pool.checkin(&second) catch {};
    try std.testing.expectEqual(@as(u16, 0), second.slot_index);
}

test "abortCheckin aborts transaction and releases slot for reuse" {
    var fixture = try RuntimeFixture.init(1);
    defer fixture.deinit();

    var pool = ConnectionPool.init(&fixture.runtime);
    var first = try pool.checkout();
    const tx = first.tx_id;
    try pool.abortCheckin(&first);

    // abortCheckin performs cleanup, so finalized tx state may be compacted.
    const tx_state = fixture.runtime.tx_manager.getState(tx) orelse
        return error.TestUnexpectedResult;
    try std.testing.expect(tx_state != .active);

    var second = try pool.checkout();
    defer pool.checkin(&second) catch {};
    try std.testing.expectEqual(@as(u16, 0), second.slot_index);
}

test "checkout returns PoolExhausted when all slots are occupied" {
    var fixture = try RuntimeFixture.init(1);
    defer fixture.deinit();

    var pool = ConnectionPool.init(&fixture.runtime);
    var held = try pool.checkout();
    defer pool.checkin(&held) catch {};

    try std.testing.expectError(error.PoolExhausted, pool.checkout());
    try std.testing.expectEqual(@as(u64, 1), pool.pool_exhausted_total);
}

test "queue overload policy waits then times out when pool stays exhausted" {
    var fixture = try RuntimeFixture.init(1);
    defer fixture.deinit();

    var pool = ConnectionPool.initWithConfig(&fixture.runtime, .{
        .overload_policy = .queue,
        .queue_wait_spins = 8,
    });
    var held = try pool.checkout();
    defer pool.checkin(&held) catch {};

    try std.testing.expectError(
        error.QueueTimeout,
        pool.checkout(),
    );
    try std.testing.expectEqual(@as(u64, 1), pool.pool_exhausted_total);
}

test "double checkin returns InvalidPoolConn" {
    var fixture = try RuntimeFixture.init(1);
    defer fixture.deinit();

    var pool = ConnectionPool.init(&fixture.runtime);
    var conn = try pool.checkout();
    try pool.checkin(&conn);

    try std.testing.expectError(error.InvalidPoolConn, pool.checkin(&conn));
}

test "pin and unpin keeps lease until unpin checkin" {
    var fixture = try RuntimeFixture.init(1);
    defer fixture.deinit();

    var pool = ConnectionPool.init(&fixture.runtime);
    var conn = try pool.checkout();
    try pool.pin(&conn);
    try std.testing.expectError(error.PoolConnPinned, pool.checkin(&conn));

    try pool.unpin(&conn);
    try std.testing.expectError(error.InvalidPoolConn, pool.checkin(&conn));
}

test "rollbackPinned aborts pinned lease and releases slot" {
    var fixture = try RuntimeFixture.init(1);
    defer fixture.deinit();

    var pool = ConnectionPool.init(&fixture.runtime);
    var conn = try pool.checkout();
    const tx_id = conn.tx_id;
    try pool.pin(&conn);
    try pool.rollbackPinned(&conn);

    // rollbackPinned delegates to abortCheckin and may compact tx state.
    const tx_state = fixture.runtime.tx_manager.getState(tx_id) orelse
        return error.TestUnexpectedResult;
    try std.testing.expect(tx_state != .active);
    try std.testing.expectEqual(@as(u16, 0), pool.snapshotStats().checked_out);
    try std.testing.expectEqual(@as(u16, 0), pool.snapshotStats().pinned);

    var reused = try pool.checkout();
    defer pool.checkin(&reused) catch {};
    try std.testing.expectEqual(@as(u16, 0), reused.slot_index);
}

test "snapshotStats tracks checkout pin and checkin counters" {
    var fixture = try RuntimeFixture.init(2);
    defer fixture.deinit();

    var pool = ConnectionPool.init(&fixture.runtime);
    const initial = pool.snapshotStats();
    try std.testing.expectEqual(@as(u16, 2), initial.pool_size);
    try std.testing.expectEqual(@as(u16, 0), initial.checked_out);
    try std.testing.expectEqual(@as(u16, 0), initial.pinned);

    var conn = try pool.checkout();
    const checked_out = pool.snapshotStats();
    try std.testing.expectEqual(@as(u16, 1), checked_out.checked_out);
    try std.testing.expectEqual(@as(u16, 0), checked_out.pinned);

    try pool.pin(&conn);
    const pinned = pool.snapshotStats();
    try std.testing.expectEqual(@as(u16, 1), pinned.checked_out);
    try std.testing.expectEqual(@as(u16, 1), pinned.pinned);

    try pool.unpin(&conn);
    const released = pool.snapshotStats();
    try std.testing.expectEqual(@as(u16, 0), released.checked_out);
    try std.testing.expectEqual(@as(u16, 0), released.pinned);
}
