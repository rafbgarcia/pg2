const io = @import("../storage/io.zig");

/// Deterministic clock for simulation. Time advances only when
/// explicitly told to by the scheduler.
pub const SimulatedClock = struct {
    current_tick: u64 = 0,

    pub fn clock(self: *SimulatedClock) io.Clock {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    const vtable = io.Clock.VTable{
        .now = &nowImpl,
    };

    fn nowImpl(ptr: *anyopaque) u64 {
        const self: *SimulatedClock = @ptrCast(@alignCast(ptr));
        return self.current_tick;
    }

    pub fn advance(self: *SimulatedClock, ticks: u64) void {
        self.current_tick += ticks;
    }

    pub fn set(self: *SimulatedClock, tick: u64) void {
        self.current_tick = tick;
    }
};

const std = @import("std");

test "SimulatedClock starts at zero" {
    var sc = SimulatedClock{};
    const c = sc.clock();
    try std.testing.expectEqual(@as(u64, 0), c.now());
}

test "SimulatedClock advances" {
    var sc = SimulatedClock{};
    const c = sc.clock();
    sc.advance(100);
    try std.testing.expectEqual(@as(u64, 100), c.now());
    sc.advance(50);
    try std.testing.expectEqual(@as(u64, 150), c.now());
}

test "SimulatedClock set" {
    var sc = SimulatedClock{};
    const c = sc.clock();
    sc.set(999);
    try std.testing.expectEqual(@as(u64, 999), c.now());
}
