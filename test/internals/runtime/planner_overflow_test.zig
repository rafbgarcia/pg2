//! Internal startup planner overflow handling checks.
//!
//! Responsibilities in this file:
//! - Verifies ratio-derived capacity math fails closed on integer overflow.
//! - Prevents startup-path panics/wraparound for extreme memory envelopes.
const std = @import("std");
const pg2 = @import("pg2");
const planner = pg2.runtime.capacity_planner;

test "planner returns Overflow when wal ratio multiplication overflows" {
    const total = std.math.maxInt(usize);
    const permilles = [_]u16{ 2, 17, 997, 2048, 65535 };

    for (permilles) |permille| {
        _ = std.math.mul(usize, total, permille) catch |err| switch (err) {
            error.Overflow => {
                const policy: planner.PlannerPolicy = .{
                    .shared_buffer_pool_ratio_permille = 0,
                    .shared_wal_ratio_permille = permille,
                    .shared_undo_ratio_permille = 0,
                    .min_wal_buffer_capacity_bytes = 0,
                    .min_undo_data_bytes = 0,
                };
                try std.testing.expectError(
                    error.Overflow,
                    planner.planWithPolicy(total, 1, policy),
                );
                continue;
            },
        };
    }
}
