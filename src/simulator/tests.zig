const std = @import("std");
const pg2 = @import("pg2");

comptime {
    _ = pg2.simulator.fault_matrix;
    _ = pg2.simulator.fk_fault_matrix;
}

test "simulator test module compiles" {
    try std.testing.expect(true);
}
