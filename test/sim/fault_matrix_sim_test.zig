//! Deterministic simulation matrix gate.
const pg2 = @import("pg2");

const fault_matrix = pg2.simulator.fault_matrix;

test "sim deterministic replay short schedules" {
    try fault_matrix.assertCiShortDeterminism();
}

test "sim deterministic replay long schedules" {
    try fault_matrix.assertCiLongDeterminism();
}
