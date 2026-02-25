//! Deterministic simulation matrix gate for foreign-key workloads.
const pg2 = @import("pg2");

const fk_fault_matrix = pg2.simulator.fk_fault_matrix;

test "sim deterministic foreign-key crash/restart matrix" {
    try fk_fault_matrix.assertSeedSetDeterminism();
}
