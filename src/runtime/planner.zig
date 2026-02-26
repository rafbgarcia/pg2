//! Compatibility wrapper for startup capacity planning.
//!
//! Responsibilities in this file:
//! - Preserves existing `runtime.planner` callsites while migration is in flight.
//! - Re-exports the startup capacity planner API from `capacity_planner.zig`.
//! - Avoids duplicate tests from importing full implementation twice.
const capacity_planner = @import("capacity_planner.zig");

pub const PlannerError = capacity_planner.PlannerError;
pub const PlannerPolicy = capacity_planner.PlannerPolicy;
pub const Plan = capacity_planner.Plan;

pub fn planFromMemory(memory_budget_bytes: usize, detected_vcpus: u16) PlannerError!Plan {
    return capacity_planner.planFromMemory(memory_budget_bytes, detected_vcpus);
}

pub fn planWithPolicy(
    memory_budget_bytes: usize,
    detected_vcpus: u16,
    policy: PlannerPolicy,
) PlannerError!Plan {
    return capacity_planner.planWithPolicy(memory_budget_bytes, detected_vcpus, policy);
}
