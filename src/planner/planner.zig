//! Deterministic query physical planner (WF07 phase 1 foundation).
const types = @import("types.zig");

pub fn planInitial(snapshot: *const types.PlannerInputSnapshot) !types.PhysicalDecisionSet {
    try snapshot.validate();

    var decisions = types.PhysicalDecisionSet{};
    decisions.streaming_mode = .disabled;
    decisions.streaming_reason = .STREAMING_DISABLED_RISK_UNBOUNDED;
    decisions.parallel_mode = if ((snapshot.feature_gate_mask & types.feature_gate_parallel_policy) != 0)
        .enabled
    else
        .sequential;
    for (snapshot.operator_sequence) |op| {
        switch (op) {
            .sort_op => {
                decisions.sort_strategy = .in_memory_merge;
                decisions.sort_reason = .SORT_IN_MEMORY_WITHIN_BUDGET;
            },
            .group_op => {
                decisions.group_strategy = .in_memory_linear;
                decisions.group_reason = .GROUP_LINEAR_WITHIN_GROUP_CAP;
            },
            else => {},
        }
    }
    return decisions;
}
