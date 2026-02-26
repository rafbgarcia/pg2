//! Deterministic query physical planner (WF07 phase 1 foundation).
const types = @import("types.zig");

pub fn planInitial(snapshot: *const types.PlannerInputSnapshot) !types.PhysicalDecisionSet {
    try snapshot.validate();

    var decisions = types.PhysicalDecisionSet{};
    decisions.streaming_mode = .disabled;
    decisions.streaming_reason = .STREAMING_DISABLED_RISK_UNBOUNDED;
    const parallel_enabled = (snapshot.feature_gate_mask & types.feature_gate_parallel_policy) != 0;
    if (!parallel_enabled) {
        decisions.parallel_mode = .sequential;
        decisions.parallel_worker_budget = 1;
        decisions.parallel_reason = .PARALLEL_DISABLED_FEATURE_GATE;
    } else if (snapshot.max_query_slots < 2) {
        decisions.parallel_mode = .sequential;
        decisions.parallel_worker_budget = 1;
        decisions.parallel_reason = .PARALLEL_DISABLED_INSUFFICIENT_QUERY_SLOTS;
    } else {
        const budget = @min(@as(u16, 8), snapshot.max_query_slots);
        decisions.parallel_mode = .enabled;
        decisions.parallel_worker_budget = @intCast(budget);
        decisions.parallel_reason = .PARALLEL_ENABLED_QUERY_SLOT_BUDGETED;
    }

    const has_join = snapshot.relation_ids_sorted[1] != 0;
    if (has_join) {
        decisions.join_strategy = .hash_in_memory;
        decisions.join_order = .source_then_nested;
        decisions.materialization_mode = .bounded_row_buffers;
        decisions.join_reason = .JOIN_HASH_IN_MEMORY_CAPACITY_OK;
        decisions.materialization_reason = .MATERIALIZE_BOUNDED_REQUIRED;
    }

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
