//! Query physical planner contracts (WF07).
//!
//! Responsibilities in this file:
//! - Defines immutable planner snapshot and decision schemas.
//! - Defines stable reason-code and checkpoint vocabularies.
//! - Defines deterministic validation error codes for fail-closed planning.
const std = @import("std");

pub const snapshot_schema_version_current: u16 = 2;
pub const policy_version_current: u16 = 2;
pub const max_operator_sequence: usize = 32;
pub const max_relations: usize = 16;
pub const feature_gate_parallel_policy: u64 = 1 << 0;

pub const PlannerError = error{
    MissingSnapshotSchemaVersion,
    MissingPolicyVersion,
    MissingQueryShapeFingerprint,
    MissingCatalogSnapshotId,
    MissingRuntimeCountersSnapshotId,
    MissingCapacityProfileId,
    MissingWorkMemoryBudget,
    MissingAggregateGroupsCap,
    MissingJoinBuildBudgetBytes,
    MissingAverageRowWidthBytes,
    MissingMaxQuerySlots,
    InvalidRelationOrdering,
};

pub const OpTag = enum(u8) {
    none = 0,
    where_filter,
    having_filter,
    group_op,
    limit_op,
    offset_op,
    insert_op,
    update_op,
    delete_op,
    sort_op,
    inspect_op,
};

pub const JoinStrategy = enum(u8) {
    none = 0,
    hash_in_memory,
    hash_spill,
};

pub const JoinOrder = enum(u8) {
    none = 0,
    source_then_nested,
};

pub const MaterializationMode = enum(u8) {
    none = 0,
    bounded_row_buffers,
};

pub const SortStrategy = enum(u8) {
    none = 0,
    in_memory_merge,
    external_merge,
};

pub const GroupStrategy = enum(u8) {
    none = 0,
    in_memory_linear,
    hash_spill,
};

pub const StreamingMode = enum(u8) {
    disabled = 0,
    enabled,
};

pub const ParallelMode = enum(u8) {
    sequential = 0,
    enabled,
};

pub const Checkpoint = enum(u8) {
    pre_scan = 0,
    post_filter,
    pre_join,
    post_group,
};

pub const ReasonCode = enum(u16) {
    none = 0,
    JOIN_HASH_IN_MEMORY_CAPACITY_OK,
    JOIN_HASH_SPILL_RIGHT_EXCEEDS_BUILD_WINDOW,
    SORT_IN_MEMORY_WITHIN_BUDGET,
    SORT_EXTERNAL_REQUIRED_BY_ROWFLOW,
    GROUP_LINEAR_WITHIN_GROUP_CAP,
    GROUP_HASH_SPILL_GROUP_CAP_EXCEEDED,
    MATERIALIZE_BOUNDED_REQUIRED,
    STREAMING_ENABLED_SAFE,
    STREAMING_DISABLED_RISK_UNBOUNDED,
    PARALLEL_DISABLED_FEATURE_GATE,
    PARALLEL_DISABLED_INSUFFICIENT_QUERY_SLOTS,
    PARALLEL_ENABLED_QUERY_SLOT_BUDGETED,
    PARALLEL_DEGRADED_LOW_ROWFLOW,
    DEGRADE_MONOTONIC_GUARD,
};

pub const PlannerInputSnapshot = extern struct {
    snapshot_schema_version: u16 = snapshot_schema_version_current,
    policy_version: u16 = policy_version_current,
    seed: u64 = 0,
    query_shape_fingerprint: u128 = 0,
    catalog_snapshot_id: u64 = 0,
    runtime_counters_snapshot_id: u64 = 0,
    capacity_profile_id: u64 = 0,
    work_memory_bytes_per_slot: u64 = 0,
    aggregate_groups_cap: u32 = 0,
    join_build_budget_bytes: u64 = 0,
    average_row_width_bytes: u32 = 0,
    max_query_slots: u16 = 0,
    feature_gate_mask: u64 = 0,
    operator_sequence: [max_operator_sequence]OpTag = [_]OpTag{.none} ** max_operator_sequence,
    relation_ids_sorted: [max_relations]u32 = [_]u32{0} ** max_relations,

    pub fn validate(self: *const PlannerInputSnapshot) PlannerError!void {
        if (self.snapshot_schema_version == 0) return error.MissingSnapshotSchemaVersion;
        if (self.policy_version == 0) return error.MissingPolicyVersion;
        if (self.query_shape_fingerprint == 0) return error.MissingQueryShapeFingerprint;
        if (self.catalog_snapshot_id == 0) return error.MissingCatalogSnapshotId;
        if (self.runtime_counters_snapshot_id == 0) return error.MissingRuntimeCountersSnapshotId;
        if (self.capacity_profile_id == 0) return error.MissingCapacityProfileId;
        if (self.work_memory_bytes_per_slot == 0) return error.MissingWorkMemoryBudget;
        if (self.aggregate_groups_cap == 0) return error.MissingAggregateGroupsCap;
        if (self.join_build_budget_bytes == 0) return error.MissingJoinBuildBudgetBytes;
        if (self.average_row_width_bytes == 0) return error.MissingAverageRowWidthBytes;
        if (self.max_query_slots == 0) return error.MissingMaxQuerySlots;

        var previous: u32 = 0;
        var seen_zero_tail = false;
        for (self.relation_ids_sorted) |rid| {
            if (rid == 0) {
                seen_zero_tail = true;
                continue;
            }
            if (seen_zero_tail) return error.InvalidRelationOrdering;
            if (rid <= previous) return error.InvalidRelationOrdering;
            previous = rid;
        }
    }
};

pub const PhysicalDecisionSet = extern struct {
    join_strategy: JoinStrategy = .none,
    join_order: JoinOrder = .none,
    materialization_mode: MaterializationMode = .none,
    sort_strategy: SortStrategy = .none,
    group_strategy: GroupStrategy = .none,
    streaming_mode: StreamingMode = .disabled,
    parallel_mode: ParallelMode = .sequential,
    parallel_worker_budget: u8 = 1,

    join_reason: ReasonCode = .none,
    materialization_reason: ReasonCode = .none,
    sort_reason: ReasonCode = .none,
    group_reason: ReasonCode = .none,
    streaming_reason: ReasonCode = .none,
    parallel_reason: ReasonCode = .none,
};

pub const CheckpointCounters = extern struct {
    rows_seen: u64 = 0,
    rows_after_filter: u64 = 0,
    bytes_accumulated: u64 = 0,
    spill_pages_used: u32 = 0,
    group_count_estimate: u32 = 0,
    join_build_rows: u64 = 0,
    join_probe_rows: u64 = 0,
};

test "snapshot validation fails with deterministic error codes" {
    var snapshot = PlannerInputSnapshot{};
    try std.testing.expectError(
        error.MissingQueryShapeFingerprint,
        snapshot.validate(),
    );
}
