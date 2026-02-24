//! Internal robustness suite entrypoint.
//!
//! Responsibilities in this file:
//! - Aggregates non-user-facing durability and maintenance tests.
//! - Keeps internal correctness coverage discoverable outside feature specs.
comptime {
    _ = @import("integer_multi_column_matrix_test.zig");
    _ = @import("durability/overflow_reclaim_crash_matrix_test.zig");
    _ = @import("durability/overflow_reclaim_replay_test.zig");
    _ = @import("durability/overflow_replay_tx_markers_test.zig");
    _ = @import("maintenance/overflow_reclaim_drain_policy_test.zig");
    _ = @import("maintenance/overflow_reclaim_surface_test.zig");
    _ = @import("query_protocol/tree_response_test.zig");
    _ = @import("server/transport_progress_test.zig");
    _ = @import("server/reactor_queueing_test.zig");
    _ = @import("spill/temp_storage_surface_test.zig");
    _ = @import("spill/temp_spill_determinism_test.zig");
    _ = @import("spill/nested_parent_overflow_contract_test.zig");
}
