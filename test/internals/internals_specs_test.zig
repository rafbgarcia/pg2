//! Internal robustness suite entrypoint.
//!
//! Responsibilities in this file:
//! - Aggregates non-user-facing durability and maintenance tests.
//! - Keeps internal correctness coverage discoverable outside feature specs.
comptime {
    _ = @import("durability/overflow_reclaim_crash_matrix_test.zig");
    _ = @import("durability/overflow_replay_tx_markers_test.zig");
    _ = @import("maintenance/overflow_reclaim_drain_policy_test.zig");
}
