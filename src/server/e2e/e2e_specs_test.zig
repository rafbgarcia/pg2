//! Server session-path E2E test suite entrypoint.
//!
//! Responsibilities in this file:
//! - Aggregates all server E2E spec modules for test discovery.
//! - Defines the canonical milestone-focused E2E scope under one import root.
comptime {
    _ = @import("constraints/default_values_test.zig");
    _ = @import("constraints/duplicate_key_test.zig");
    _ = @import("constraints/foreign_key_violation_test.zig");
    _ = @import("constraints/nullable_test.zig");
    _ = @import("constraints/not_null_test.zig");
    _ = @import("constraints/type_mismatch_test.zig");
    _ = @import("constraints/unknown_field_test.zig");
    _ = @import("field_types/bigint_test.zig");
    _ = @import("field_types/boolean_test.zig");
    _ = @import("field_types/string_test.zig");
    _ = @import("mutations/delete_test.zig");
    _ = @import("mutations/insert_test.zig");
    _ = @import("overflow_reclaim_crash_matrix_test.zig");
    _ = @import("overflow_reclaim_drain_policy_test.zig");
    _ = @import("overflow_replay_tx_markers_test.zig");
    _ = @import("string_overflow_test.zig");
    _ = @import("queries/select_test.zig");
    _ = @import("mutations/update_test.zig");
}
