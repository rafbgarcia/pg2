//! Server session-path E2E test suite entrypoint.
//!
//! Responsibilities in this file:
//! - Aggregates all server E2E spec modules for test discovery.
//! - Defines the canonical milestone-focused E2E scope under one import root.
comptime {
    _ = @import("constraints/default_values.zig");
    _ = @import("constraints/duplicate_key.zig");
    _ = @import("constraints/foreign_key_violation.zig");
    _ = @import("constraints/nullable.zig");
    _ = @import("constraints/not_null.zig");
    _ = @import("constraints/type_mismatch.zig");
    _ = @import("constraints/unknown_field.zig");
    _ = @import("field_types/bigint.zig");
    _ = @import("field_types/boolean.zig");
    _ = @import("field_types/string.zig");
    _ = @import("mutations/delete.zig");
    _ = @import("mutations/insert.zig");
    _ = @import("overflow_reclaim_crash_matrix.zig");
    _ = @import("overflow_reclaim_drain_policy.zig");
    _ = @import("overflow_replay_tx_markers.zig");
    _ = @import("string_overflow.zig");
    _ = @import("queries/select.zig");
    _ = @import("mutations/update.zig");
}
