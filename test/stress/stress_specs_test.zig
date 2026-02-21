//! Stress test suite entrypoint.
//!
//! Responsibilities in this file:
//! - Aggregates intentionally heavy scenarios that are excluded from default `zig build test`.
//! - Provides an opt-in root for stress/performance-oriented validation.
comptime {
    _ = @import("mutations/stress_mutations_test.zig");
}
