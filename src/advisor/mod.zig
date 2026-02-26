//! Advisor module surface.
pub const metrics = @import("metrics.zig");
pub const rules = @import("rules.zig");

comptime {
    _ = metrics;
    _ = rules;
}
