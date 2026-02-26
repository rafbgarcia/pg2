//! Advisor module surface.
pub const metrics = @import("metrics.zig");
pub const rules = @import("rules.zig");
pub const sink = @import("sink.zig");

comptime {
    _ = metrics;
    _ = rules;
    _ = sink;
}
