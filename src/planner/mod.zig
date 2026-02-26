//! Query planner module surface (WF07).
pub const types = @import("types.zig");
pub const fingerprint = @import("fingerprint.zig");
pub const planner = @import("planner.zig");
pub const adaptation = @import("adaptation.zig");
pub const parallel = @import("parallel.zig");

test {
    _ = types;
    _ = fingerprint;
    _ = planner;
    _ = adaptation;
    _ = parallel;
}
