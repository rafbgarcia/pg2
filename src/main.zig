const std = @import("std");

pub fn main() !void {
    try std.fs.File.stdout().writeAll("pg2 — experimental database\n");
}
