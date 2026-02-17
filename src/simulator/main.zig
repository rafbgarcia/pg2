const std = @import("std");

pub fn main() !void {
    const stdout = std.fs.File.stdout();

    var args = std.process.args();
    _ = args.skip(); // program name

    var seed: u64 = 0;
    var ticks: u64 = 100_000;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--seed")) {
            if (args.next()) |val| {
                seed = std.fmt.parseInt(u64, val, 10) catch {
                    try stdout.writeAll("invalid seed\n");
                    return;
                };
            }
        } else if (std.mem.eql(u8, arg, "--ticks")) {
            if (args.next()) |val| {
                ticks = std.fmt.parseInt(u64, val, 10) catch {
                    try stdout.writeAll("invalid ticks\n");
                    return;
                };
            }
        }
    }

    var buf: [128]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, "pg2 simulation — seed: {d}, ticks: {d}\n", .{ seed, ticks }) catch unreachable;
    try stdout.writeAll(msg);
    try stdout.writeAll("(no simulation logic yet)\n");
}
