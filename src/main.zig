const std = @import("std");
const runtime_config = @import("pg2").runtime.config;

pub fn main() !void {
    const stdout = std.fs.File.stdout();

    var args = std.process.args();
    _ = args.skip(); // program name

    var memory_bytes: usize = runtime_config.default_memory_bytes;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--memory")) {
            const raw = args.next() orelse {
                try stdout.writeAll("missing value for --memory\n");
                return;
            };
            memory_bytes = runtime_config.parseMemoryBytes(raw) catch {
                try stdout.writeAll("invalid --memory value\n");
                return;
            };
        } else if (std.mem.eql(u8, arg, "--help")) {
            try stdout.writeAll(
                \\Usage: pg2 [--memory <bytes|MiB|GiB>]
                \\  --memory   Startup memory budget (default: 512MiB)
                \\
            );
            return;
        } else {
            try stdout.writeAll("unknown argument\n");
            return;
        }
    }

    var buf: [128]u8 = undefined;
    const msg = std.fmt.bufPrint(
        &buf,
        "pg2 — experimental database (memory: {d} bytes)\n",
        .{memory_bytes},
    ) catch {
        try stdout.writeAll("startup banner format overflow\n");
        return;
    };
    try stdout.writeAll(msg);
}
