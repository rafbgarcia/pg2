const std = @import("std");
const runtime_config = @import("pg2").runtime.config;
const runtime_bootstrap = @import("pg2").runtime.bootstrap;
const disk_mod = @import("pg2").simulator.disk;

pub fn main() !void {
    const stdout = std.fs.File.stdout();
    const allocator = std.heap.page_allocator;

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

    const memory_region = allocator.alloc(u8, memory_bytes) catch {
        try stdout.writeAll("startup failed: could not allocate memory region\n");
        return;
    };
    defer allocator.free(memory_region);

    var disk = disk_mod.SimulatedDisk.init(allocator);
    defer disk.deinit();

    var runtime = runtime_bootstrap.BootstrappedRuntime.init(
        memory_region,
        disk.storage(),
        .{},
    ) catch |err| switch (err) {
        error.InsufficientMemoryBudget => {
            try stdout.writeAll(
                "startup failed: insufficient memory budget for runtime bootstrap\n",
            );
            return;
        },
        error.InvalidConfig => {
            try stdout.writeAll("startup failed: invalid runtime configuration\n");
            return;
        },
        error.OutOfMemory => {
            try stdout.writeAll("startup failed: bootstrap allocation failed\n");
            return;
        },
    };
    defer runtime.deinit();

    var buf: [160]u8 = undefined;
    const msg = std.fmt.bufPrint(
        &buf,
        "pg2 — runtime bootstrapped (memory: {d} bytes)\n",
        .{memory_bytes},
    ) catch {
        try stdout.writeAll("startup banner format overflow\n");
        return;
    };
    try stdout.writeAll(msg);
}
