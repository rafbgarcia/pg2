const std = @import("std");
const builtin = @import("builtin");
const runtime_config = @import("pg2").runtime.config;
const runtime_bootstrap = @import("pg2").runtime.bootstrap;
const session_mod = @import("pg2").server.session;
const io_uring_transport_mod = @import("pg2").server.io_uring_transport;
const catalog_mod = @import("pg2").catalog.meta;
const disk_mod = @import("pg2").simulator.disk;

pub fn main() !void {
    const stdout = std.fs.File.stdout();
    const allocator = std.heap.page_allocator;

    var args = std.process.args();
    _ = args.skip(); // program name

    var memory_bytes: usize = runtime_config.default_memory_bytes;
    var listen_addr: ?[]const u8 = null;

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
        } else if (std.mem.eql(u8, arg, "--listen")) {
            const raw = args.next() orelse {
                try stdout.writeAll("missing value for --listen\n");
                return;
            };
            listen_addr = raw;
        } else if (std.mem.eql(u8, arg, "--help")) {
            try stdout.writeAll(
                \\Usage: pg2 [--memory <bytes|MiB|GiB>] [--listen <host:port>]
                \\  --memory   Startup memory budget (default: 512MiB)
                \\  --listen   Start server accept loop (Linux-only target)
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

    if (listen_addr) |raw_listen_addr| {
        if (builtin.os.tag != .linux) {
            try stdout.writeAll(
                "server mode is Linux-only; use Docker to run dev/test on macOS\n",
            );
            return;
        }

        const listen_address = std.net.Address.parseIpAndPort(raw_listen_addr) catch {
            try stdout.writeAll("invalid --listen address (expected host:port)\n");
            return;
        };

        var catalog = catalog_mod.Catalog{};
        var session = session_mod.Session.init(&runtime, &catalog);
        const tx_id = runtime.tx_manager.begin() catch {
            try stdout.writeAll("startup failed: could not open session transaction\n");
            return;
        };
        var snapshot = runtime.tx_manager.snapshot(tx_id) catch {
            try stdout.writeAll("startup failed: could not create session snapshot\n");
            return;
        };
        defer snapshot.deinit();

        var request_buf: [4096]u8 = undefined;
        var response_buf: [4096]u8 = undefined;
        var io_uring_acceptor = io_uring_transport_mod.IoUringAcceptor.listen(
            listen_address,
            .{ .reuse_address = true },
        ) catch |err| switch (err) {
            error.IoUringUnavailable => {
                try stdout.writeAll(
                    "startup failed: io_uring unavailable on this Linux runtime\n",
                );
                return;
            },
            error.ListenFailed => {
                try stdout.writeAll("startup failed: could not listen on requested address\n");
                return;
            },
            error.UnsupportedPlatform => {
                try stdout.writeAll("startup failed: unsupported platform\n");
                return;
            },
        };
        defer io_uring_acceptor.deinit();

        try stdout.writeAll("server accept loop started (io_uring)\n");
        while (true) {
            const connection = io_uring_acceptor.acceptor().accept() catch {
                try stdout.writeAll("accept loop error: accept failed\n");
                continue;
            } orelse continue;

            session.serveConnection(
                connection,
                tx_id,
                &snapshot,
                request_buf[0..],
                response_buf[0..],
            ) catch {
                try stdout.writeAll("connection error: request handling failed\n");
            };
        }
    }
}
