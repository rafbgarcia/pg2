//! pg2 process entrypoint and server bootstrap loop.
//!
//! Responsibilities in this file:
//! - Parses CLI flags (`--memory`, `--listen`) and validates startup args.
//! - Bootstraps runtime/canonical in-memory catalog for local process use.
//! - Starts the server accept loop when listen mode is requested.
//! - Emits user-facing startup/failure messages at process boundary.
const std = @import("std");
const builtin = @import("builtin");
const runtime_config = @import("pg2").runtime.config;
const runtime_planner = @import("pg2").runtime.planner;
const runtime_bootstrap = @import("pg2").runtime.bootstrap;
const session_mod = @import("pg2").server.session;
const pool_mod = @import("pg2").server.pool;
const diagnostics_mod = @import("pg2").server.diagnostics;
const reactor_mod = @import("pg2").server.reactor;
const io_uring_transport_mod = @import("pg2").server.io_uring_transport;
const catalog_mod = @import("pg2").catalog.meta;
const disk_mod = @import("pg2").simulator.disk;
const io_mod = @import("pg2").storage.io;

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
                \\  --memory       Startup memory budget (default: 512MiB)
                \\  --listen       Start server accept loop (Linux-only target)
                \\
            );
            return;
        } else {
            try stdout.writeAll("unknown argument\n");
            return;
        }
    }

    var disk = disk_mod.SimulatedDisk.init(allocator);
    defer disk.deinit();

    const detected_vcpus = runtime_config.detectVcpus();
    const plan = runtime_planner.planFromMemory(memory_bytes, detected_vcpus) catch |err| switch (err) {
        error.InvalidInput => {
            try stdout.writeAll("startup failed: invalid startup planning inputs\n");
            return;
        },
        error.InsufficientMemoryBudget => {
            try stdout.writeAll("startup failed: memory budget below minimum runtime footprint\n");
            return;
        },
        error.Overflow => {
            try stdout.writeAll("startup failed: planning overflow\n");
            return;
        },
    };
    const required_bytes = runtime_bootstrap.requiredBytesForConfig(
        allocator,
        disk.storage(),
        plan.bootstrap,
    ) catch |err| switch (err) {
        error.InsufficientMemoryBudget => {
            try stdout.writeAll("startup failed: could not compute admission minimum\n");
            return;
        },
        error.InvalidConfig => {
            try stdout.writeAll("startup failed: invalid planned runtime configuration\n");
            return;
        },
        error.OutOfMemory => {
            try stdout.writeAll("startup failed: admission probe allocation failed\n");
            return;
        },
    };
    if (memory_bytes < required_bytes) {
        var admission_buf: [192]u8 = undefined;
        const admission_msg = std.fmt.bufPrint(
            &admission_buf,
            "startup failed: insufficient memory budget (required: {d} bytes, provided: {d} bytes)\n",
            .{ required_bytes, memory_bytes },
        ) catch {
            try stdout.writeAll("startup failed: insufficient memory budget\n");
            return;
        };
        try stdout.writeAll(admission_msg);
        return;
    }

    const memory_region = allocator.alloc(u8, memory_bytes) catch {
        try stdout.writeAll("startup failed: could not allocate memory region\n");
        return;
    };
    defer allocator.free(memory_region);

    var runtime = runtime_bootstrap.BootstrappedRuntime.init(
        memory_region,
        disk.storage(),
        plan.bootstrap,
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
        "pg2 — runtime bootstrapped (memory: {d} bytes, vcpus: {d}, slots: {d})\n",
        .{ memory_bytes, detected_vcpus, runtime.max_query_slots },
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
        var pool = pool_mod.ConnectionPool.initWithConfig(&runtime, .{
            .overload_policy = .queue,
        });

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

        const Reactor = reactor_mod.ServerReactor(256, 4096, 4096);
        const DispatchCtx = struct {
            session: *session_mod.Session,
            pool: *pool_mod.ConnectionPool,
            pin_states: [256]session_mod.SessionPinState =
                [_]session_mod.SessionPinState{.{}} ** 256,

            fn dispatch(
                ptr: *anyopaque,
                session_id: u16,
                request: []const u8,
                runtime_inspect_stats: diagnostics_mod.RuntimeInspectStats,
                out: []u8,
            ) session_mod.SessionError!reactor_mod.Dispatcher.DispatchResult {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const result = try self.session.dispatchRequestForSession(
                    self.pool,
                    &self.pin_states[session_id],
                    request,
                    runtime_inspect_stats,
                    out,
                );
                return .{
                    .response_len = result.bytes_written,
                    .pin_transition = result.pin_transition,
                };
            }

            fn cleanupSession(ptr: *anyopaque, session_id: u16) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.session.cleanupPinnedSession(
                    self.pool,
                    &self.pin_states[session_id],
                );
            }
        };
        var dispatch_ctx = DispatchCtx{
            .session = &session,
            .pool = &pool,
        };
        var clock = io_mod.RealClock{};
        var reactor = Reactor.init(.{
            .ctx = &dispatch_ctx,
            .dispatch = &DispatchCtx.dispatch,
            .cleanupSession = &DispatchCtx.cleanupSession,
        }, .{
            .clock = clock.clock(),
            .max_inflight = runtime.max_query_slots,
        });
        defer reactor.deinit();

        try stdout.writeAll("server accept loop started (io_uring)\n");
        while (true) {
            reactor.step(io_uring_acceptor.acceptor()) catch {
                try stdout.writeAll("accept loop error: accept failed\n");
                continue;
            };
        }
    }
}
