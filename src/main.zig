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
const runtime_capacity_planner = @import("pg2").runtime.capacity_planner;
const runtime_bootstrap = @import("pg2").runtime.bootstrap;
const runtime_storage_root_mod = @import("pg2").runtime.storage_root;
const session_mod = @import("pg2").server.session;
const pool_mod = @import("pg2").server.pool;
const diagnostics_mod = @import("pg2").server.diagnostics;
const reactor_mod = @import("pg2").server.reactor;
const io_uring_transport_mod = @import("pg2").server.io_uring_transport;
const advisor_mod = @import("pg2").advisor;
const catalog_mod = @import("pg2").catalog.meta;
const io_mod = @import("pg2").storage.io;

pub fn main() !void {
    const stdout = std.fs.File.stdout();
    const allocator = std.heap.page_allocator;

    var args = std.process.args();
    _ = args.skip(); // program name

    var argv = std.ArrayList([]const u8){};
    defer argv.deinit(allocator);
    while (args.next()) |arg| {
        argv.append(allocator, arg) catch {
            try stdout.writeAll("startup failed: argument parsing allocation failed\n");
            return;
        };
    }

    if (argv.items.len > 0 and std.mem.eql(u8, argv.items[0], "lock")) {
        try handleLockCommand(stdout, argv.items[1..]);
        return;
    }
    if (argv.items.len > 0 and std.mem.eql(u8, argv.items[0], "inspect")) {
        try handleInspectCommand(stdout, argv.items[1..]);
        return;
    }
    if (argv.items.len > 0 and std.mem.eql(u8, argv.items[0], "advise")) {
        try handleAdviseCommand(stdout, argv.items[1..]);
        return;
    }

    var memory_bytes: usize = runtime_config.default_memory_bytes;
    var listen_addr: ?[]const u8 = null;
    var storage_root_path: []const u8 = runtime_storage_root_mod.default_storage_root;

    var index: usize = 0;
    while (index < argv.items.len) : (index += 1) {
        const arg = argv.items[index];
        if (std.mem.eql(u8, arg, "--memory")) {
            index += 1;
            if (index >= argv.items.len) {
                try stdout.writeAll("missing value for --memory\n");
                return;
            }
            const raw = argv.items[index];
            memory_bytes = runtime_config.parseMemoryBytes(raw) catch {
                try stdout.writeAll("invalid --memory value\n");
                return;
            };
        } else if (std.mem.eql(u8, arg, "--listen")) {
            index += 1;
            if (index >= argv.items.len) {
                try stdout.writeAll("missing value for --listen\n");
                return;
            }
            const raw = argv.items[index];
            listen_addr = raw;
        } else if (std.mem.eql(u8, arg, "--storage")) {
            index += 1;
            if (index >= argv.items.len) {
                try stdout.writeAll("missing value for --storage\n");
                return;
            }
            storage_root_path = argv.items[index];
        } else if (std.mem.eql(u8, arg, "--help")) {
            try stdout.writeAll(
                \\Usage:
                \\  pg2 [--memory <bytes|MiB|GiB>] [--listen <host:port>] [--storage <dir>]
                \\  pg2 lock inspect [--storage <dir>]
                \\  pg2 inspect runtime --format json --server <host:port>
                \\  pg2 advise
                \\  --memory       Startup memory budget (default: 512MiB)
                \\  --listen       Start server accept loop (Linux-only target)
                \\  --storage      Runtime storage root (default: .pg2)
                \\
            );
            return;
        } else {
            try stdout.writeAll("unknown argument\n");
            return;
        }
    }

    var storage_root = runtime_storage_root_mod.RuntimeStorageRoot.openOrCreate(
        storage_root_path,
    ) catch |err| switch (err) {
        error.StorageRootOpenFailed => {
            try stdout.writeAll("startup failed: could not open storage root\n");
            return;
        },
        error.LockOpenFailed => {
            try stdout.writeAll("startup failed: could not open storage lock file\n");
            return;
        },
        error.WriterAlreadyActive => {
            try stdout.writeAll(
                "startup failed: another writer is active for this storage root\n",
            );
            return;
        },
        error.LockAcquireFailed => {
            try stdout.writeAll("startup failed: could not acquire storage root lock\n");
            return;
        },
        error.LockMetadataWriteFailed => {
            try stdout.writeAll("startup failed: could not write lock metadata\n");
            return;
        },
        error.DataFileOpenFailed => {
            try stdout.writeAll("startup failed: could not open data.pg2\n");
            return;
        },
        error.WalFileOpenFailed => {
            try stdout.writeAll("startup failed: could not open wal.pg2\n");
            return;
        },
        error.TempFileOpenFailed => {
            try stdout.writeAll("startup failed: could not open temp.pg2\n");
            return;
        },
        error.TempTruncateFailed => {
            try stdout.writeAll("startup failed: could not truncate temp.pg2\n");
            return;
        },
    };
    defer storage_root.deinit();

    const detected_vcpus = runtime_config.detectVcpus();
    const plan = runtime_capacity_planner.planFromMemory(memory_bytes, detected_vcpus) catch |err| switch (err) {
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
        storage_root.storage(),
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
        storage_root.storage(),
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
        var advisor_sink = advisor_mod.sink.Sink.init();
        advisor_sink.start(&storage_root.root_dir) catch {
            try stdout.writeAll("startup failed: could not start advisor sink\n");
            return;
        };
        defer advisor_sink.deinit();
        var session = session_mod.Session.initWithStorageRootAndAdvisor(
            &runtime,
            &catalog,
            &storage_root,
            &advisor_sink,
        );
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

fn handleLockCommand(stdout: std.fs.File, args: []const []const u8) !void {
    if (args.len == 0 or !std.mem.eql(u8, args[0], "inspect")) {
        try stdout.writeAll("unknown lock command\n");
        return;
    }

    var storage_root_path: []const u8 = runtime_storage_root_mod.default_storage_root;
    var index: usize = 1;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--storage")) {
            index += 1;
            if (index >= args.len) {
                try stdout.writeAll("missing value for --storage\n");
                return;
            }
            storage_root_path = args[index];
            continue;
        }

        if (std.mem.eql(u8, arg, "--help")) {
            try stdout.writeAll("Usage: pg2 lock inspect [--storage <dir>]\n");
            return;
        }

        try stdout.writeAll("unknown argument\n");
        return;
    }

    const metadata = runtime_storage_root_mod.RuntimeStorageRoot.inspectLockMetadata(
        storage_root_path,
    ) catch |err| switch (err) {
        error.StorageRootOpenFailed => {
            try stdout.writeAll("lock inspect failed: could not open storage root\n");
            return;
        },
        error.LockOpenFailed => {
            try stdout.writeAll("lock inspect failed: LOCK file not found\n");
            return;
        },
        error.LockReadFailed => {
            try stdout.writeAll("lock inspect failed: could not read LOCK file\n");
            return;
        },
        error.InvalidLockMetadata => {
            try stdout.writeAll("lock inspect failed: invalid lock metadata format\n");
            return;
        },
    };

    var buf: [512]u8 = undefined;
    const out = std.fmt.bufPrint(
        &buf,
        "storage={s}\npid={d}\nhostname={s}\nstarted_at_unix_ns={d}\n",
        .{
            storage_root_path,
            metadata.pid,
            metadata.hostname(),
            metadata.started_at_unix_ns,
        },
    ) catch {
        try stdout.writeAll("lock inspect failed: output formatting overflow\n");
        return;
    };
    try stdout.writeAll(out);
}

fn handleInspectCommand(stdout: std.fs.File, args: []const []const u8) !void {
    if (args.len == 0 or !std.mem.eql(u8, args[0], "runtime")) {
        try stdout.writeAll("unknown inspect command\n");
        return;
    }

    var format_json = false;
    var server_addr: ?[]const u8 = null;
    var index: usize = 1;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--format")) {
            index += 1;
            if (index >= args.len) {
                try stdout.writeAll("missing value for --format\n");
                return;
            }
            format_json = std.mem.eql(u8, args[index], "json");
            if (!format_json) {
                try stdout.writeAll("inspect runtime failed: unsupported --format (use json)\n");
                return;
            }
            continue;
        }
        if (std.mem.eql(u8, arg, "--server")) {
            index += 1;
            if (index >= args.len) {
                try stdout.writeAll("missing value for --server\n");
                return;
            }
            server_addr = args[index];
            continue;
        }
        if (std.mem.eql(u8, arg, "--help")) {
            try stdout.writeAll(
                "Usage: pg2 inspect runtime --format json --server <host:port>\n",
            );
            return;
        }
        try stdout.writeAll("unknown argument\n");
        return;
    }

    if (!format_json) {
        try stdout.writeAll("inspect runtime failed: --format json is required\n");
        return;
    }
    const endpoint = server_addr orelse {
        try stdout.writeAll("inspect runtime failed: missing required --server <host:port>\n");
        return;
    };

    const address = std.net.Address.parseIpAndPort(endpoint) catch {
        try stdout.writeAll("inspect runtime failed: invalid --server address (expected host:port)\n");
        return;
    };
    var stream = std.net.tcpConnectToAddress(address) catch {
        try stdout.writeAll("inspect runtime failed: could not connect to server\n");
        return;
    };
    defer stream.close();

    stream.writeAll("inspect runtime --format json\n") catch {
        try stdout.writeAll("inspect runtime failed: request write failed\n");
        return;
    };

    var response_buf: [8192]u8 = undefined;
    var response_len: usize = 0;
    var byte: [1]u8 = undefined;
    while (response_len < response_buf.len) {
        const n = stream.read(byte[0..]) catch {
            try stdout.writeAll("inspect runtime failed: response read failed\n");
            return;
        };
        if (n == 0) break;
        response_buf[response_len] = byte[0];
        response_len += 1;
        if (byte[0] == '\n') break;
    }
    if (response_len == 0) {
        try stdout.writeAll("inspect runtime failed: empty response\n");
        return;
    }

    const response = response_buf[0..response_len];
    if (std.mem.startsWith(u8, response, "ERR ")) {
        try stdout.writeAll("inspect runtime failed: server returned error\n");
        return;
    }
    try stdout.writeAll(response);
}

fn handleAdviseCommand(stdout: std.fs.File, args: []const []const u8) !void {
    try advisor_mod.cli.runAdviseCommand(
        stdout.writer(),
        std.fs.cwd(),
        args,
    );
}
