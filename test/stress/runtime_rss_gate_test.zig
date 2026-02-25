//! WF14 Phase 3 RSS guardrail stress gate.
//!
//! Contract:
//! - Run sustained inserts on file-backed runtime storage.
//! - Continue until `1_000_000` rows or `1 GiB` on-disk bytes (whichever first).
//! - Sample RSS every second, exclude first 10% warm-up samples.
//! - Assert steady-state p95 RSS <= 1.35 * memory budget.
const std = @import("std");
const builtin = @import("builtin");
const pg2 = @import("pg2");

const bootstrap_mod = pg2.runtime.bootstrap;
const storage_root_mod = pg2.runtime.storage_root;
const catalog_mod = pg2.catalog.meta;
const schema_loader_mod = pg2.catalog.schema_loader;
const parser_mod = pg2.parser.parse;
const tokenizer_mod = pg2.parser.tokenizer;
const heap_mod = pg2.storage.heap;
const session_mod = pg2.server.session;
const pool_mod = pg2.server.pool;

const BootstrappedRuntime = bootstrap_mod.BootstrappedRuntime;
const RuntimeStorageRoot = storage_root_mod.RuntimeStorageRoot;
const Catalog = catalog_mod.Catalog;
const Session = session_mod.Session;
const ConnectionPool = pool_mod.ConnectionPool;

const target_rows: u64 = 1_000_000;
const target_storage_bytes: u64 = 1024 * 1024 * 1024;
const memory_budget_bytes: usize = 256 * 1024 * 1024;
const rss_limit_scale: f64 = 1.35;
const batch_size: u32 = 256;
const sample_interval_ns: i128 = std.time.ns_per_s;
const storage_checkpoint_every_batches: u32 = 16;
const storage_near_limit_window_bytes: u64 = 64 * 1024 * 1024;

fn refreshOnDiskUsage(
    runtime: *BootstrappedRuntime,
    storage_root: *RuntimeStorageRoot,
) !u64 {
    try runtime.wal.forceFlush();
    try runtime.pool.flushAll();
    const usage = try storage_root.snapshotUsage();
    return usage.data_pg2_bytes + usage.wal_pg2_bytes + usage.temp_pg2_bytes;
}

fn applySchemaAndInit(runtime: *BootstrappedRuntime, catalog: *Catalog) !void {
    const schema =
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, bool, notNull)
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(schema);
    if (tokens.has_error) return error.InvalidSchema;
    const parsed = parser_mod.parse(&tokens, schema);
    if (parsed.has_error) return error.InvalidSchema;
    try schema_loader_mod.loadSchema(catalog, &parsed.ast, &tokens, schema);

    const heap_region_start: u32 = 100;
    const heap_region_stride_pages: u32 = 512;
    var model_id: u16 = 0;
    while (model_id < catalog.model_count) : (model_id += 1) {
        const page_id: u32 = heap_region_start + @as(u32, model_id) * heap_region_stride_pages;
        catalog.models[model_id].heap_first_page_id = page_id;
        catalog.models[model_id].total_pages = 1;

        const page = try runtime.pool.pin(page_id);
        heap_mod.HeapPage.init(page);
        runtime.pool.unpin(page_id, true);
    }

    // Keep schema/index setup semantics aligned with shared test harness:
    // add missing PK metadata if needed, then initialize all unique indexes.
    model_id = 0;
    while (model_id < catalog.model_count) : (model_id += 1) {
        const pk_col = catalog_mod.findPrimaryKeyColumnId(catalog, model_id) orelse continue;
        const model = &catalog.models[model_id];

        var has_pk_index = false;
        var ii: u16 = 0;
        while (ii < model.index_count) : (ii += 1) {
            if (model.indexes[ii].is_unique and
                model.indexes[ii].column_count == 1 and
                model.indexes[ii].column_ids[0] == pk_col)
            {
                has_pk_index = true;
                break;
            }
        }
        if (!has_pk_index) {
            _ = catalog.addIndex(
                model_id,
                "pk",
                &[_]catalog_mod.ColumnId{pk_col},
                true,
            ) catch continue;
        }
    }

    var next_index_page_id: u32 = 10_000;
    try catalog.initializeIndexTrees(
        &runtime.pool,
        &runtime.wal,
        &next_index_page_id,
    );
}

fn runBatchInsert(
    session: *Session,
    pool: *ConnectionPool,
    start_id: u64,
    count: u32,
) !void {
    var req_buf: [24 * 1024]u8 = undefined;
    var response_buf: [1024]u8 = undefined;
    var stream = std.io.fixedBufferStream(req_buf[0..]);
    const writer = stream.writer();

    try writer.writeAll("User |> insert(");
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        if (i > 0) try writer.writeAll(", ");
        const id = start_id + i;
        try writer.print(
            "(id = {d}, name = \"user-{d}\", active = true)",
            .{ id, id },
        );
    }
    try writer.writeAll(") {}");

    const written = try session.dispatchRequest(pool, stream.getWritten(), response_buf[0..]);
    const response = response_buf[0..written];
    try std.testing.expect(std.mem.startsWith(u8, response, "OK returned_rows=0 inserted_rows="));
}

fn sampleRssBytes(allocator: std.mem.Allocator) !u64 {
    return switch (builtin.os.tag) {
        .linux => sampleRssLinux(),
        .macos => sampleRssMacos(allocator),
        else => error.UnsupportedPlatform,
    };
}

fn sampleRssLinux() !u64 {
    var file = try std.fs.openFileAbsolute("/proc/self/status", .{});
    defer file.close();
    var buf: [8192]u8 = undefined;
    const len = try file.readAll(&buf);
    const content = buf[0..len];
    const marker = "VmRSS:";
    const start = std.mem.indexOf(u8, content, marker) orelse return error.MissingVmRss;
    var cursor = start + marker.len;
    while (cursor < content.len and (content[cursor] == ' ' or content[cursor] == '\t')) : (cursor += 1) {}
    var end = cursor;
    while (end < content.len and std.ascii.isDigit(content[end])) : (end += 1) {}
    if (end == cursor) return error.InvalidVmRss;
    const value_kib = try std.fmt.parseInt(u64, content[cursor..end], 10);
    return value_kib * 1024;
}

fn sampleRssMacos(allocator: std.mem.Allocator) !u64 {
    var pid_buf: [32]u8 = undefined;
    const pid_text = try std.fmt.bufPrint(
        &pid_buf,
        "{d}",
        .{std.c.getpid()},
    );
    const run = try std.process.Child.run(.{
        .allocator = allocator,
        .argv = &[_][]const u8{ "ps", "-o", "rss=", "-p", pid_text },
        .max_output_bytes = 256,
    });
    defer allocator.free(run.stdout);
    defer allocator.free(run.stderr);
    switch (run.term) {
        .Exited => |code| if (code != 0) return error.PsFailed,
        else => return error.PsFailed,
    }
    const trimmed = std.mem.trim(u8, run.stdout, " \t\r\n");
    if (trimmed.len == 0) return error.InvalidPsRss;
    const value_kib = try std.fmt.parseInt(u64, trimmed, 10);
    return value_kib * 1024;
}

fn percentile95(samples: []const u64) !u64 {
    std.debug.assert(samples.len > 0);
    const allocator = std.testing.allocator;
    const sorted = try allocator.alloc(u64, samples.len);
    defer allocator.free(sorted);
    @memcpy(sorted, samples);

    std.sort.heap(u64, sorted, {}, lessThanU64);

    const rank = (95 * sorted.len + 99) / 100;
    const idx = if (rank == 0) 0 else rank - 1;
    return sorted[idx];
}

fn lessThanU64(_: void, a: u64, b: u64) bool {
    return a < b;
}

test "wf14 rss p95 remains within 1.35x memory budget under sustained file-backed growth" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var original_dir = try std.fs.cwd().openDir(".", .{});
    defer original_dir.close();
    try std.posix.fchdir(tmp.dir.fd);
    defer std.posix.fchdir(original_dir.fd) catch {};

    var storage_root = try RuntimeStorageRoot.openOrCreate("rss-gate");
    defer storage_root.deinit();

    const backing_memory = try std.testing.allocator.alloc(u8, memory_budget_bytes);
    defer std.testing.allocator.free(backing_memory);

    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        storage_root.storage(),
        .{
            .max_query_slots = 1,
            .buffer_pool_frames = 64,
            .wal_buffer_capacity_bytes = 1024 * 1024,
            .wal_flush_threshold_bytes = 256 * 1024,
            .undo_max_entries = 16 * 1024,
            .undo_max_data_bytes = 8 * 1024 * 1024,
        },
    );
    defer runtime.deinit();

    var catalog = Catalog{};
    try applySchemaAndInit(&runtime, &catalog);
    var session = Session.initWithStorageRoot(&runtime, &catalog, &storage_root);
    var pool = ConnectionPool.initWithConfig(&runtime, .{ .overload_policy = .queue });

    var rss_samples: std.ArrayList(u64) = .empty;
    defer rss_samples.deinit(std.testing.allocator);

    var inserted_rows: u64 = 0;
    var next_row_id: u64 = 1;
    var now_ns: i128 = std.time.nanoTimestamp();
    var next_sample_ns = now_ns + sample_interval_ns;
    var on_disk_bytes = try refreshOnDiskUsage(&runtime, &storage_root);
    var batches_since_storage_checkpoint: u32 = 0;

    while (inserted_rows < target_rows and on_disk_bytes < target_storage_bytes) {
        const remaining = target_rows - inserted_rows;
        const rows_this_batch: u32 = @intCast(@min(@as(u64, batch_size), remaining));
        try runBatchInsert(&session, &pool, next_row_id, rows_this_batch);
        next_row_id += rows_this_batch;
        inserted_rows += rows_this_batch;
        batches_since_storage_checkpoint += 1;

        const bytes_remaining = target_storage_bytes -| on_disk_bytes;
        const near_storage_limit = bytes_remaining <= storage_near_limit_window_bytes;
        if (near_storage_limit or batches_since_storage_checkpoint >= storage_checkpoint_every_batches) {
            on_disk_bytes = try refreshOnDiskUsage(&runtime, &storage_root);
            batches_since_storage_checkpoint = 0;
        }

        now_ns = std.time.nanoTimestamp();
        while (now_ns >= next_sample_ns) : (next_sample_ns += sample_interval_ns) {
            const rss_bytes = try sampleRssBytes(std.testing.allocator);
            try rss_samples.append(std.testing.allocator, rss_bytes);
        }
    }

    if (rss_samples.items.len == 0) {
        try rss_samples.append(std.testing.allocator, try sampleRssBytes(std.testing.allocator));
    }

    // Ensure final assertion checks against a current durable size.
    if (batches_since_storage_checkpoint > 0) {
        on_disk_bytes = try refreshOnDiskUsage(&runtime, &storage_root);
    }

    const warmup_count = (rss_samples.items.len + 9) / 10;
    const steady = if (warmup_count >= rss_samples.items.len)
        rss_samples.items[rss_samples.items.len - 1 ..]
    else
        rss_samples.items[warmup_count..];
    const p95_rss = try percentile95(steady);

    const rss_limit = @as(u64, @intFromFloat(
        @as(f64, @floatFromInt(memory_budget_bytes)) * rss_limit_scale,
    ));
    try std.testing.expect(p95_rss <= rss_limit);
    try std.testing.expect(inserted_rows == target_rows or on_disk_bytes >= target_storage_bytes);
}
