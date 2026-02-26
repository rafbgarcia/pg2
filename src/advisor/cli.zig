//! CLI entrypoints for advisor surfaces.
const std = @import("std");
const metrics_mod = @import("metrics.zig");
const rules_mod = @import("rules.zig");
const runtime_storage_root_mod = @import("../runtime/storage_root.zig");

pub fn runAdviseCommand(
    writer: anytype,
    cwd: std.fs.Dir,
    args: []const []const u8,
) !void {
    if (args.len > 0) {
        if (args.len == 1 and std.mem.eql(u8, args[0], "--help")) {
            try writer.writeAll("Usage: pg2 advise\n");
            return;
        }
        try writer.writeAll("unknown argument\n");
        return;
    }

    var storage_dir = cwd.openDir(runtime_storage_root_mod.default_storage_root, .{}) catch {
        try writer.writeAll("no advisories\n");
        return;
    };
    defer storage_dir.close();

    const allocator = std.heap.page_allocator;
    const records = metrics_mod.readAll(allocator, &storage_dir) catch |err| switch (err) {
        error.FileNotFound => {
            try writer.writeAll("no advisories\n");
            return;
        },
        error.InvalidFormat, error.UnsupportedVersion => {
            try writer.writeAll("advise failed: advisor metrics file is corrupted\n");
            return;
        },
        else => {
            try writer.writeAll("advise failed: could not read advisor metrics\n");
            return;
        },
    };
    defer allocator.free(records);

    const advisories = rules_mod.evaluate(allocator, records) catch {
        try writer.writeAll("advise failed: could not evaluate advisories\n");
        return;
    };
    defer allocator.free(advisories);

    rules_mod.writeText(writer, advisories) catch {
        try writer.writeAll("advise failed: output formatting failed\n");
        return;
    };
}

test "runAdviseCommand returns no advisories when storage root is absent" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var out_buf: [128]u8 = undefined;
    var stream = std.io.fixedBufferStream(&out_buf);
    try runAdviseCommand(stream.writer(), tmp.dir, &.{});
    try std.testing.expectEqualStrings("no advisories\n", stream.getWritten());
}

test "runAdviseCommand handles missing metrics file" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makeDir(runtime_storage_root_mod.default_storage_root);

    var out_buf: [128]u8 = undefined;
    var stream = std.io.fixedBufferStream(&out_buf);
    try runAdviseCommand(stream.writer(), tmp.dir, &.{});
    try std.testing.expectEqualStrings("no advisories\n", stream.getWritten());
}

test "runAdviseCommand reports corruption" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makeDir(runtime_storage_root_mod.default_storage_root);
    var storage_dir = try tmp.dir.openDir(runtime_storage_root_mod.default_storage_root, .{});
    defer storage_dir.close();

    var file = try storage_dir.createFile(metrics_mod.metrics_filename, .{});
    defer file.close();
    try file.writeAll("corrupt");

    var out_buf: [256]u8 = undefined;
    var stream = std.io.fixedBufferStream(&out_buf);
    try runAdviseCommand(stream.writer(), tmp.dir, &.{});
    try std.testing.expectEqualStrings(
        "advise failed: advisor metrics file is corrupted\n",
        stream.getWritten(),
    );
}

test "runAdviseCommand prints no advisories for healthy metrics with no triggers" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makeDir(runtime_storage_root_mod.default_storage_root);
    var storage_dir = try tmp.dir.openDir(runtime_storage_root_mod.default_storage_root, .{});
    defer storage_dir.close();

    var i: usize = 0;
    while (i < 10) : (i += 1) {
        try metrics_mod.appendRecord(&storage_dir, &.{
            .operation_kind = .select,
            .has_predicate_filter = true,
            .rows_scanned = 100,
            .rows_matched = 90,
            .total_ns = 1_000_000,
        });
    }

    var out_buf: [256]u8 = undefined;
    var stream = std.io.fixedBufferStream(&out_buf);
    try runAdviseCommand(stream.writer(), tmp.dir, &.{});
    try std.testing.expectEqualStrings("no advisories\n", stream.getWritten());
}

test "runAdviseCommand prints deterministic multi-rule advisories" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.makeDir(runtime_storage_root_mod.default_storage_root);
    var storage_dir = try tmp.dir.openDir(runtime_storage_root_mod.default_storage_root, .{});
    defer storage_dir.close();

    var i: usize = 0;
    while (i < 40) : (i += 1) {
        try metrics_mod.appendRecord(&storage_dir, &.{
            .queue_depth = 4,
            .workers_busy = 2,
            .queue_timeout_total = @intCast(i / 10),
            .spill_triggered = i < 20,
            .operation_kind = .select,
            .has_predicate_filter = true,
            .rows_scanned = 100,
            .rows_matched = if (i < 6) 10 else 90,
            .total_ns = if (i < 35) 1_000_000 else 10_000_000,
        });
    }

    var out_buf: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&out_buf);
    try runAdviseCommand(stream.writer(), tmp.dir, &.{});
    const output = stream.getWritten();

    const queue_idx = std.mem.indexOf(u8, output, "advisory: queue pressure") orelse return error.TestUnexpectedResult;
    const spill_idx = std.mem.indexOf(u8, output, "advisory: high spill ratio") orelse return error.TestUnexpectedResult;
    const low_selectivity_idx = std.mem.indexOf(u8, output, "advisory: low-selectivity predicates") orelse return error.TestUnexpectedResult;
    const latency_idx = std.mem.indexOf(u8, output, "advisory: latency spikes") orelse return error.TestUnexpectedResult;

    try std.testing.expect(queue_idx < spill_idx);
    try std.testing.expect(spill_idx < low_selectivity_idx);
    try std.testing.expect(low_selectivity_idx < latency_idx);
}
