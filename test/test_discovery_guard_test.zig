const std = @import("std");

const Suite = struct {
    name: []const u8,
    dir: []const u8,
    root: []const u8,
};

const suites = [_]Suite{
    .{ .name = "unit", .dir = "unit", .root = "test/unit/unit_specs_test.zig" },
    .{ .name = "features", .dir = "features", .root = "test/features/features_specs_test.zig" },
    .{ .name = "internals", .dir = "internals", .root = "test/internals/internals_specs_test.zig" },
    .{ .name = "stress", .dir = "stress", .root = "test/stress/stress_specs_test.zig" },
    .{ .name = "sim", .dir = "sim", .root = "test/sim/sim_specs_test.zig" },
};

fn managedTestFile(path: []const u8) bool {
    if (!std.mem.endsWith(u8, path, "_test.zig")) return false;

    const base = std.fs.path.basename(path);
    if (std.mem.endsWith(u8, base, "_specs_test.zig")) return false;
    if (std.mem.eql(u8, base, "test_env_test.zig")) return false;

    return true;
}

fn suiteFilePath(allocator: std.mem.Allocator, suite_dir: []const u8, rel: []const u8) ![]const u8 {
    return std.mem.concat(allocator, u8, &.{ "test/", suite_dir, "/", rel });
}

test "suite aggregators include every managed test exactly once" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var global_expected = std.StringHashMap(void).init(allocator);
    var assignment_counts = std.StringHashMap(u8).init(allocator);

    inline for (suites) |suite| {
        var expected = std.StringHashMap(void).init(allocator);
        var imported = std.StringHashMap(void).init(allocator);

        const suite_dir = try std.mem.concat(allocator, u8, &.{ "test/", suite.dir });
        var dir = try std.fs.cwd().openDir(suite_dir, .{ .iterate = true });
        defer dir.close();

        var walker = try dir.walk(allocator);
        defer walker.deinit();

        while (try walker.next()) |entry| {
            if (entry.kind != .file) continue;
            if (!managedTestFile(entry.path)) continue;

            const repo_rel = try suiteFilePath(allocator, suite.dir, entry.path);
            try expected.put(repo_rel, {});
            try global_expected.put(repo_rel, {});
        }

        const root_source = try std.fs.cwd().readFileAlloc(allocator, suite.root, 1024 * 1024);
        var lines = std.mem.splitScalar(u8, root_source, '\n');
        while (lines.next()) |line| {
            const prefix = "_ = @import(\"";
            const start = std.mem.indexOf(u8, line, prefix) orelse continue;
            const tail = line[start + prefix.len ..];
            const end = std.mem.indexOf(u8, tail, "\");") orelse continue;
            const import_rel = tail[0..end];

            const repo_rel = try suiteFilePath(allocator, suite.dir, import_rel);
            if (!expected.contains(repo_rel)) {
                std.debug.print(
                    "unexpected import in {s} suite root: {s}\n",
                    .{ suite.name, import_rel },
                );
                return error.TestUnexpectedResult;
            }

            try imported.put(repo_rel, {});
            if (assignment_counts.getPtr(repo_rel)) |count| {
                count.* += 1;
            } else {
                try assignment_counts.put(repo_rel, 1);
            }
        }

        var expected_it = expected.iterator();
        while (expected_it.next()) |entry| {
            if (!imported.contains(entry.key_ptr.*)) {
                std.debug.print(
                    "managed test missing from {s} suite root: {s}\n",
                    .{ suite.name, entry.key_ptr.* },
                );
                return error.TestUnexpectedResult;
            }
        }
    }

    var expected_it = global_expected.iterator();
    while (expected_it.next()) |entry| {
        const count = assignment_counts.get(entry.key_ptr.*) orelse 0;
        if (count != 1) {
            std.debug.print(
                "managed test assigned {d} times across suite roots: {s}\n",
                .{ count, entry.key_ptr.* },
            );
            return error.TestUnexpectedResult;
        }
    }
}

test "internals suite does not import feature test wrapper paths" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var dir = try std.fs.cwd().openDir("test/internals", .{ .iterate = true });
    defer dir.close();

    var walker = try dir.walk(allocator);
    defer walker.deinit();

    while (try walker.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".zig")) continue;

        const full_path = try std.mem.concat(allocator, u8, &.{ "test/internals/", entry.path });
        const source = try std.fs.cwd().readFileAlloc(allocator, full_path, 1024 * 1024);

        if (std.mem.indexOf(u8, source, "features/test_env_test.zig") != null) {
            std.debug.print(
                "internal test imports feature wrapper path: {s}\n",
                .{full_path},
            );
            return error.TestUnexpectedResult;
        }
    }
}
