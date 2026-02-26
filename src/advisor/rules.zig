//! Advisor rule evaluation and text formatting for `pg2 advise`.
const std = @import("std");
const metrics_mod = @import("metrics.zig");

const MetricRecord = metrics_mod.MetricRecord;

pub const Severity = enum(u8) {
    info,
    warning,
    critical,
};

pub const Confidence = enum(u8) {
    low,
    medium,
    high,
};

pub const RuleId = enum {
    low_selectivity_predicates,
};

pub const Advisory = struct {
    id: RuleId,
    severity: Severity,
    confidence: Confidence,
    low_selectivity_count: u64,
    predicate_operation_count: u64,
    lowest_selectivity_ppm: u32,
};

pub fn evaluate(
    allocator: std.mem.Allocator,
    records: []const MetricRecord,
) ![]Advisory {
    var out = std.ArrayList(Advisory){};
    defer out.deinit(allocator);

    var predicate_ops: u64 = 0;
    var low_selective_ops: u64 = 0;
    var lowest_ppm: u32 = 1_000_000;

    for (records) |record| {
        if (record.had_error) continue;
        switch (record.operation_kind) {
            .select, .update, .delete => {},
            else => continue,
        }
        if (!record.has_predicate_filter) continue;
        if (record.rows_scanned == 0) continue;

        predicate_ops += 1;
        const ppm = selectivityPpm(record.rows_matched, record.rows_scanned);
        if (ppm < lowest_ppm) lowest_ppm = ppm;

        // Low selectivity threshold is a locked v1 decision: < 50%.
        if (isLowSelectivity(record.rows_matched, record.rows_scanned)) {
            low_selective_ops += 1;
        }
    }

    if (low_selective_ops > 0) {
        try out.append(allocator, .{
            .id = .low_selectivity_predicates,
            .severity = .warning,
            .confidence = confidenceFromCount(low_selective_ops),
            .low_selectivity_count = low_selective_ops,
            .predicate_operation_count = predicate_ops,
            .lowest_selectivity_ppm = lowest_ppm,
        });
    }

    return out.toOwnedSlice(allocator);
}

pub fn writeText(writer: anytype, advisories: []const Advisory) !void {
    if (advisories.len == 0) {
        try writer.writeAll("no advisories\n");
        return;
    }

    for (advisories) |advisory| {
        switch (advisory.id) {
            .low_selectivity_predicates => {
                const pct10 = advisory.lowest_selectivity_ppm / 1000;
                const pct_whole = pct10 / 10;
                const pct_tenth = pct10 % 10;
                try writer.print(
                    "advisory: low-selectivity predicates\nseverity: {s}\nconfidence: {s}\nevidence: low_selectivity_ops={d} predicate_ops={d} lowest_selectivity={d}.{d}%\naction: consider adding or refining indexes on frequently filtered fields\n\n",
                    .{
                        @tagName(advisory.severity),
                        @tagName(advisory.confidence),
                        advisory.low_selectivity_count,
                        advisory.predicate_operation_count,
                        pct_whole,
                        pct_tenth,
                    },
                );
            },
        }
    }
}

fn isLowSelectivity(rows_matched: u32, rows_scanned: u32) bool {
    const matched: u64 = rows_matched;
    const scanned: u64 = rows_scanned;
    return matched * 2 < scanned;
}

fn selectivityPpm(rows_matched: u32, rows_scanned: u32) u32 {
    if (rows_scanned == 0) return 0;
    const ppm_u64 = (@as(u64, rows_matched) * 1_000_000) / @as(u64, rows_scanned);
    return @intCast(@min(ppm_u64, @as(u64, std.math.maxInt(u32))));
}

fn confidenceFromCount(low_selective_ops: u64) Confidence {
    if (low_selective_ops >= 10) return .high;
    if (low_selective_ops >= 3) return .medium;
    return .low;
}

test "evaluate emits low-selectivity advisory for predicate-driven select/update/delete" {
    const records = [_]MetricRecord{
        .{
            .operation_kind = .select,
            .has_predicate_filter = true,
            .rows_scanned = 100,
            .rows_matched = 20,
        },
        .{
            .operation_kind = .update,
            .has_predicate_filter = true,
            .rows_scanned = 100,
            .rows_matched = 40,
        },
        .{
            .operation_kind = .delete,
            .has_predicate_filter = true,
            .rows_scanned = 100,
            .rows_matched = 60,
        },
        .{
            .operation_kind = .insert,
            .has_predicate_filter = false,
            .rows_scanned = 0,
            .rows_matched = 0,
        },
    };

    const advisories = try evaluate(std.testing.allocator, &records);
    defer std.testing.allocator.free(advisories);

    try std.testing.expectEqual(@as(usize, 1), advisories.len);
    try std.testing.expectEqual(RuleId.low_selectivity_predicates, advisories[0].id);
    try std.testing.expectEqual(@as(u64, 2), advisories[0].low_selectivity_count);
    try std.testing.expectEqual(@as(u64, 3), advisories[0].predicate_operation_count);
}

test "writeText prints deterministic no-advisories output" {
    var buf: [64]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);
    try writeText(stream.writer(), &.{});
    try std.testing.expectEqualStrings("no advisories\n", stream.getWritten());
}
