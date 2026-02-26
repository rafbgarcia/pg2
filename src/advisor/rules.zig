//! Advisor rule evaluation and text formatting for `pg2 advise`.
const std = @import("std");
const metrics_mod = @import("metrics.zig");

const MetricRecord = metrics_mod.MetricRecord;

const queue_pressure_min_samples: u64 = 20;
const queue_pressure_saturation_threshold_pct: u64 = 30;
const queue_pressure_timeout_edge_threshold: u64 = 3;

const spill_ratio_min_samples: u64 = 20;
const spill_ratio_threshold_pct: u64 = 25;

const latency_spike_min_samples: usize = 30;
const latency_spike_multiplier: u64 = 4;
const latency_spike_absolute_ns: u64 = 5 * std.time.ns_per_ms;
const latency_spike_count_threshold: u64 = 3;

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
    queue_pressure,
    high_spill_ratio,
    low_selectivity_predicates,
    latency_spikes,
};

pub const Advisory = struct {
    id: RuleId,
    severity: Severity,
    confidence: Confidence,
    sample_count: u64 = 0,
    saturated_count: u64 = 0,
    timeout_edge_count: u64 = 0,
    spill_count: u64 = 0,
    low_selectivity_count: u64 = 0,
    predicate_operation_count: u64 = 0,
    lowest_selectivity_ppm: u32 = 0,
    latency_spike_count: u64 = 0,
    latency_median_ns: u64 = 0,
    latency_spike_threshold_ns: u64 = 0,
    max_total_ns: u64 = 0,
};

pub fn evaluate(
    allocator: std.mem.Allocator,
    records: []const MetricRecord,
) ![]Advisory {
    var out = std.ArrayList(Advisory){};
    defer out.deinit(allocator);

    const queue_eval = evaluateQueuePressure(records);
    if (queue_eval.triggered) {
        try out.append(allocator, .{
            .id = .queue_pressure,
            .severity = queueEvalSeverity(queue_eval),
            .confidence = confidenceFromCount(queue_eval.sample_count),
            .sample_count = queue_eval.sample_count,
            .saturated_count = queue_eval.saturated_count,
            .timeout_edge_count = queue_eval.timeout_edge_count,
        });
    }

    const spill_eval = evaluateSpillRatio(records);
    if (spill_eval.triggered) {
        try out.append(allocator, .{
            .id = .high_spill_ratio,
            .severity = spillEvalSeverity(spill_eval),
            .confidence = confidenceFromCount(spill_eval.sample_count),
            .sample_count = spill_eval.sample_count,
            .spill_count = spill_eval.spill_count,
        });
    }

    const low_selectivity_eval = evaluateLowSelectivity(records);
    if (low_selectivity_eval.low_selective_ops > 0) {
        try out.append(allocator, .{
            .id = .low_selectivity_predicates,
            .severity = .warning,
            .confidence = confidenceFromCount(low_selectivity_eval.low_selective_ops),
            .low_selectivity_count = low_selectivity_eval.low_selective_ops,
            .predicate_operation_count = low_selectivity_eval.predicate_ops,
            .lowest_selectivity_ppm = low_selectivity_eval.lowest_ppm,
        });
    }

    const latency_eval = try evaluateLatencySpikes(allocator, records);
    if (latency_eval.triggered) {
        try out.append(allocator, .{
            .id = .latency_spikes,
            .severity = latencyEvalSeverity(latency_eval),
            .confidence = confidenceFromCount(latency_eval.sample_count),
            .sample_count = latency_eval.sample_count,
            .latency_spike_count = latency_eval.spike_count,
            .latency_median_ns = latency_eval.median_ns,
            .latency_spike_threshold_ns = latency_eval.threshold_ns,
            .max_total_ns = latency_eval.max_total_ns,
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
            .queue_pressure => {
                const saturated_pct_tenths = ratioTenths(advisory.saturated_count, advisory.sample_count);
                try writer.print(
                    "advisory: queue pressure\nseverity: {s}\nconfidence: {s}\nevidence: samples={d} saturated_records={d} saturated_ratio={d}.{d}% timeout_edges={d}\naction: reduce concurrency or provision more workers/memory to absorb queue backlog\n\n",
                    .{
                        @tagName(advisory.severity),
                        @tagName(advisory.confidence),
                        advisory.sample_count,
                        advisory.saturated_count,
                        saturated_pct_tenths / 10,
                        saturated_pct_tenths % 10,
                        advisory.timeout_edge_count,
                    },
                );
            },
            .high_spill_ratio => {
                const spill_pct_tenths = ratioTenths(advisory.spill_count, advisory.sample_count);
                try writer.print(
                    "advisory: high spill ratio\nseverity: {s}\nconfidence: {s}\nevidence: samples={d} spill_records={d} spill_ratio={d}.{d}%\naction: increase work memory per query slot or reduce data shape causing spill\n\n",
                    .{
                        @tagName(advisory.severity),
                        @tagName(advisory.confidence),
                        advisory.sample_count,
                        advisory.spill_count,
                        spill_pct_tenths / 10,
                        spill_pct_tenths % 10,
                    },
                );
            },
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
            .latency_spikes => {
                try writer.print(
                    "advisory: latency spikes\nseverity: {s}\nconfidence: {s}\nevidence: samples={d} spikes={d} median_ns={d} spike_threshold_ns={d} max_total_ns={d}\naction: inspect high-latency queries and tune memory/concurrency to reduce tail latency\n\n",
                    .{
                        @tagName(advisory.severity),
                        @tagName(advisory.confidence),
                        advisory.sample_count,
                        advisory.latency_spike_count,
                        advisory.latency_median_ns,
                        advisory.latency_spike_threshold_ns,
                        advisory.max_total_ns,
                    },
                );
            },
        }
    }
}

const QueueEval = struct {
    sample_count: u64 = 0,
    saturated_count: u64 = 0,
    timeout_edge_count: u64 = 0,
    triggered: bool = false,
};

fn evaluateQueuePressure(records: []const MetricRecord) QueueEval {
    var sample_count: u64 = 0;
    var saturated_count: u64 = 0;
    var timeout_edge_count: u64 = 0;
    var prev_timeout_total: ?u64 = null;

    for (records) |record| {
        if (record.had_error) continue;
        sample_count += 1;
        if (record.workers_busy > 0 and record.queue_depth >= record.workers_busy) {
            saturated_count += 1;
        }
        if (prev_timeout_total) |prev| {
            if (record.queue_timeout_total > prev) {
                timeout_edge_count += 1;
            }
        }
        prev_timeout_total = record.queue_timeout_total;
    }

    const saturation_triggered = sample_count >= queue_pressure_min_samples and
        saturated_count * 100 >= sample_count * queue_pressure_saturation_threshold_pct;
    const timeout_triggered = sample_count >= queue_pressure_min_samples and
        timeout_edge_count >= queue_pressure_timeout_edge_threshold;

    return .{
        .sample_count = sample_count,
        .saturated_count = saturated_count,
        .timeout_edge_count = timeout_edge_count,
        .triggered = saturation_triggered or timeout_triggered,
    };
}

fn queueEvalSeverity(eval: QueueEval) Severity {
    if (eval.sample_count == 0) return .warning;
    if (eval.timeout_edge_count >= queue_pressure_timeout_edge_threshold and
        eval.saturated_count * 100 >= eval.sample_count * 50)
    {
        return .critical;
    }
    return .warning;
}

const SpillEval = struct {
    sample_count: u64 = 0,
    spill_count: u64 = 0,
    triggered: bool = false,
};

fn evaluateSpillRatio(records: []const MetricRecord) SpillEval {
    var sample_count: u64 = 0;
    var spill_count: u64 = 0;

    for (records) |record| {
        if (record.had_error) continue;
        sample_count += 1;
        if (record.spill_triggered or record.temp_bytes_written > 0 or record.temp_bytes_read > 0) {
            spill_count += 1;
        }
    }

    const triggered = sample_count >= spill_ratio_min_samples and
        spill_count * 100 >= sample_count * spill_ratio_threshold_pct;

    return .{
        .sample_count = sample_count,
        .spill_count = spill_count,
        .triggered = triggered,
    };
}

fn spillEvalSeverity(eval: SpillEval) Severity {
    if (eval.sample_count == 0) return .warning;
    if (eval.spill_count * 100 >= eval.sample_count * 50) return .critical;
    return .warning;
}

const LowSelectivityEval = struct {
    predicate_ops: u64 = 0,
    low_selective_ops: u64 = 0,
    lowest_ppm: u32 = 1_000_000,
};

fn evaluateLowSelectivity(records: []const MetricRecord) LowSelectivityEval {
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
        if (isLowSelectivity(record.rows_matched, record.rows_scanned)) {
            low_selective_ops += 1;
        }
    }

    return .{
        .predicate_ops = predicate_ops,
        .low_selective_ops = low_selective_ops,
        .lowest_ppm = lowest_ppm,
    };
}

const LatencyEval = struct {
    sample_count: u64 = 0,
    spike_count: u64 = 0,
    median_ns: u64 = 0,
    threshold_ns: u64 = 0,
    max_total_ns: u64 = 0,
    triggered: bool = false,
};

fn evaluateLatencySpikes(
    allocator: std.mem.Allocator,
    records: []const MetricRecord,
) !LatencyEval {
    var values = std.ArrayList(u64){};
    defer values.deinit(allocator);

    var max_total_ns: u64 = 0;
    for (records) |record| {
        if (record.had_error) continue;
        if (record.total_ns == 0) continue;
        if (record.total_ns > max_total_ns) max_total_ns = record.total_ns;
        try values.append(allocator, record.total_ns);
    }

    if (values.items.len < latency_spike_min_samples) {
        return .{
            .sample_count = values.items.len,
            .max_total_ns = max_total_ns,
        };
    }

    const sorted = try allocator.alloc(u64, values.items.len);
    defer allocator.free(sorted);
    @memcpy(sorted, values.items);
    std.mem.sort(u64, sorted, {}, comptime std.sort.asc(u64));

    const mid = sorted.len / 2;
    const median_ns = if ((sorted.len % 2) == 1)
        sorted[mid]
    else
        sorted[mid - 1] / 2 + sorted[mid] / 2;

    const multiplied = std.math.mul(u64, median_ns, latency_spike_multiplier) catch std.math.maxInt(u64);
    const additive = std.math.add(u64, median_ns, latency_spike_absolute_ns) catch std.math.maxInt(u64);
    const threshold_ns = @max(multiplied, additive);

    var spike_count: u64 = 0;
    for (values.items) |total_ns| {
        if (total_ns >= threshold_ns) spike_count += 1;
    }

    return .{
        .sample_count = values.items.len,
        .spike_count = spike_count,
        .median_ns = median_ns,
        .threshold_ns = threshold_ns,
        .max_total_ns = max_total_ns,
        .triggered = spike_count >= latency_spike_count_threshold,
    };
}

fn latencyEvalSeverity(eval: LatencyEval) Severity {
    if (eval.sample_count == 0) return .warning;
    if (eval.spike_count * 100 >= eval.sample_count * 20) return .critical;
    return .warning;
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

fn ratioTenths(numerator: u64, denominator: u64) u64 {
    if (denominator == 0) return 0;
    return (numerator * 1000) / denominator;
}

fn confidenceFromCount(count: u64) Confidence {
    if (count >= 30) return .high;
    if (count >= 10) return .medium;
    return .low;
}

test "evaluate emits queue pressure advisory when saturation ratio exceeds threshold" {
    var records: [24]MetricRecord = [_]MetricRecord{.{}} ** 24;
    var i: usize = 0;
    while (i < records.len) : (i += 1) {
        records[i] = .{
            .queue_depth = 2,
            .workers_busy = 2,
            .queue_timeout_total = @intCast(i / 8),
        };
    }

    const advisories = try evaluate(std.testing.allocator, &records);
    defer std.testing.allocator.free(advisories);

    try std.testing.expectEqual(@as(usize, 1), advisories.len);
    try std.testing.expectEqual(RuleId.queue_pressure, advisories[0].id);
}

test "evaluate does not emit queue pressure advisory below sample gate" {
    var records: [10]MetricRecord = [_]MetricRecord{.{}} ** 10;
    for (&records) |*record| {
        record.* = .{ .queue_depth = 4, .workers_busy = 1 };
    }

    const advisories = try evaluate(std.testing.allocator, &records);
    defer std.testing.allocator.free(advisories);
    try std.testing.expectEqual(@as(usize, 0), advisories.len);
}

test "evaluate emits spill ratio advisory when spill ratio exceeds threshold" {
    var records: [20]MetricRecord = [_]MetricRecord{.{}} ** 20;
    var i: usize = 0;
    while (i < records.len) : (i += 1) {
        records[i] = .{};
        if (i < 6) records[i].spill_triggered = true;
    }

    const advisories = try evaluate(std.testing.allocator, &records);
    defer std.testing.allocator.free(advisories);

    try std.testing.expectEqual(@as(usize, 1), advisories.len);
    try std.testing.expectEqual(RuleId.high_spill_ratio, advisories[0].id);
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

test "evaluate emits latency spike advisory when repeated spikes exceed threshold" {
    var records: [30]MetricRecord = [_]MetricRecord{.{}} ** 30;
    var i: usize = 0;
    while (i < records.len) : (i += 1) {
        records[i] = .{ .total_ns = 1_000_000 };
    }
    records[27].total_ns = 10_000_000;
    records[28].total_ns = 11_000_000;
    records[29].total_ns = 12_000_000;

    const advisories = try evaluate(std.testing.allocator, &records);
    defer std.testing.allocator.free(advisories);

    try std.testing.expectEqual(@as(usize, 1), advisories.len);
    try std.testing.expectEqual(RuleId.latency_spikes, advisories[0].id);
}

test "evaluate emits advisories in deterministic rule order" {
    var records: [40]MetricRecord = [_]MetricRecord{.{}} ** 40;
    var i: usize = 0;
    while (i < records.len) : (i += 1) {
        records[i] = .{
            .queue_depth = 3,
            .workers_busy = 2,
            .queue_timeout_total = @intCast(i / 10),
            .spill_triggered = i < 20,
            .total_ns = if (i < 35) 1_000_000 else 10_000_000,
            .operation_kind = .select,
            .has_predicate_filter = true,
            .rows_scanned = 100,
            .rows_matched = if (i < 5) 10 else 90,
        };
    }

    const advisories = try evaluate(std.testing.allocator, &records);
    defer std.testing.allocator.free(advisories);

    try std.testing.expectEqual(@as(usize, 4), advisories.len);
    try std.testing.expectEqual(RuleId.queue_pressure, advisories[0].id);
    try std.testing.expectEqual(RuleId.high_spill_ratio, advisories[1].id);
    try std.testing.expectEqual(RuleId.low_selectivity_predicates, advisories[2].id);
    try std.testing.expectEqual(RuleId.latency_spikes, advisories[3].id);
}

test "writeText prints deterministic no-advisories output" {
    var buf: [64]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);
    try writeText(stream.writer(), &.{});
    try std.testing.expectEqualStrings("no advisories\n", stream.getWritten());
}
