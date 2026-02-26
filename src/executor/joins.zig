//! Nested-loop join implementations with bounded capacity contracts.
//!
//! Provides inner and left join execution over pre-materialized row buffers.
//! All joins enforce explicit capacity limits to keep memory usage bounded.
const std = @import("std");
const row_mod = @import("../storage/row.zig");
const scan_mod = @import("scan.zig");
const capacity_mod = @import("capacity.zig");
const hash_join_mod = @import("hash_join.zig");

const Value = row_mod.Value;
const compareValues = row_mod.compareValues;
const ResultRow = scan_mod.ResultRow;
const QueryResult = @import("executor.zig").QueryResult;
const max_parallel_join_workers: usize = 8;

pub const JoinDescriptor = struct {
    left_key_index: u16,
    right_key_index: u16,
};

pub fn executeLeftJoinHashFlat(
    result: *QueryResult,
    left_rows: []const ResultRow,
    right_rows: []const ResultRow,
    join: JoinDescriptor,
    right_column_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
    parallel_enabled: bool,
    parallel_worker_budget: u8,
    parallel_min_rows_per_worker: u16,
) bool {
    result.row_count = 0;
    if (parallel_enabled) {
        switch (tryExecuteLeftJoinHashFlatParallel(
            result,
            left_rows,
            right_rows,
            join,
            right_column_count,
            caps,
            parallel_worker_budget,
            parallel_min_rows_per_worker,
        )) {
            .applied => {
                result.stats.plan.parallel_schedule_applied_tasks =
                    result.stats.plan.parallel_schedule_task_count;
                return true;
            },
            .failed => return false,
            .fallback_serial, .not_applied => {},
        }
    }

    hash_join_mod.executeLeftJoinFlatInMemoryHash(
        result.rows,
        &result.row_count,
        left_rows,
        right_rows,
        .{
            .left_key_index = join.left_key_index,
            .right_key_index = join.right_key_index,
        },
        right_column_count,
        caps,
    ) catch |err| {
        setError(result, switch (err) {
            error.BuildRowCapacityExceeded => "join build row capacity exceeded",
            error.StateCapacityExceeded => "join state capacity exceeded",
            error.LeftKeyOutOfBounds, error.RightKeyOutOfBounds => "join key out of bounds",
            error.JoinColumnCapacityExceeded => "join column capacity exceeded",
            error.OutputRowCapacityExceeded => "join output row capacity exceeded",
        });
        return false;
    };
    return true;
}

const JoinParallelOutcome = enum {
    not_applied,
    fallback_serial,
    failed,
    applied,
};

const ParallelJoinError = enum(u8) {
    none = 0,
    join_key_oob,
    join_column_capacity,
};

const ParallelJoinCountWorker = struct {
    index: *const hash_join_mod.LeftHashIndex,
    left_rows: []const ResultRow,
    right_column_count: u16,
    join: JoinDescriptor,
    start_idx: u16,
    end_idx: u16,
    row_output_counts: *[scan_mod.scan_batch_size]u16,
    first_error_index: u16 = std.math.maxInt(u16),
    first_error_code: ParallelJoinError = .none,

    fn run(self: *ParallelJoinCountWorker) void {
        var row_idx = self.start_idx;
        while (row_idx < self.end_idx) : (row_idx += 1) {
            const left_row = self.left_rows[row_idx];
            if (self.join.left_key_index >= left_row.column_count) {
                self.first_error_index = row_idx;
                self.first_error_code = .join_key_oob;
                return;
            }
            const total_columns_with_right = @as(usize, left_row.column_count) +
                @as(usize, self.right_column_count);
            if (total_columns_with_right > scan_mod.max_columns) {
                self.first_error_index = row_idx;
                self.first_error_code = .join_column_capacity;
                return;
            }

            var matches: u16 = 0;
            var iter = self.index.matchIterator(left_row.values[self.join.left_key_index]);
            while (iter.next()) |right_row| {
                const total_columns = @as(usize, left_row.column_count) +
                    @as(usize, right_row.column_count);
                if (total_columns > scan_mod.max_columns) {
                    self.first_error_index = row_idx;
                    self.first_error_code = .join_column_capacity;
                    return;
                }
                matches +|= 1;
            }
            self.row_output_counts[row_idx] = if (matches == 0) 1 else matches;
        }
    }
};

const ParallelJoinWriteWorker = struct {
    index: *const hash_join_mod.LeftHashIndex,
    left_rows: []const ResultRow,
    right_column_count: u16,
    join: JoinDescriptor,
    start_idx: u16,
    end_idx: u16,
    row_offsets: *const [scan_mod.scan_batch_size]u16,
    out_rows: []ResultRow,
    first_error_index: u16 = std.math.maxInt(u16),
    first_error_code: ParallelJoinError = .none,

    fn run(self: *ParallelJoinWriteWorker) void {
        var row_idx = self.start_idx;
        while (row_idx < self.end_idx) : (row_idx += 1) {
            const left_row = self.left_rows[row_idx];
            if (self.join.left_key_index >= left_row.column_count) {
                self.first_error_index = row_idx;
                self.first_error_code = .join_key_oob;
                return;
            }

            var out_idx = self.row_offsets[row_idx];
            var matched = false;
            var iter = self.index.matchIterator(left_row.values[self.join.left_key_index]);
            while (iter.next()) |right_row| {
                matched = true;
                const total_columns = @as(usize, left_row.column_count) +
                    @as(usize, right_row.column_count);
                if (total_columns > scan_mod.max_columns) {
                    self.first_error_index = row_idx;
                    self.first_error_code = .join_column_capacity;
                    return;
                }

                var out = ResultRow.init();
                out.column_count = @intCast(total_columns);
                out.row_id = left_row.row_id;
                @memcpy(
                    out.values[0..left_row.column_count],
                    left_row.values[0..left_row.column_count],
                );
                @memcpy(
                    out.values[left_row.column_count..out.column_count],
                    right_row.values[0..right_row.column_count],
                );
                self.out_rows[out_idx] = out;
                out_idx += 1;
            }

            if (!matched) {
                const total_columns_with_right = @as(usize, left_row.column_count) +
                    @as(usize, self.right_column_count);
                if (total_columns_with_right > scan_mod.max_columns) {
                    self.first_error_index = row_idx;
                    self.first_error_code = .join_column_capacity;
                    return;
                }
                var out = ResultRow.init();
                out.column_count = @intCast(total_columns_with_right);
                out.row_id = left_row.row_id;
                @memcpy(
                    out.values[0..left_row.column_count],
                    left_row.values[0..left_row.column_count],
                );
                var right_col: u16 = 0;
                while (right_col < self.right_column_count) : (right_col += 1) {
                    out.values[left_row.column_count + right_col] = .{ .null_value = {} };
                }
                self.out_rows[out_idx] = out;
            }
        }
    }
};

fn applyParallelJoinError(result: *QueryResult, code: ParallelJoinError) void {
    switch (code) {
        .none => {},
        .join_key_oob => setError(result, "join key out of bounds"),
        .join_column_capacity => setError(result, "join column capacity exceeded"),
    }
}

fn tryExecuteLeftJoinHashFlatParallel(
    result: *QueryResult,
    left_rows: []const ResultRow,
    right_rows: []const ResultRow,
    join: JoinDescriptor,
    right_column_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
    parallel_worker_budget: u8,
    parallel_min_rows_per_worker: u16,
) JoinParallelOutcome {
    const min_rows_per_worker = @as(usize, @max(@as(u16, 1), parallel_min_rows_per_worker));
    if (left_rows.len < min_rows_per_worker * 2) return .not_applied;
    if (parallel_worker_budget < 2) return .not_applied;
    if (left_rows.len > scan_mod.scan_batch_size) return .failed;

    if (left_rows.len > 0 and join.left_key_index >= left_rows[0].column_count) {
        setError(result, "join key out of bounds");
        return .failed;
    }

    const index = hash_join_mod.LeftHashIndex.init(right_rows, .{
        .left_key_index = join.left_key_index,
        .right_key_index = join.right_key_index,
    }, caps) catch |err| {
        setError(result, switch (err) {
            error.BuildRowCapacityExceeded => "join build row capacity exceeded",
            error.StateCapacityExceeded => "join state capacity exceeded",
            error.LeftKeyOutOfBounds, error.RightKeyOutOfBounds => "join key out of bounds",
            error.JoinColumnCapacityExceeded => "join column capacity exceeded",
            error.OutputRowCapacityExceeded => "join output row capacity exceeded",
        });
        return .failed;
    };

    const budget_cap = @min(@as(usize, parallel_worker_budget), max_parallel_join_workers);
    const max_workers = @min(budget_cap, left_rows.len);
    var worker_count = @min(max_workers, left_rows.len / min_rows_per_worker);
    if (worker_count < 2) return .not_applied;
    if (worker_count > max_parallel_join_workers) worker_count = max_parallel_join_workers;

    var row_output_counts: [scan_mod.scan_batch_size]u16 = [_]u16{0} ** scan_mod.scan_batch_size;
    var row_offsets: [scan_mod.scan_batch_size]u16 = [_]u16{0} ** scan_mod.scan_batch_size;
    var count_workers: [max_parallel_join_workers]ParallelJoinCountWorker = undefined;
    var write_workers: [max_parallel_join_workers]ParallelJoinWriteWorker = undefined;
    var threads: [max_parallel_join_workers - 1]?std.Thread =
        [_]?std.Thread{null} ** (max_parallel_join_workers - 1);

    const base = left_rows.len / worker_count;
    const remainder = left_rows.len % worker_count;
    var start_idx: usize = 0;
    var worker_idx: usize = 0;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        const span = base + if (worker_idx < remainder) @as(usize, 1) else @as(usize, 0);
        const end_idx = start_idx + span;
        count_workers[worker_idx] = .{
            .index = &index,
            .left_rows = left_rows,
            .right_column_count = right_column_count,
            .join = join,
            .start_idx = @intCast(start_idx),
            .end_idx = @intCast(end_idx),
            .row_output_counts = &row_output_counts,
        };
        start_idx = end_idx;
    }

    var spawned: usize = 0;
    worker_idx = 1;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        threads[spawned] = std.Thread.spawn(
            .{},
            ParallelJoinCountWorker.run,
            .{&count_workers[worker_idx]},
        ) catch {
            var join_idx: usize = 0;
            while (join_idx < spawned) : (join_idx += 1) {
                threads[join_idx].?.join();
            }
            return .fallback_serial;
        };
        spawned += 1;
    }
    count_workers[0].run();
    var join_idx: usize = 0;
    while (join_idx < spawned) : (join_idx += 1) {
        threads[join_idx].?.join();
    }

    var first_error_index: u16 = std.math.maxInt(u16);
    var first_error_code: ParallelJoinError = .none;
    worker_idx = 0;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        if (count_workers[worker_idx].first_error_code == .none) continue;
        if (count_workers[worker_idx].first_error_index < first_error_index) {
            first_error_index = count_workers[worker_idx].first_error_index;
            first_error_code = count_workers[worker_idx].first_error_code;
        }
    }
    if (first_error_code != .none) {
        applyParallelJoinError(result, first_error_code);
        return .failed;
    }

    var total_outputs: usize = 0;
    var row_idx: usize = 0;
    while (row_idx < left_rows.len) : (row_idx += 1) {
        row_offsets[row_idx] = @intCast(total_outputs);
        total_outputs += row_output_counts[row_idx];
        if (total_outputs > result.rows.len or total_outputs > caps.join_output_rows) {
            setError(result, "join output row capacity exceeded");
            return .failed;
        }
    }
    result.row_count = @intCast(total_outputs);

    start_idx = 0;
    worker_idx = 0;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        const span = base + if (worker_idx < remainder) @as(usize, 1) else @as(usize, 0);
        const end_idx = start_idx + span;
        write_workers[worker_idx] = .{
            .index = &index,
            .left_rows = left_rows,
            .right_column_count = right_column_count,
            .join = join,
            .start_idx = @intCast(start_idx),
            .end_idx = @intCast(end_idx),
            .row_offsets = &row_offsets,
            .out_rows = result.rows[0..result.row_count],
        };
        start_idx = end_idx;
    }

    spawned = 0;
    worker_idx = 1;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        threads[spawned] = std.Thread.spawn(
            .{},
            ParallelJoinWriteWorker.run,
            .{&write_workers[worker_idx]},
        ) catch {
            var join_write_idx: usize = 0;
            while (join_write_idx < spawned) : (join_write_idx += 1) {
                threads[join_write_idx].?.join();
            }
            return .fallback_serial;
        };
        spawned += 1;
    }
    write_workers[0].run();
    join_idx = 0;
    while (join_idx < spawned) : (join_idx += 1) {
        threads[join_idx].?.join();
    }

    first_error_index = std.math.maxInt(u16);
    first_error_code = .none;
    worker_idx = 0;
    while (worker_idx < worker_count) : (worker_idx += 1) {
        if (write_workers[worker_idx].first_error_code == .none) continue;
        if (write_workers[worker_idx].first_error_index < first_error_index) {
            first_error_index = write_workers[worker_idx].first_error_index;
            first_error_code = write_workers[worker_idx].first_error_code;
        }
    }
    if (first_error_code != .none) {
        applyParallelJoinError(result, first_error_code);
        return .failed;
    }
    return .applied;
}

pub fn executeInnerJoinBounded(
    result: *QueryResult,
    left_rows: []const ResultRow,
    right_rows: []const ResultRow,
    join: JoinDescriptor,
    caps: *const capacity_mod.OperatorCapacities,
) bool {
    if (left_rows.len > caps.join_build_rows) {
        setError(result, "join build row capacity exceeded");
        return false;
    }
    const state_bytes = left_rows.len * (@sizeOf(Value) + @sizeOf(u16));
    if (state_bytes > caps.join_state_bytes) {
        setError(result, "join state capacity exceeded");
        return false;
    }
    if (left_rows.len > 0 and
        join.left_key_index >= left_rows[0].column_count)
    {
        setError(result, "join key out of bounds");
        return false;
    }
    if (right_rows.len > 0 and
        join.right_key_index >= right_rows[0].column_count)
    {
        setError(result, "join key out of bounds");
        return false;
    }

    result.row_count = 0;
    for (left_rows) |left_row| {
        const left_key = left_row.values[join.left_key_index];
        for (right_rows) |right_row| {
            if (compareValues(left_key, right_row.values[join.right_key_index]) !=
                .eq) continue;
            const total_columns = @as(usize, left_row.column_count) +
                @as(usize, right_row.column_count);
            if (total_columns > scan_mod.max_columns) {
                setError(result, "join column capacity exceeded");
                return false;
            }
            if (@as(usize, result.row_count) >= caps.join_output_rows) {
                setError(result, "join output row capacity exceeded");
                return false;
            }

            var out = ResultRow.init();
            out.column_count = @intCast(total_columns);
            out.row_id = left_row.row_id;
            @memcpy(
                out.values[0..left_row.column_count],
                left_row.values[0..left_row.column_count],
            );
            @memcpy(
                out.values[left_row.column_count..out.column_count],
                right_row.values[0..right_row.column_count],
            );
            result.rows[result.row_count] = out;
            result.row_count += 1;
        }
    }
    return true;
}

pub fn executeLeftJoinBounded(
    result: *QueryResult,
    left_rows: []const ResultRow,
    right_rows: []const ResultRow,
    join: JoinDescriptor,
    right_column_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
) bool {
    if (left_rows.len > caps.join_build_rows) {
        setError(result, "join build row capacity exceeded");
        return false;
    }
    const state_bytes = left_rows.len * (@sizeOf(Value) + @sizeOf(u16));
    if (state_bytes > caps.join_state_bytes) {
        setError(result, "join state capacity exceeded");
        return false;
    }
    if (left_rows.len > 0 and
        join.left_key_index >= left_rows[0].column_count)
    {
        setError(result, "join key out of bounds");
        return false;
    }
    if (right_rows.len > 0 and
        join.right_key_index >= right_rows[0].column_count)
    {
        setError(result, "join key out of bounds");
        return false;
    }

    result.row_count = 0;
    for (left_rows) |left_row| {
        var matched = false;
        const total_columns_with_right = @as(usize, left_row.column_count) +
            @as(usize, right_column_count);
        if (total_columns_with_right > scan_mod.max_columns) {
            setError(result, "join column capacity exceeded");
            return false;
        }

        const left_key = left_row.values[join.left_key_index];
        for (right_rows) |right_row| {
            if (compareValues(left_key, right_row.values[join.right_key_index]) !=
                .eq) continue;
            matched = true;
            const total_columns = @as(usize, left_row.column_count) +
                @as(usize, right_row.column_count);
            if (total_columns > scan_mod.max_columns) {
                setError(result, "join column capacity exceeded");
                return false;
            }
            if (@as(usize, result.row_count) >= caps.join_output_rows) {
                setError(result, "join output row capacity exceeded");
                return false;
            }

            var out = ResultRow.init();
            out.column_count = @intCast(total_columns);
            out.row_id = left_row.row_id;
            @memcpy(
                out.values[0..left_row.column_count],
                left_row.values[0..left_row.column_count],
            );
            @memcpy(
                out.values[left_row.column_count..out.column_count],
                right_row.values[0..right_row.column_count],
            );
            result.rows[result.row_count] = out;
            result.row_count += 1;
        }

        if (!matched) {
            if (@as(usize, result.row_count) >= caps.join_output_rows) {
                setError(result, "join output row capacity exceeded");
                return false;
            }

            var out = ResultRow.init();
            out.column_count = @intCast(total_columns_with_right);
            out.row_id = left_row.row_id;
            @memcpy(
                out.values[0..left_row.column_count],
                left_row.values[0..left_row.column_count],
            );
            var right_idx: u16 = 0;
            while (right_idx < right_column_count) : (right_idx += 1) {
                out.values[left_row.column_count + right_idx] = .{ .null_value = {} };
            }
            result.rows[result.row_count] = out;
            result.row_count += 1;
        }
    }
    return true;
}

fn setError(result: *QueryResult, msg: []const u8) void {
    result.has_error = true;
    @memset(&result.error_message, 0);
    const copy_len = @min(msg.len, result.error_message.len);
    @memcpy(result.error_message[0..copy_len], msg[0..copy_len]);
}

const testing = std.testing;

fn makeLeftRow(id: i64, label: []const u8) ResultRow {
    var row = ResultRow.init();
    row.column_count = 2;
    row.values[0] = .{ .i64 = id };
    row.values[1] = .{ .string = label };
    return row;
}

fn makeRightRow(id: i64, flag: bool) ResultRow {
    var row = ResultRow.init();
    row.column_count = 2;
    row.values[0] = .{ .i64 = id };
    row.values[1] = .{ .bool = flag };
    return row;
}

test "hash left join preserves deterministic left-major order" {
    const left = [_]ResultRow{
        makeLeftRow(1, "A"),
        makeLeftRow(2, "B"),
        makeLeftRow(1, "C"),
    };
    const right = [_]ResultRow{
        makeRightRow(1, true),
        makeRightRow(1, false),
        makeRightRow(2, true),
    };

    var result_rows: [scan_mod.scan_batch_size]ResultRow = undefined;
    var result = QueryResult.init(result_rows[0..]);
    const caps = capacity_mod.OperatorCapacities.defaults();
    const ok = executeLeftJoinHashFlat(
        &result,
        left[0..],
        right[0..],
        .{ .left_key_index = 0, .right_key_index = 0 },
        2,
        &caps,
        false,
        1,
        16,
    );

    try testing.expect(ok);
    try testing.expect(!result.has_error);
    try testing.expectEqual(@as(u16, 5), result.row_count);
    try testing.expectEqualSlices(u8, "A", result.rows[0].values[1].string);
    try testing.expectEqual(true, result.rows[0].values[3].bool);
    try testing.expectEqualSlices(u8, "A", result.rows[1].values[1].string);
    try testing.expectEqual(false, result.rows[1].values[3].bool);
    try testing.expectEqualSlices(u8, "B", result.rows[2].values[1].string);
    try testing.expectEqual(true, result.rows[2].values[3].bool);
    try testing.expectEqualSlices(u8, "C", result.rows[3].values[1].string);
    try testing.expectEqual(true, result.rows[3].values[3].bool);
    try testing.expectEqualSlices(u8, "C", result.rows[4].values[1].string);
    try testing.expectEqual(false, result.rows[4].values[3].bool);
}

test "hash left join emits null-extended row for unmatched key" {
    const left = [_]ResultRow{
        makeLeftRow(99, "Z"),
    };
    const right = [_]ResultRow{
        makeRightRow(1, true),
    };

    var result_rows: [scan_mod.scan_batch_size]ResultRow = undefined;
    var result = QueryResult.init(result_rows[0..]);
    const caps = capacity_mod.OperatorCapacities.defaults();
    const ok = executeLeftJoinHashFlat(
        &result,
        left[0..],
        right[0..],
        .{ .left_key_index = 0, .right_key_index = 0 },
        2,
        &caps,
        false,
        1,
        16,
    );

    try testing.expect(ok);
    try testing.expectEqual(@as(u16, 1), result.row_count);
    try testing.expect(result.rows[0].values[2] == .null_value);
    try testing.expect(result.rows[0].values[3] == .null_value);
}
