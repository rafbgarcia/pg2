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
) bool {
    result.row_count = 0;
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
    );

    try testing.expect(ok);
    try testing.expectEqual(@as(u16, 1), result.row_count);
    try testing.expect(result.rows[0].values[2] == .null_value);
    try testing.expect(result.rows[0].values[3] == .null_value);
}
