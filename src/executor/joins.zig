//! Nested-loop join implementations with bounded capacity contracts.
//!
//! Provides inner and left join execution over pre-materialized row buffers.
//! All joins enforce explicit capacity limits to keep memory usage bounded.
const std = @import("std");
const row_mod = @import("../storage/row.zig");
const scan_mod = @import("scan.zig");
const capacity_mod = @import("capacity.zig");

const Value = row_mod.Value;
const compareValues = row_mod.compareValues;
const ResultRow = scan_mod.ResultRow;
const QueryResult = @import("executor.zig").QueryResult;

pub const JoinDescriptor = struct {
    left_key_index: u16,
    right_key_index: u16,
};

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
