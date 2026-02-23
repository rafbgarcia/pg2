//! In-memory hash join primitives for executor operator paths.
//!
//! This module provides deterministic flat-row hash join behavior without
//! runtime allocator use. Spill-aware paths will extend these contracts.
const std = @import("std");
const row_mod = @import("../storage/row.zig");
const scan_mod = @import("scan.zig");
const capacity_mod = @import("capacity.zig");

const Value = row_mod.Value;
const compareValues = row_mod.compareValues;
const ResultRow = scan_mod.ResultRow;

pub const JoinDescriptor = struct {
    left_key_index: u16,
    right_key_index: u16,
};

pub const JoinError = error{
    BuildRowCapacityExceeded,
    StateCapacityExceeded,
    LeftKeyOutOfBounds,
    RightKeyOutOfBounds,
    JoinColumnCapacityExceeded,
    OutputRowCapacityExceeded,
};

const hash_table_size: usize = 8192;
const empty_slot: u16 = std.math.maxInt(u16);
const empty_next: u16 = std.math.maxInt(u16);

pub const LeftHashIndex = struct {
    right_rows: []const ResultRow,
    join: JoinDescriptor,
    buckets: [hash_table_size]u16 = [_]u16{empty_slot} ** hash_table_size,
    chain_next: [scan_mod.scan_batch_size]u16 = [_]u16{empty_next} ** scan_mod.scan_batch_size,
    chain_tail: [hash_table_size]u16 = [_]u16{empty_slot} ** hash_table_size,

    pub fn init(
        right_rows: []const ResultRow,
        join: JoinDescriptor,
        caps: *const capacity_mod.OperatorCapacities,
    ) JoinError!LeftHashIndex {
        if (right_rows.len > caps.join_build_rows) return error.BuildRowCapacityExceeded;
        if (right_rows.len > scan_mod.scan_batch_size) return error.BuildRowCapacityExceeded;
        const state_bytes = right_rows.len * (@sizeOf(Value) + @sizeOf(u16) * 2);
        if (state_bytes > caps.join_state_bytes) return error.StateCapacityExceeded;
        if (right_rows.len > 0 and join.right_key_index >= right_rows[0].column_count) {
            return error.RightKeyOutOfBounds;
        }

        var idx = LeftHashIndex{
            .right_rows = right_rows,
            .join = join,
        };
        var right_index: u16 = 0;
        while (right_index < right_rows.len) : (right_index += 1) {
            const right_row = right_rows[right_index];
            const right_key = right_row.values[join.right_key_index];
            const slot = hashKey(right_key) & (hash_table_size - 1);
            if (idx.buckets[slot] == empty_slot) {
                idx.buckets[slot] = right_index;
                idx.chain_tail[slot] = right_index;
            } else {
                const tail = idx.chain_tail[slot];
                std.debug.assert(tail != empty_slot);
                idx.chain_next[tail] = right_index;
                idx.chain_tail[slot] = right_index;
            }
        }
        return idx;
    }

    pub fn matchIterator(self: *const LeftHashIndex, left_key: Value) MatchIterator {
        const slot = hashKey(left_key) & (hash_table_size - 1);
        return .{
            .index = self,
            .left_key = left_key,
            .cursor = self.buckets[slot],
        };
    }
};

pub const MatchIterator = struct {
    index: *const LeftHashIndex,
    left_key: Value,
    cursor: u16,

    pub fn next(self: *MatchIterator) ?*const ResultRow {
        while (self.cursor != empty_slot) : (self.cursor = self.index.chain_next[self.cursor]) {
            const right_row = &self.index.right_rows[self.cursor];
            if (compareValues(self.left_key, right_row.values[self.index.join.right_key_index]) != .eq) {
                continue;
            }
            self.cursor = self.index.chain_next[self.cursor];
            return right_row;
        }
        return null;
    }
};

pub fn executeLeftJoinFlatInMemoryHash(
    out_rows: []ResultRow,
    out_count: *u16,
    left_rows: []const ResultRow,
    right_rows: []const ResultRow,
    join: JoinDescriptor,
    right_column_count: u16,
    caps: *const capacity_mod.OperatorCapacities,
) JoinError!void {
    if (left_rows.len > 0 and join.left_key_index >= left_rows[0].column_count) {
        return error.LeftKeyOutOfBounds;
    }
    const index = try LeftHashIndex.init(right_rows, join, caps);

    var emitted: u16 = 0;
    for (left_rows) |left_row| {
        const total_columns_with_right = @as(usize, left_row.column_count) +
            @as(usize, right_column_count);
        if (total_columns_with_right > scan_mod.max_columns) {
            return error.JoinColumnCapacityExceeded;
        }

        const left_key = left_row.values[join.left_key_index];
        var matched = false;
        var matches = index.matchIterator(left_key);
        while (matches.next()) |right_row| {
            matched = true;
            const total_columns = @as(usize, left_row.column_count) +
                @as(usize, right_row.column_count);
            if (total_columns > scan_mod.max_columns) {
                return error.JoinColumnCapacityExceeded;
            }
            if (emitted >= out_rows.len or @as(usize, emitted) >= caps.join_output_rows) {
                return error.OutputRowCapacityExceeded;
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
            out_rows[emitted] = out;
            emitted += 1;
        }

        if (!matched) {
            if (emitted >= out_rows.len or @as(usize, emitted) >= caps.join_output_rows) {
                return error.OutputRowCapacityExceeded;
            }

            var out = ResultRow.init();
            out.column_count = @intCast(total_columns_with_right);
            out.row_id = left_row.row_id;
            @memcpy(
                out.values[0..left_row.column_count],
                left_row.values[0..left_row.column_count],
            );
            var right_col: u16 = 0;
            while (right_col < right_column_count) : (right_col += 1) {
                out.values[left_row.column_count + right_col] = .{ .null_value = {} };
            }
            out_rows[emitted] = out;
            emitted += 1;
        }
    }
    out_count.* = emitted;
}

fn hashKey(value: Value) usize {
    var hasher = std.hash.Wyhash.init(0);
    switch (value) {
        .null_value => hasher.update(&[_]u8{0}),
        .bool => |v| {
            hasher.update(&[_]u8{1});
            hasher.update(&[_]u8{@intFromBool(v)});
        },
        .i8 => |v| {
            hasher.update(&[_]u8{2});
            hasher.update(std.mem.asBytes(&v));
        },
        .i16 => |v| {
            hasher.update(&[_]u8{3});
            hasher.update(std.mem.asBytes(&v));
        },
        .i32 => |v| {
            hasher.update(&[_]u8{4});
            hasher.update(std.mem.asBytes(&v));
        },
        .i64 => |v| {
            hasher.update(&[_]u8{5});
            hasher.update(std.mem.asBytes(&v));
        },
        .u8 => |v| {
            hasher.update(&[_]u8{6});
            hasher.update(std.mem.asBytes(&v));
        },
        .u16 => |v| {
            hasher.update(&[_]u8{7});
            hasher.update(std.mem.asBytes(&v));
        },
        .u32 => |v| {
            hasher.update(&[_]u8{8});
            hasher.update(std.mem.asBytes(&v));
        },
        .u64 => |v| {
            hasher.update(&[_]u8{9});
            hasher.update(std.mem.asBytes(&v));
        },
        .f64 => |v| {
            hasher.update(&[_]u8{10});
            hasher.update(std.mem.asBytes(&v));
        },
        .string => |v| {
            hasher.update(&[_]u8{11});
            hasher.update(v);
        },
        .timestamp => |v| {
            hasher.update(&[_]u8{12});
            hasher.update(std.mem.asBytes(&v));
        },
    }
    return @intCast(hasher.final());
}
