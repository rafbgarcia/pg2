//! In-memory hash join primitives for executor operator paths.
//!
//! This module provides deterministic flat-row hash join behavior without
//! runtime allocator use. Spill-aware paths will extend these contracts.
const std = @import("std");
const row_mod = @import("../storage/row.zig");
const scan_mod = @import("scan.zig");
const capacity_mod = @import("capacity.zig");
const temp_mod = @import("../storage/temp.zig");
const page_mod = @import("../storage/page.zig");
const spill_row_mod = @import("../storage/spill_row.zig");
const spill_collector_mod = @import("spill_collector.zig");

const Value = row_mod.Value;
const compareValues = row_mod.compareValues;
const ResultRow = scan_mod.ResultRow;
const StringArena = scan_mod.StringArena;
const TempStorageManager = temp_mod.TempStorageManager;
const TempPage = temp_mod.TempPage;
const SpillPageWriter = spill_row_mod.SpillPageWriter;
const SpillPageReader = spill_row_mod.SpillPageReader;
const max_spill_pages = spill_collector_mod.max_spill_pages;

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
pub const max_partitions: u8 = 16;

pub const SpillPartitionError =
    spill_row_mod.SpillError ||
    temp_mod.TempAllocatorError ||
    temp_mod.TempPageError ||
    temp_mod.TempStorageError ||
    page_mod.PageDeserializeError ||
    error{
        InvalidPartitionCount,
        RightKeyOutOfBounds,
        SpillPageBudgetExceeded,
    };

pub const PartitionSpillDescriptor = struct {
    partition_count: u8,
    page_ids: [max_spill_pages]u64 = undefined,
    page_partition: [max_spill_pages]u8 = undefined,
    page_count: u32 = 0,
    partition_row_counts: [max_partitions]u64 = [_]u64{0} ** max_partitions,

    pub fn init(partition_count: u8) SpillPartitionError!PartitionSpillDescriptor {
        if (partition_count == 0 or partition_count > max_partitions) {
            return error.InvalidPartitionCount;
        }
        return .{
            .partition_count = partition_count,
        };
    }
};

pub const PartitionRowIterator = struct {
    temp_mgr: *TempStorageManager,
    descriptor: *const PartitionSpillDescriptor,
    partition: u8,
    page_index: u32 = 0,
    reader_loaded: bool = false,
    reader: SpillPageReader = undefined,
    current_page: page_mod.Page = undefined,

    pub fn init(
        temp_mgr: *TempStorageManager,
        descriptor: *const PartitionSpillDescriptor,
        partition: u8,
    ) SpillPartitionError!PartitionRowIterator {
        if (partition >= descriptor.partition_count) return error.InvalidPartitionCount;
        return .{
            .temp_mgr = temp_mgr,
            .descriptor = descriptor,
            .partition = partition,
        };
    }

    pub fn next(
        self: *PartitionRowIterator,
        out: *ResultRow,
        arena: *StringArena,
    ) SpillPartitionError!bool {
        while (true) {
            if (self.reader_loaded) {
                const has_row = self.reader.next(out, arena) catch return error.SpillError;
                if (has_row) return true;
                self.reader_loaded = false;
            }

            var found_next_page = false;
            while (self.page_index < self.descriptor.page_count) : (self.page_index += 1) {
                if (self.descriptor.page_partition[self.page_index] != self.partition) {
                    continue;
                }
                const page_id = self.descriptor.page_ids[self.page_index];
                self.page_index += 1;
                const read_result = try self.temp_mgr.readPage(page_id);
                self.current_page = read_result.page;
                const chunk = try TempPage.readChunk(&self.current_page);
                self.reader = try SpillPageReader.init(chunk.payload);
                self.reader_loaded = true;
                found_next_page = true;
                break;
            }
            if (!found_next_page) return false;
        }
    }
};

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

pub fn partitionForKey(key: Value, partition_count: u8) SpillPartitionError!u8 {
    if (partition_count == 0 or partition_count > max_partitions) {
        return error.InvalidPartitionCount;
    }
    return @intCast(hashKey(key) % partition_count);
}

pub fn spillRowsByPartition(
    temp_mgr: *TempStorageManager,
    rows: []const ResultRow,
    right_key_index: u16,
    descriptor: *PartitionSpillDescriptor,
) SpillPartitionError!void {
    if (rows.len > 0 and right_key_index >= rows[0].column_count) {
        return error.RightKeyOutOfBounds;
    }

    var writers: [max_partitions]SpillPageWriter = undefined;
    for (0..descriptor.partition_count) |p| {
        writers[p] = SpillPageWriter.init();
        descriptor.partition_row_counts[p] = 0;
    }
    descriptor.page_count = 0;

    for (rows) |row| {
        const partition = try partitionForKey(
            row.values[right_key_index],
            descriptor.partition_count,
        );
        descriptor.partition_row_counts[partition] += 1;
        const appended = writers[partition].appendRow(&row) catch return error.SpillError;
        if (!appended) {
            if (descriptor.page_count >= max_spill_pages) return error.SpillPageBudgetExceeded;
            const payload = writers[partition].finalize();
            const page_id = try temp_mgr.allocateAndWrite(payload, TempPage.null_page_id);
            descriptor.page_ids[descriptor.page_count] = page_id;
            descriptor.page_partition[descriptor.page_count] = partition;
            descriptor.page_count += 1;

            writers[partition].reset();
            const retry = writers[partition].appendRow(&row) catch return error.SpillError;
            std.debug.assert(retry);
        }
    }

    for (0..descriptor.partition_count) |p| {
        if (writers[p].row_count == 0) continue;
        if (descriptor.page_count >= max_spill_pages) return error.SpillPageBudgetExceeded;
        const payload = writers[p].finalize();
        const page_id = try temp_mgr.allocateAndWrite(payload, TempPage.null_page_id);
        descriptor.page_ids[descriptor.page_count] = page_id;
        descriptor.page_partition[descriptor.page_count] = @intCast(p);
        descriptor.page_count += 1;
    }
}

const testing = std.testing;
const disk_mod = @import("../simulator/disk.zig");

fn makeRow(id: i64, payload: []const u8) ResultRow {
    var row = ResultRow.init();
    row.column_count = 2;
    row.values[0] = .{ .i64 = id };
    row.values[1] = .{ .string = payload };
    return row;
}

fn rowEquals(a: *const ResultRow, b: *const ResultRow) bool {
    if (a.column_count != b.column_count) return false;
    var i: u16 = 0;
    while (i < a.column_count) : (i += 1) {
        if (compareValues(a.values[i], b.values[i]) != .eq) return false;
    }
    return true;
}

test "partitionForKey is deterministic for fixed partition count" {
    const key: Value = .{ .i64 = 42 };
    const p1 = try partitionForKey(key, 8);
    const p2 = try partitionForKey(key, 8);
    try testing.expectEqual(p1, p2);
}

test "spillRowsByPartition and PartitionRowIterator preserve per-partition row order" {
    var disk = disk_mod.SimulatedDisk.init(testing.allocator);
    defer disk.deinit();
    var temp_mgr = try TempStorageManager.init(0, disk.storage(), 128, 70_000_000);

    const rows = [_]ResultRow{
        makeRow(1, "a"),
        makeRow(2, "b"),
        makeRow(3, "c"),
        makeRow(4, "d"),
        makeRow(5, "e"),
        makeRow(6, "f"),
        makeRow(7, "g"),
        makeRow(8, "h"),
    };

    var desc = try PartitionSpillDescriptor.init(4);
    try spillRowsByPartition(
        &temp_mgr,
        rows[0..],
        0,
        &desc,
    );

    try testing.expect(desc.page_count > 0);

    for (0..desc.partition_count) |partition| {
        var expected_idx: [rows.len]u16 = undefined;
        var expected_count: u16 = 0;
        for (rows, 0..) |row, idx| {
            const p = try partitionForKey(row.values[0], desc.partition_count);
            if (p == partition) {
                expected_idx[expected_count] = @intCast(idx);
                expected_count += 1;
            }
        }
        try testing.expectEqual(
            @as(u64, expected_count),
            desc.partition_row_counts[partition],
        );

        var iter = try PartitionRowIterator.init(
            &temp_mgr,
            &desc,
            @intCast(partition),
        );
        var decode_arena_buf: [scan_mod.max_row_size_bytes * 2]u8 = undefined;
        var decode_arena = StringArena.init(&decode_arena_buf);
        var out = ResultRow.init();
        var got: u16 = 0;
        while (true) {
            decode_arena.reset();
            const has_row = try iter.next(&out, &decode_arena);
            if (!has_row) break;
            try testing.expect(got < expected_count);
            const expected = rows[expected_idx[got]];
            try testing.expect(rowEquals(&out, &expected));
            got += 1;
        }
        try testing.expectEqual(expected_count, got);
    }
}
