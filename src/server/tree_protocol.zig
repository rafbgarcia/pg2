//! Tree protocol: nested relation serialization, row grouping, and shape output.
//!
//! Handles the structured wire format for queries with nested relations,
//! converting flat executor rows into grouped tree-shaped protocol output.
const std = @import("std");
const session = @import("session.zig");
const row_mod = @import("../storage/row.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const ast_mod = @import("../parser/ast.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const exec_mod = @import("../executor/executor.zig");
const scan_mod = @import("../executor/scan.zig");

const Value = row_mod.Value;
const ColumnType = row_mod.ColumnType;
const compareValues = row_mod.compareValues;
const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const null_model = catalog_mod.null_model;
const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const null_node = ast_mod.null_node;
const serializeValue = session.serializeValue;
const ResultRow = scan_mod.ResultRow;
const StringArena = scan_mod.StringArena;

const max_protocol_fields = row_mod.max_columns;

const SelectionEntryKind = enum {
    scalar,
    nested,
};

pub const SelectionEntry = struct {
    kind: SelectionEntryKind,
    token_idx: u16,
    column_pos: u16,
};

pub const TreeProjection = struct {
    root_model_id: ModelId,
    entry_count: u16 = 0,
    entries: [max_protocol_fields]SelectionEntry = undefined,
    root_scalar_count: u16 = 0,
    root_scalar_positions: [max_protocol_fields]u16 = undefined,
    nested_field_token: u16 = 0,
    nested_model_id: ModelId = 0,
    nested_scalar_count: u16 = 0,
    nested_scalar_tokens: [max_protocol_fields]u16 = undefined,
    nested_scalar_positions: [max_protocol_fields]u16 = undefined,
};

pub fn serializeTreeProtocol(
    writer: anytype,
    result: *const exec_mod.QueryResult,
    projection: *const TreeProjection,
    catalog: *const Catalog,
    tokens: *const tokenizer_mod.TokenizeResult,
    source: []const u8,
) error{ResponseTooLarge}!void {
    try writeShape(writer, projection, catalog, tokens, source);

    if (result.collector) |_| {
        try serializeTreeProtocolCollector(writer, result, projection);
        return;
    }

    if (result.row_count == 0) return;

    var row_index: u16 = 0;
    while (row_index < result.row_count) {
        const group_start = row_index;
        var group_end = row_index + 1;
        while (group_end < result.row_count and
            rowsShareRoot(result, projection, group_start, group_end))
        {
            group_end += 1;
        }

        var entry_index: u16 = 0;
        while (entry_index < projection.entry_count) : (entry_index += 1) {
            if (entry_index > 0) {
                writer.writeAll(",") catch return error.ResponseTooLarge;
            }
            const entry = projection.entries[entry_index];
            switch (entry.kind) {
                .scalar => try writeProtocolValue(
                    writer,
                    result.rows[group_start].values[entry.column_pos],
                ),
                .nested => try writeNestedList(
                    writer,
                    result,
                    projection,
                    group_start,
                    group_end,
                ),
            }
        }
        writer.writeAll("\n") catch return error.ResponseTooLarge;
        row_index = group_end;
    }
}

pub fn buildTreeProjection(
    ast: *const Ast,
    tokens: *const tokenizer_mod.TokenizeResult,
    source: []const u8,
    catalog: *const Catalog,
) ?TreeProjection {
    const pipeline = getFinalPipeline(ast) orelse return null;
    const pipeline_node = ast.getNode(pipeline);
    if (pipeline_node.tag != .pipeline) return null;
    const source_node = ast.getNode(pipeline_node.data.binary.lhs);
    if (source_node.tag != .pipe_source) return null;

    const source_model_name = tokens.getText(source_node.data.token, source);
    const source_model_id = catalog.findModel(source_model_name) orelse return null;
    const selection = pipeline_node.extra;
    if (selection == 0 or selection >= ast.node_count) return null;
    const selection_node = ast.getNode(selection);
    if (selection_node.tag != .selection_set) return null;

    var projection = TreeProjection{
        .root_model_id = source_model_id,
    };

    const source_schema = &catalog.models[source_model_id].row_schema;
    var projection_col_pos: u16 = 0;
    var nested_count: u16 = 0;
    var field = selection_node.data.unary;
    while (field != null_node) {
        const node = ast.getNode(field);
        switch (node.tag) {
            .select_field => {
                const field_name = tokens.getText(node.data.token, source);
                _ = source_schema.findColumn(field_name) orelse return null;
                projection.entries[projection.entry_count] = .{
                    .kind = .scalar,
                    .token_idx = node.data.token,
                    .column_pos = projection_col_pos,
                };
                projection.entry_count += 1;
                projection.root_scalar_positions[projection.root_scalar_count] = projection_col_pos;
                projection.root_scalar_count += 1;
                projection_col_pos += 1;
            },
            .select_nested => {
                if (nested_count > 0) return null;
                nested_count += 1;

                const relation_name = tokens.getText(node.extra, source);
                const assoc_id = catalog.findAssociation(source_model_id, relation_name) orelse
                    return null;
                const assoc = &catalog.models[source_model_id].associations[assoc_id];
                if (assoc.target_model_id == null_model) return null;

                projection.entries[projection.entry_count] = .{
                    .kind = .nested,
                    .token_idx = node.extra,
                    .column_pos = 0,
                };
                projection.entry_count += 1;
                projection.nested_field_token = node.extra;
                projection.nested_model_id = assoc.target_model_id;

                const nested_selection = getNestedSelection(ast, field) orelse return null;
                var nested_field = ast.getNode(nested_selection).data.unary;
                while (nested_field != null_node) {
                    const nested_node = ast.getNode(nested_field);
                    if (nested_node.tag != .select_field) return null;
                    projection.nested_scalar_tokens[projection.nested_scalar_count] = nested_node.data.token;
                    projection.nested_scalar_positions[projection.nested_scalar_count] = projection_col_pos;
                    projection.nested_scalar_count += 1;
                    projection_col_pos += 1;
                    nested_field = nested_node.next;
                }
            },
            else => return null,
        }
        field = node.next;
    }

    if (nested_count != 1) return null;
    if (projection.root_scalar_count == 0) return null;
    if (projection.entry_count == 0 or projection.nested_scalar_count == 0) return null;
    return projection;
}

pub fn countProtocolRootRows(
    result: *const exec_mod.QueryResult,
    projection: *const TreeProjection,
) u16 {
    if (result.collector) |_| return countProtocolRootRowsCollector(result, projection);
    if (result.row_count == 0) return 0;
    var root_count: u16 = 0;
    var row_idx: u16 = 0;
    while (row_idx < result.row_count) {
        root_count += 1;
        var next_idx = row_idx + 1;
        while (next_idx < result.row_count and
            rowsShareRoot(result, projection, row_idx, next_idx))
        {
            next_idx += 1;
        }
        row_idx = next_idx;
    }
    return root_count;
}

fn countProtocolRootRowsCollector(
    result: *const exec_mod.QueryResult,
    projection: *const TreeProjection,
) u16 {
    const collector = result.collector orelse return 0;
    var iter = collector.iterator();
    var arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
    var arena = StringArena.init(&arena_buf);
    var row = ResultRow.init();
    var prev = ResultRow.init();
    var have_prev = false;
    var root_count: u16 = 0;
    var skip = result.collector_output_offset;
    var remaining = result.collector_output_count;

    while (remaining > 0) {
        arena.reset();
        const has_row = iter.next(&row, &arena) catch break;
        if (!has_row) break;
        if (skip > 0) {
            skip -= 1;
            continue;
        }
        remaining -= 1;

        if (!have_prev) {
            root_count +|= 1;
            prev = row;
            have_prev = true;
            continue;
        }
        if (!rowsShareRootValues(&prev, &row, projection)) {
            root_count +|= 1;
            prev = row;
        }
    }
    return root_count;
}

fn serializeTreeProtocolCollector(
    writer: anytype,
    result: *const exec_mod.QueryResult,
    projection: *const TreeProjection,
) error{ResponseTooLarge}!void {
    const collector = result.collector orelse return;
    if (result.collector_output_count == 0) return;

    const nested_idx = findNestedEntryIndex(projection) orelse return;
    var iter = collector.iterator();
    var arena_buf: [scan_mod.max_row_size_bytes + 192]u8 = undefined;
    var arena = StringArena.init(&arena_buf);

    var row = ResultRow.init();
    var current_root = ResultRow.init();
    var has_group = false;
    var emitted_any_nested = false;
    var skip = result.collector_output_offset;
    var remaining = result.collector_output_count;

    while (remaining > 0) {
        arena.reset();
        const has_row = iter.next(&row, &arena) catch break;
        if (!has_row) break;
        if (skip > 0) {
            skip -= 1;
            continue;
        }
        remaining -= 1;

        if (!has_group) {
            current_root = row;
            try writeRootPrefixUntilNested(writer, &row, projection, nested_idx);
            emitted_any_nested = false;
            if (!isNestedNullRow(row.values[0..], projection)) {
                try writeNestedEntry(writer, &row, projection);
                emitted_any_nested = true;
            }
            has_group = true;
            continue;
        }

        if (rowsShareRootValues(&current_root, &row, projection)) {
            if (!isNestedNullRow(row.values[0..], projection)) {
                if (emitted_any_nested) {
                    writer.writeAll(",") catch return error.ResponseTooLarge;
                }
                try writeNestedEntry(writer, &row, projection);
                emitted_any_nested = true;
            }
        } else {
            try writeRootSuffixAfterNested(
                writer,
                &current_root,
                projection,
                nested_idx,
            );
            current_root = row;
            try writeRootPrefixUntilNested(writer, &row, projection, nested_idx);
            emitted_any_nested = false;
            if (!isNestedNullRow(row.values[0..], projection)) {
                try writeNestedEntry(writer, &row, projection);
                emitted_any_nested = true;
            }
        }
    }

    if (has_group) {
        try writeRootSuffixAfterNested(
            writer,
            &current_root,
            projection,
            nested_idx,
        );
    }
}

fn findNestedEntryIndex(projection: *const TreeProjection) ?u16 {
    var i: u16 = 0;
    while (i < projection.entry_count) : (i += 1) {
        if (projection.entries[i].kind == .nested) return i;
    }
    return null;
}

fn rowsShareRootValues(
    lhs: *const ResultRow,
    rhs: *const ResultRow,
    projection: *const TreeProjection,
) bool {
    var i: u16 = 0;
    while (i < projection.root_scalar_count) : (i += 1) {
        const pos = projection.root_scalar_positions[i];
        if (compareValues(lhs.values[pos], rhs.values[pos]) != .eq) return false;
    }
    return true;
}

fn writeRootPrefixUntilNested(
    writer: anytype,
    row: *const ResultRow,
    projection: *const TreeProjection,
    nested_entry_idx: u16,
) error{ResponseTooLarge}!void {
    var entry_index: u16 = 0;
    while (entry_index <= nested_entry_idx) : (entry_index += 1) {
        if (entry_index > 0) {
            writer.writeAll(",") catch return error.ResponseTooLarge;
        }
        const entry = projection.entries[entry_index];
        switch (entry.kind) {
            .scalar => try writeProtocolValue(
                writer,
                row.values[entry.column_pos],
            ),
            .nested => writer.writeAll("[") catch return error.ResponseTooLarge,
        }
    }
}

fn writeRootSuffixAfterNested(
    writer: anytype,
    row: *const ResultRow,
    projection: *const TreeProjection,
    nested_entry_idx: u16,
) error{ResponseTooLarge}!void {
    writer.writeAll("]") catch return error.ResponseTooLarge;
    var entry_index = nested_entry_idx + 1;
    while (entry_index < projection.entry_count) : (entry_index += 1) {
        writer.writeAll(",") catch return error.ResponseTooLarge;
        const entry = projection.entries[entry_index];
        std.debug.assert(entry.kind == .scalar);
        try writeProtocolValue(writer, row.values[entry.column_pos]);
    }
    writer.writeAll("\n") catch return error.ResponseTooLarge;
}

fn writeNestedEntry(
    writer: anytype,
    row: *const ResultRow,
    projection: *const TreeProjection,
) error{ResponseTooLarge}!void {
    writer.writeAll("[") catch return error.ResponseTooLarge;
    var col_idx: u16 = 0;
    while (col_idx < projection.nested_scalar_count) : (col_idx += 1) {
        if (col_idx > 0) {
            writer.writeAll(",") catch return error.ResponseTooLarge;
        }
        const pos = projection.nested_scalar_positions[col_idx];
        try writeProtocolValue(writer, row.values[pos]);
    }
    writer.writeAll("]") catch return error.ResponseTooLarge;
}

fn getFinalPipeline(ast: *const Ast) ?NodeIndex {
    if (ast.root == null_node) return null;
    const root = ast.getNode(ast.root);
    if (root.tag != .root) return null;
    var stmt = root.data.unary;
    var last_pipeline: NodeIndex = null_node;
    while (stmt != null_node) {
        const current = ast.getNode(stmt);
        if (current.tag == .pipeline) last_pipeline = stmt;
        if (current.tag == .let_binding and current.data.unary != null_node) {
            const bound = ast.getNode(current.data.unary);
            if (bound.tag == .pipeline) {
                last_pipeline = current.data.unary;
            }
        }
        stmt = current.next;
    }
    if (last_pipeline == null_node) return null;
    return last_pipeline;
}

fn getNestedSelection(ast: *const Ast, nested_node: NodeIndex) ?NodeIndex {
    const nested = ast.getNode(nested_node);
    if (nested.tag != .select_nested) return null;
    if (nested.data.unary == null_node) return null;
    const nested_pipeline = ast.getNode(nested.data.unary);
    if (nested_pipeline.tag != .pipeline) return null;
    const selection = nested_pipeline.extra;
    if (selection == 0 or selection >= ast.node_count) return null;
    if (ast.getNode(selection).tag != .selection_set) return null;
    return selection;
}

fn rowsShareRoot(
    result: *const exec_mod.QueryResult,
    projection: *const TreeProjection,
    lhs_idx: u16,
    rhs_idx: u16,
) bool {
    const lhs = result.rows[lhs_idx];
    const rhs = result.rows[rhs_idx];
    var i: u16 = 0;
    while (i < projection.root_scalar_count) : (i += 1) {
        const pos = projection.root_scalar_positions[i];
        if (compareValues(lhs.values[pos], rhs.values[pos]) != .eq) return false;
    }
    return true;
}

fn writeShape(
    writer: anytype,
    projection: *const TreeProjection,
    catalog: *const Catalog,
    tokens: *const tokenizer_mod.TokenizeResult,
    source: []const u8,
) error{ResponseTooLarge}!void {
    writer.writeAll("{") catch return error.ResponseTooLarge;
    const root_schema = &catalog.models[projection.root_model_id].row_schema;
    const nested_schema = &catalog.models[projection.nested_model_id].row_schema;

    var entry_index: u16 = 0;
    while (entry_index < projection.entry_count) : (entry_index += 1) {
        if (entry_index > 0) {
            writer.writeAll(",") catch return error.ResponseTooLarge;
        }
        const entry = projection.entries[entry_index];
        switch (entry.kind) {
            .scalar => {
                const field_name = tokens.getText(entry.token_idx, source);
                const col_idx = root_schema.findColumn(field_name) orelse
                    return error.ResponseTooLarge;
                const col = root_schema.columns[col_idx];
                writer.print(
                    "{s}:{s}",
                    .{ field_name, protocolTypeName(col.column_type) },
                ) catch return error.ResponseTooLarge;
            },
            .nested => {
                const relation_name = tokens.getText(projection.nested_field_token, source);
                writer.print("{s}:[{{", .{relation_name}) catch return error.ResponseTooLarge;
                var nested_i: u16 = 0;
                while (nested_i < projection.nested_scalar_count) : (nested_i += 1) {
                    if (nested_i > 0) {
                        writer.writeAll(",") catch return error.ResponseTooLarge;
                    }
                    const field_name = tokens.getText(
                        projection.nested_scalar_tokens[nested_i],
                        source,
                    );
                    const col_idx = nested_schema.findColumn(field_name) orelse
                        return error.ResponseTooLarge;
                    const col = nested_schema.columns[col_idx];
                    writer.print(
                        "{s}:{s}",
                        .{ field_name, protocolTypeName(col.column_type) },
                    ) catch return error.ResponseTooLarge;
                }
                writer.writeAll("}]") catch return error.ResponseTooLarge;
            },
        }
    }
    writer.writeAll("}\n") catch return error.ResponseTooLarge;
}

fn writeNestedList(
    writer: anytype,
    result: *const exec_mod.QueryResult,
    projection: *const TreeProjection,
    start_row: u16,
    end_row: u16,
) error{ResponseTooLarge}!void {
    writer.writeAll("[") catch return error.ResponseTooLarge;
    var emitted_any = false;
    var row_idx = start_row;
    while (row_idx < end_row) : (row_idx += 1) {
        if (isNestedNullRow(result.rows[row_idx].values[0..], projection)) continue;
        if (emitted_any) {
            writer.writeAll(",") catch return error.ResponseTooLarge;
        }
        emitted_any = true;
        writer.writeAll("[") catch return error.ResponseTooLarge;
        var col_idx: u16 = 0;
        while (col_idx < projection.nested_scalar_count) : (col_idx += 1) {
            if (col_idx > 0) {
                writer.writeAll(",") catch return error.ResponseTooLarge;
            }
            const pos = projection.nested_scalar_positions[col_idx];
            try writeProtocolValue(writer, result.rows[row_idx].values[pos]);
        }
        writer.writeAll("]") catch return error.ResponseTooLarge;
    }
    writer.writeAll("]") catch return error.ResponseTooLarge;
}

fn isNestedNullRow(
    row_values: []const Value,
    projection: *const TreeProjection,
) bool {
    var col_idx: u16 = 0;
    while (col_idx < projection.nested_scalar_count) : (col_idx += 1) {
        const pos = projection.nested_scalar_positions[col_idx];
        if (row_values[pos] != .null_value) return false;
    }
    return true;
}

fn protocolTypeName(column_type: ColumnType) []const u8 {
    return switch (column_type) {
        .i8 => "i8",
        .i16 => "i16",
        .u8 => "u8",
        .u16 => "u16",
        .u32 => "u32",
        .u64 => "u64",
        .i64 => "i64",
        .i32 => "i32",
        .f64 => "f64",
        .bool => "bool",
        .string => "str",
        .timestamp => "ts",
    };
}

fn writeProtocolValue(
    writer: anytype,
    value: Value,
) error{ResponseTooLarge}!void {
    switch (value) {
        .string => |v| try writeQuotedString(writer, v),
        else => try serializeValue(writer, value),
    }
}

fn writeQuotedString(
    writer: anytype,
    value: []const u8,
) error{ResponseTooLarge}!void {
    writer.writeAll("\"") catch return error.ResponseTooLarge;
    for (value) |byte| {
        switch (byte) {
            '\\' => writer.writeAll("\\\\") catch return error.ResponseTooLarge,
            '"' => writer.writeAll("\\\"") catch return error.ResponseTooLarge,
            '\n' => writer.writeAll("\\n") catch return error.ResponseTooLarge,
            '\r' => writer.writeAll("\\r") catch return error.ResponseTooLarge,
            '\t' => writer.writeAll("\\t") catch return error.ResponseTooLarge,
            else => writer.writeByte(byte) catch return error.ResponseTooLarge,
        }
    }
    writer.writeAll("\"") catch return error.ResponseTooLarge;
}
