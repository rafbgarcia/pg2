//! Value construction and type coercion for mutation assignments.
//!
//! Evaluates assignment expressions, resolves parameter bindings,
//! applies column defaults for INSERT, coerces values to target
//! column types, and produces diagnostics for type/range errors.
const std = @import("std");
const ast_mod = @import("../parser/ast.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const row_mod = @import("../storage/row.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const filter_mod = @import("filter.zig");
const scan_mod = @import("scan.zig");

const mutation = @import("mutation.zig");
const MutationError = mutation.MutationError;
const MutationDiagnostic = mutation.MutationDiagnostic;
const MutationDiagnosticCode = mutation.MutationDiagnosticCode;
const mapFilterError = mutation.mapFilterError;

const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const null_node = ast_mod.null_node;
const TokenizeResult = tokenizer_mod.TokenizeResult;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;
const ColumnType = row_mod.ColumnType;
const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const ParameterBinding = filter_mod.ParameterBinding;
const ParameterResolver = filter_mod.ParameterResolver;

pub const ParameterBindingContext = struct {
    bindings: []const ParameterBinding,
};

pub fn resolveParameterBinding(
    raw_ctx: *const anyopaque,
    tokens: *const TokenizeResult,
    source: []const u8,
    token_index: u16,
) filter_mod.EvalError!Value {
    const ctx: *const ParameterBindingContext = @ptrCast(@alignCast(raw_ctx));
    const parameter_name = tokens.getText(token_index, source);
    for (ctx.bindings) |binding| {
        if (std.mem.eql(u8, binding.name, parameter_name)) {
            return binding.value;
        }
    }
    return error.UndefinedParameter;
}

pub fn buildRowFromAssignments(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    schema: *const RowSchema,
    first_assignment: NodeIndex,
    parameter_bindings: []const ParameterBinding,
    out_values: []Value,
    out_assigned: []bool,
    diagnostic: ?*MutationDiagnostic,
    string_arena: *scan_mod.StringArena,
    eval_ctx: *const filter_mod.EvalContext,
) MutationError!void {
    std.debug.assert(out_values.len >= schema.column_count);
    std.debug.assert(out_assigned.len >= schema.column_count);

    var current = first_assignment;
    // Build a local EvalContext that uses the mutation's own parameter
    // bindings but inherits the statement timestamp from the caller.
    const parameter_ctx = ParameterBindingContext{
        .bindings = parameter_bindings,
    };
    const parameter_resolver = ParameterResolver{
        .ctx = &parameter_ctx,
        .resolve = resolveParameterBinding,
    };
    var local_eval_ctx = filter_mod.EvalContext{
        .statement_timestamp_micros = eval_ctx.statement_timestamp_micros,
        .parameter_resolver = &parameter_resolver,
        .string_arena = string_arena,
    };
    while (current != null_node) {
        const node = tree.getNode(current);
        std.debug.assert(node.tag == .assignment);

        const field_name = tokens.getText(node.extra, source);
        const col_idx = schema.findColumn(field_name) orelse {
            setDiagnostic(
                diagnostic,
                .ColumnNotFound,
                node.extra,
                node.extra,
                "field does not exist on model; check the schema field name or update the assignment",
            );
            return error.ColumnNotFound;
        };

        const expr_node = node.data.unary;
        const val = filter_mod.evaluateExpressionFull(
            tree,
            tokens,
            source,
            expr_node,
            &.{},
            schema,
            null,
            &local_eval_ctx,
        ) catch |e| {
            const mapped = mapFilterError(e);
            if (mapped == error.NumericOverflow) {
                setIntegerRangeDiagnosticForColumn(
                    diagnostic,
                    schema.columns[col_idx].column_type,
                    node.extra,
                    expressionLocationToken(tree, expr_node),
                );
            } else if (mapped == error.NullArithmeticOperand) {
                setNullArithmeticOperandDiagnostic(
                    diagnostic,
                    node.extra,
                    expressionLocationToken(tree, expr_node),
                );
            }
            return mapped;
        };

        const expected_type = schema.columns[col_idx].column_type;
        out_values[col_idx] = coerceValueForColumn(
            val,
            expected_type,
            diagnostic,
            node.extra,
            expressionLocationToken(tree, expr_node),
        ) catch |e| return e;
        out_assigned[col_idx] = true;
        current = node.next;
    }
}

pub fn applyColumnDefaultsForInsert(
    catalog: *const Catalog,
    model_id: ModelId,
    values: []Value,
    assigned_columns: []const bool,
) void {
    std.debug.assert(model_id < catalog.model_count);
    const model = &catalog.models[model_id];
    std.debug.assert(values.len >= model.column_count);
    std.debug.assert(assigned_columns.len >= model.column_count);

    var col_id: u16 = 0;
    while (col_id < model.column_count) : (col_id += 1) {
        if (assigned_columns[col_id]) continue;
        const default_value = catalog.getColumnDefault(model_id, col_id) orelse continue;
        values[col_id] = default_value;
    }
}

pub fn applyAssignments(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    schema: *const RowSchema,
    first_assignment: NodeIndex,
    parameter_bindings: []const ParameterBinding,
    values: []Value,
    diagnostic: ?*MutationDiagnostic,
    string_arena: *scan_mod.StringArena,
    eval_ctx: *const filter_mod.EvalContext,
) MutationError!void {
    std.debug.assert(values.len >= schema.column_count);

    var current = first_assignment;
    const parameter_ctx = ParameterBindingContext{
        .bindings = parameter_bindings,
    };
    const parameter_resolver = ParameterResolver{
        .ctx = &parameter_ctx,
        .resolve = resolveParameterBinding,
    };
    var local_eval_ctx = filter_mod.EvalContext{
        .statement_timestamp_micros = eval_ctx.statement_timestamp_micros,
        .parameter_resolver = &parameter_resolver,
        .string_arena = string_arena,
    };
    while (current != null_node) {
        const node = tree.getNode(current);
        std.debug.assert(node.tag == .assignment);

        const field_name = tokens.getText(node.extra, source);
        const col_idx = schema.findColumn(field_name) orelse {
            setDiagnostic(
                diagnostic,
                .ColumnNotFound,
                node.extra,
                node.extra,
                "field does not exist on model; check the schema field name or update the assignment",
            );
            return error.ColumnNotFound;
        };

        // Evaluate expression with current row values for context.
        const expr_node = node.data.unary;
        const val = filter_mod.evaluateExpressionFull(
            tree,
            tokens,
            source,
            expr_node,
            values,
            schema,
            null,
            &local_eval_ctx,
        ) catch |e| {
            const mapped = mapFilterError(e);
            if (mapped == error.NumericOverflow) {
                setIntegerRangeDiagnosticForColumn(
                    diagnostic,
                    schema.columns[col_idx].column_type,
                    node.extra,
                    expressionLocationToken(tree, expr_node),
                );
            } else if (mapped == error.NullArithmeticOperand) {
                setNullArithmeticOperandDiagnostic(
                    diagnostic,
                    node.extra,
                    expressionLocationToken(tree, expr_node),
                );
            }
            return mapped;
        };

        const expected_type = schema.columns[col_idx].column_type;
        values[col_idx] = coerceValueForColumn(
            val,
            expected_type,
            diagnostic,
            node.extra,
            expressionLocationToken(tree, expr_node),
        ) catch |e| return e;
        current = node.next;
    }
}

pub fn coerceValueForColumn(
    value: Value,
    target: ColumnType,
    diagnostic: ?*MutationDiagnostic,
    field_token: u16,
    location_token: ?u16,
) MutationError!Value {
    if (value == .null_value) return value;
    if (value.columnType()) |actual| {
        if (actual == target) return value;
    }

    return switch (target) {
        .i8 => Value{ .i8 = toI8(value) catch |e| {
            annotateCoercionError(diagnostic, e, value, target, field_token, location_token);
            return e;
        } },
        .i16 => Value{ .i16 = toI16(value) catch |e| {
            annotateCoercionError(diagnostic, e, value, target, field_token, location_token);
            return e;
        } },
        .i32 => Value{ .i32 = toI32(value) catch |e| {
            annotateCoercionError(diagnostic, e, value, target, field_token, location_token);
            return e;
        } },
        .i64 => Value{ .i64 = toI64(value) catch |e| {
            annotateCoercionError(diagnostic, e, value, target, field_token, location_token);
            return e;
        } },
        .u8 => Value{ .u8 = toU8(value) catch |e| {
            annotateCoercionError(diagnostic, e, value, target, field_token, location_token);
            return e;
        } },
        .u16 => Value{ .u16 = toU16(value) catch |e| {
            annotateCoercionError(diagnostic, e, value, target, field_token, location_token);
            return e;
        } },
        .u32 => Value{ .u32 = toU32(value) catch |e| {
            annotateCoercionError(diagnostic, e, value, target, field_token, location_token);
            return e;
        } },
        .u64 => Value{ .u64 = toU64(value) catch |e| {
            annotateCoercionError(diagnostic, e, value, target, field_token, location_token);
            return e;
        } },
        .f64 => Value{ .f64 = toF64(value) catch |e| {
            annotateCoercionError(diagnostic, e, value, target, field_token, location_token);
            return e;
        } },
        .bool => if (value == .bool) value else blk: {
            annotateCoercionError(diagnostic, error.TypeMismatch, value, target, field_token, location_token);
            break :blk error.TypeMismatch;
        },
        .string => if (value == .string) value else blk: {
            annotateCoercionError(diagnostic, error.TypeMismatch, value, target, field_token, location_token);
            break :blk error.TypeMismatch;
        },
        .timestamp => Value{ .timestamp = toI64(value) catch |e| {
            annotateCoercionError(diagnostic, e, value, target, field_token, location_token);
            return e;
        } },
    };
}

fn annotateCoercionError(
    diagnostic: ?*MutationDiagnostic,
    err: MutationError,
    value: Value,
    target: ColumnType,
    field_token: u16,
    location_token: ?u16,
) void {
    if (isIntegerTarget(target) and isIntegerValue(value)) {
        setIntegerRangeDiagnosticForColumn(
            diagnostic,
            target,
            field_token,
            location_token,
        );
        return;
    }

    if (err == error.TypeMismatch) {
        var message_buf: [160]u8 = std.mem.zeroes([160]u8);
        const message = std.fmt.bufPrint(
            message_buf[0..],
            "value type is incompatible with {s}",
            .{columnTypeName(target)},
        ) catch "value type is incompatible with target column";
        setDiagnostic(
            diagnostic,
            .TypeMismatch,
            field_token,
            location_token,
            message,
        );
    }
}

fn setIntegerRangeDiagnosticForColumn(
    diagnostic: ?*MutationDiagnostic,
    target: ColumnType,
    field_token: u16,
    location_token: ?u16,
) void {
    var message_buf: [160]u8 = std.mem.zeroes([160]u8);
    const message = integerRangeMessage(&message_buf, target);
    setDiagnostic(
        diagnostic,
        .IntegerOutOfRange,
        field_token,
        location_token,
        message,
    );
}

fn setNullArithmeticOperandDiagnostic(
    diagnostic: ?*MutationDiagnostic,
    field_token: u16,
    location_token: ?u16,
) void {
    setDiagnostic(
        diagnostic,
        .NullArithmeticOperand,
        field_token,
        location_token,
        "arithmetic operand cannot be null",
    );
}

fn integerRangeMessage(buf: *[160]u8, target: ColumnType) []const u8 {
    return switch (target) {
        .i8 => std.fmt.bufPrint(buf[0..], "value is out of range (-128 to 127)", .{}) catch "value is out of range for i8",
        .i16 => std.fmt.bufPrint(buf[0..], "value is out of range (-32768 to 32767)", .{}) catch "value is out of range for i16",
        .i32 => std.fmt.bufPrint(buf[0..], "value is out of range (-2147483648 to 2147483647)", .{}) catch "value is out of range for i32",
        .i64 => std.fmt.bufPrint(buf[0..], "value is out of range (-9223372036854775808 to 9223372036854775807)", .{}) catch "value is out of range for i64",
        .u8 => std.fmt.bufPrint(buf[0..], "value is out of range (0 to 255)", .{}) catch "value is out of range for u8",
        .u16 => std.fmt.bufPrint(buf[0..], "value is out of range (0 to 65535)", .{}) catch "value is out of range for u16",
        .u32 => std.fmt.bufPrint(buf[0..], "value is out of range (0 to 4294967295)", .{}) catch "value is out of range for u32",
        .u64 => std.fmt.bufPrint(buf[0..], "value is out of range (0 to 18446744073709551615)", .{}) catch "value is out of range for u64",
        else => std.fmt.bufPrint(buf[0..], "value is out of range for {s}", .{columnTypeName(target)}) catch "value is out of range for target column",
    };
}

pub fn setDiagnostic(
    diagnostic: ?*MutationDiagnostic,
    code: MutationDiagnosticCode,
    field_token: ?u16,
    location_token: ?u16,
    message: []const u8,
) void {
    const diag = diagnostic orelse return;
    diag.has_value = true;
    diag.code = code;
    diag.field_token = field_token;
    diag.location_token = location_token;
    @memset(&diag.message, 0);
    const message_len = @min(message.len, diag.message.len);
    @memcpy(diag.message[0..message_len], message[0..message_len]);
}

fn isIntegerTarget(target: ColumnType) bool {
    return switch (target) {
        .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64, .timestamp => true,
        else => false,
    };
}

fn isIntegerValue(value: Value) bool {
    return switch (value) {
        .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64 => true,
        else => false,
    };
}

fn columnTypeName(target: ColumnType) []const u8 {
    return switch (target) {
        .i8 => "i8",
        .i16 => "i16",
        .i32 => "i32",
        .i64 => "i64",
        .u8 => "u8",
        .u16 => "u16",
        .u32 => "u32",
        .u64 => "u64",
        .f64 => "f64",
        .bool => "bool",
        .string => "string",
        .timestamp => "timestamp",
    };
}

fn expressionLocationToken(tree: *const Ast, expr_node: NodeIndex) ?u16 {
    const node = tree.getNode(expr_node);
    return switch (node.tag) {
        .expr_literal, .expr_column_ref, .expr_parameter => node.data.token,
        .expr_function_call, .expr_aggregate => node.extra,
        .expr_list => expressionLocationToken(tree, node.data.unary),
        .expr_binary, .expr_unary => node.extra,
        else => null,
    };
}

pub fn toI8(value: Value) MutationError!i8 {
    const v = try toI64(value);
    return std.math.cast(i8, v) orelse error.TypeMismatch;
}

pub fn toI16(value: Value) MutationError!i16 {
    const v = try toI64(value);
    return std.math.cast(i16, v) orelse error.TypeMismatch;
}

pub fn toI32(value: Value) MutationError!i32 {
    const v = try toI64(value);
    return std.math.cast(i32, v) orelse error.TypeMismatch;
}

pub fn toI64(value: Value) MutationError!i64 {
    return switch (value) {
        .i8 => |v| v,
        .i16 => |v| v,
        .i32 => |v| v,
        .i64 => |v| v,
        .u8 => |v| v,
        .u16 => |v| v,
        .u32 => |v| v,
        .u64 => |v| std.math.cast(i64, v) orelse return error.TypeMismatch,
        else => error.TypeMismatch,
    };
}

pub fn toU8(value: Value) MutationError!u8 {
    const v = try toU64(value);
    return std.math.cast(u8, v) orelse error.TypeMismatch;
}

pub fn toU16(value: Value) MutationError!u16 {
    const v = try toU64(value);
    return std.math.cast(u16, v) orelse error.TypeMismatch;
}

pub fn toU32(value: Value) MutationError!u32 {
    const v = try toU64(value);
    return std.math.cast(u32, v) orelse error.TypeMismatch;
}

pub fn toU64(value: Value) MutationError!u64 {
    return switch (value) {
        .i8 => |v| std.math.cast(u64, v) orelse return error.TypeMismatch,
        .i16 => |v| std.math.cast(u64, v) orelse return error.TypeMismatch,
        .i32 => |v| std.math.cast(u64, v) orelse return error.TypeMismatch,
        .i64 => |v| std.math.cast(u64, v) orelse return error.TypeMismatch,
        .u8 => |v| v,
        .u16 => |v| v,
        .u32 => |v| v,
        .u64 => |v| v,
        else => error.TypeMismatch,
    };
}

pub fn toF64(value: Value) MutationError!f64 {
    return switch (value) {
        .i8 => |v| @floatFromInt(v),
        .i16 => |v| @floatFromInt(v),
        .i32 => |v| @floatFromInt(v),
        .i64 => |v| @floatFromInt(v),
        .u8 => |v| @floatFromInt(v),
        .u16 => |v| @floatFromInt(v),
        .u32 => |v| @floatFromInt(v),
        .u64 => |v| @floatFromInt(v),
        .f64 => |v| v,
        else => error.TypeMismatch,
    };
}
