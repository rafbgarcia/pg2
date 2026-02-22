//! Expression evaluation for row-level filtering and computed values.
//!
//! Responsibilities in this file:
//! - Evaluates AST expression trees against row values and schema metadata.
//! - Implements deterministic stack-based evaluation and builtin functions.
//! - Handles aggregate-node resolution through an explicit resolver callback.
//! - Returns strict typed errors for invalid predicates and expression misuse.
const std = @import("std");
const ast_mod = @import("../parser/ast.zig");
const expression_mod = @import("../parser/expression.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const row_mod = @import("../storage/row.zig");

const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const NodeTag = ast_mod.NodeTag;
const null_node = ast_mod.null_node;
const TokenType = tokenizer_mod.TokenType;
const TokenizeResult = tokenizer_mod.TokenizeResult;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;

/// Stack capacity for expression evaluation.
const max_eval_stack = 64;
const max_work_stack = 128;
const max_string_result_bytes = 1024;

pub const EvalError = error{
    StackOverflow,
    StackUnderflow,
    TypeMismatch,
    DivisionByZero,
    NumericOverflow,
    NullArithmeticOperand,
    ColumnNotFound,
    InvalidLiteral,
    UnknownFunction,
    NullInPredicate,
    UndefinedParameter,
};

pub const AggregateResolver = struct {
    ctx: *const anyopaque,
    resolve: *const fn (
        ctx: *const anyopaque,
        node_index: NodeIndex,
        row_values: []const Value,
        schema: *const RowSchema,
    ) EvalError!Value,
};

pub const ParameterBinding = struct {
    name: []const u8,
    value: Value,
};

pub const ParameterResolver = struct {
    ctx: *const anyopaque,
    resolve: *const fn (
        ctx: *const anyopaque,
        tokens: *const TokenizeResult,
        source: []const u8,
        token_index: u16,
    ) EvalError!Value,
};

/// Work items for iterative post-order traversal.
const WorkItem = union(enum) {
    evaluate: NodeIndex,
    apply_binary: u16,
    apply_unary: u16,
    apply_column_ref: u16,
    apply_parameter: u16,
    apply_literal: u16,
    apply_aggregate: NodeIndex,
    apply_function: struct { token_index: u16, arg_count: u16 },
    apply_membership: struct { token_index: u16, list_count: u16 },
};

/// Evaluate an AST expression node to a Value using iterative traversal.
pub fn evaluateExpression(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    node_index: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
) EvalError!Value {
    return evaluateExpressionWithResolvers(
        tree,
        tokens,
        source,
        node_index,
        row_values,
        schema,
        null,
        null,
    );
}

pub fn evaluateExpressionWithResolver(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    node_index: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
    resolver: ?*const AggregateResolver,
) EvalError!Value {
    return evaluateExpressionWithResolvers(
        tree,
        tokens,
        source,
        node_index,
        row_values,
        schema,
        resolver,
        null,
    );
}

pub fn evaluateExpressionWithResolvers(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    node_index: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
    aggregate_resolver: ?*const AggregateResolver,
    parameter_resolver: ?*const ParameterResolver,
) EvalError!Value {
    var eval_stack: [max_eval_stack]Value = undefined;
    var eval_count: u16 = 0;
    var work_stack: [max_work_stack]WorkItem = undefined;
    var work_count: u16 = 0;

    // Seed with root node.
    work_stack[0] = .{ .evaluate = node_index };
    work_count = 1;

    while (work_count > 0) {
        work_count -= 1;
        const item = work_stack[work_count];
        switch (item) {
            .evaluate => |idx| try pushNodeWork(
                tree,
                tokens,
                source,
                idx,
                &work_stack,
                &work_count,
            ),
            .apply_literal => |tok_idx| {
                const val = try parseLiteralValue(tokens, source, tok_idx);
                try evalPush(&eval_stack, &eval_count, val);
            },
            .apply_column_ref => |tok_idx| {
                const name = tokens.getText(tok_idx, source);
                const col = schema.findColumn(name) orelse
                    return error.ColumnNotFound;
                try evalPush(&eval_stack, &eval_count, row_values[col]);
            },
            .apply_parameter => |tok_idx| {
                const r = parameter_resolver orelse return error.UndefinedParameter;
                const val = try r.resolve(
                    r.ctx,
                    tokens,
                    source,
                    tok_idx,
                );
                try evalPush(&eval_stack, &eval_count, val);
            },
            .apply_binary => |tok_idx| {
                const op_type = tokens.tokens[tok_idx].token_type;
                if (eval_count < 2) return error.StackUnderflow;
                const rhs = evalPop(&eval_stack, &eval_count);
                const lhs = evalPop(&eval_stack, &eval_count);
                const result = try applyBinaryOp(lhs, rhs, op_type);
                try evalPush(&eval_stack, &eval_count, result);
            },
            .apply_unary => |tok_idx| {
                const op_type = tokens.tokens[tok_idx].token_type;
                if (eval_count < 1) return error.StackUnderflow;
                const operand = evalPop(&eval_stack, &eval_count);
                const result = try applyUnaryOp(operand, op_type);
                try evalPush(&eval_stack, &eval_count, result);
            },
            .apply_function => |info| {
                const fn_type = tokens.tokens[info.token_index].token_type;
                if (eval_count < info.arg_count) return error.StackUnderflow;
                var args: [8]Value = undefined;
                const count = @min(info.arg_count, 8);
                var i: u16 = count;
                while (i > 0) {
                    i -= 1;
                    args[i] = evalPop(&eval_stack, &eval_count);
                }
                const result = try applyBuiltinFunction(
                    fn_type,
                    args[0..count],
                );
                try evalPush(&eval_stack, &eval_count, result);
            },
            .apply_aggregate => |agg_node_idx| {
                const r = aggregate_resolver orelse return error.UnknownFunction;
                const val = try r.resolve(
                    r.ctx,
                    agg_node_idx,
                    row_values,
                    schema,
                );
                try evalPush(&eval_stack, &eval_count, val);
            },
            .apply_membership => |info| {
                if (eval_count < info.list_count + 1) return error.StackUnderflow;
                var list_values: [64]Value = undefined;
                var i: u16 = info.list_count;
                while (i > 0) {
                    i -= 1;
                    list_values[i] = evalPop(&eval_stack, &eval_count);
                }
                const needle = evalPop(&eval_stack, &eval_count);
                const result = try applyMembershipFunction(
                    tokens,
                    source,
                    info.token_index,
                    needle,
                    list_values[0..info.list_count],
                );
                try evalPush(&eval_stack, &eval_count, result);
            },
        }
    }

    if (eval_count == 0) return error.StackUnderflow;
    return eval_stack[eval_count - 1];
}

/// Evaluate an expression and assert the result is bool.
pub fn evaluatePredicate(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    node_index: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
) EvalError!bool {
    return evaluatePredicateWithResolvers(
        tree,
        tokens,
        source,
        node_index,
        row_values,
        schema,
        null,
        null,
    );
}

pub fn evaluatePredicateWithResolver(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    node_index: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
    resolver: ?*const AggregateResolver,
) EvalError!bool {
    return evaluatePredicateWithResolvers(
        tree,
        tokens,
        source,
        node_index,
        row_values,
        schema,
        resolver,
        null,
    );
}

pub fn evaluatePredicateWithResolvers(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    node_index: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
    aggregate_resolver: ?*const AggregateResolver,
    parameter_resolver: ?*const ParameterResolver,
) EvalError!bool {
    const val = try evaluateExpressionWithResolvers(
        tree,
        tokens,
        source,
        node_index,
        row_values,
        schema,
        aggregate_resolver,
        parameter_resolver,
    );
    if (val == .null_value) return error.NullInPredicate;
    if (val != .bool) return error.TypeMismatch;
    return val.bool;
}

/// Push work items for an AST node (post-order: push apply first, then children).
fn pushNodeWork(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    idx: NodeIndex,
    work_stack: *[max_work_stack]WorkItem,
    work_count: *u16,
) EvalError!void {
    if (idx == null_node) return error.StackUnderflow;
    const node = tree.getNode(idx);

    switch (node.tag) {
        .expr_literal => {
            try workPush(work_stack, work_count, .{
                .apply_literal = node.data.token,
            });
        },
        .expr_column_ref => {
            try workPush(work_stack, work_count, .{
                .apply_column_ref = node.data.token,
            });
        },
        .expr_parameter => {
            try workPush(work_stack, work_count, .{
                .apply_parameter = node.data.token,
            });
        },
        .expr_binary => {
            // Post-order: push apply first (executed last), then children.
            const op_tok = node.extra;
            try workPush(work_stack, work_count, .{ .apply_binary = op_tok });
            try workPush(work_stack, work_count, .{
                .evaluate = node.data.binary.rhs,
            });
            try workPush(work_stack, work_count, .{
                .evaluate = node.data.binary.lhs,
            });
        },
        .expr_unary => {
            const op_tok = node.extra;
            try workPush(work_stack, work_count, .{ .apply_unary = op_tok });
            try workPush(work_stack, work_count, .{
                .evaluate = node.data.unary,
            });
        },
        .expr_function_call => {
            const fn_tok = node.extra;
            if (isMembershipFunction(tokens, source, fn_tok)) {
                const arg_count = tree.listLen(node.data.unary);
                if (arg_count != 2) return error.TypeMismatch;

                const value_arg = node.data.unary;
                const list_arg = tree.getNode(value_arg).next;
                if (list_arg == null_node) return error.TypeMismatch;
                if (tree.getNode(list_arg).tag != .expr_list) return error.TypeMismatch;

                const list_head = tree.getNode(list_arg).data.unary;
                const list_count = tree.listLen(list_head);

                try workPush(work_stack, work_count, .{
                    .apply_membership = .{
                        .token_index = fn_tok,
                        .list_count = list_count,
                    },
                });
                try pushLinkedListReverse(
                    tree,
                    work_stack,
                    work_count,
                    list_head,
                    list_count,
                );
                try workPush(work_stack, work_count, .{
                    .evaluate = value_arg,
                });
            } else {
                const arg_count = tree.listLen(node.data.unary);
                try workPush(work_stack, work_count, .{
                    .apply_function = .{
                        .token_index = fn_tok,
                        .arg_count = arg_count,
                    },
                });
                // Push args in reverse order (last arg pushed first onto work stack).
                try pushLinkedListReverse(
                    tree,
                    work_stack,
                    work_count,
                    node.data.unary,
                    arg_count,
                );
            }
        },
        .expr_aggregate => {
            try workPush(work_stack, work_count, .{
                .apply_aggregate = idx,
            });
        },
        else => {
            // Fallback: treat as literal token reference.
            const tok_idx = node.data.token;
            if (tok_idx < tokens.count) {
                try workPush(work_stack, work_count, .{
                    .apply_literal = tok_idx,
                });
            } else {
                return error.InvalidLiteral;
            }
        },
    }
}

fn isMembershipFunction(
    tokens: *const TokenizeResult,
    source: []const u8,
    token_index: u16,
) bool {
    if (token_index >= tokens.count) return false;
    if (tokens.tokens[token_index].token_type != .identifier) return false;
    return std.mem.eql(
        u8,
        tokens.getText(token_index, source),
        "in",
    );
}

/// Push linked list nodes in reverse order onto work stack.
fn pushLinkedListReverse(
    tree: *const Ast,
    work_stack: *[max_work_stack]WorkItem,
    work_count: *u16,
    head: NodeIndex,
    count: u16,
) EvalError!void {
    // Collect indices into a temp buffer, then push in reverse.
    var indices: [64]NodeIndex = undefined;
    var current = head;
    var i: u16 = 0;
    while (current != null_node and i < 64) {
        indices[i] = current;
        current = tree.getNode(current).next;
        i += 1;
    }
    // Push in reverse so first arg is evaluated first.
    var j: u16 = i;
    while (j > 0) {
        j -= 1;
        try workPush(work_stack, work_count, .{ .evaluate = indices[j] });
    }
    _ = count;
}

fn evalPush(stack: *[max_eval_stack]Value, count: *u16, val: Value) EvalError!void {
    if (count.* >= max_eval_stack) return error.StackOverflow;
    stack[count.*] = val;
    count.* += 1;
}

fn evalPop(stack: *[max_eval_stack]Value, count: *u16) Value {
    std.debug.assert(count.* > 0);
    count.* -= 1;
    return stack[count.*];
}

fn workPush(
    stack: *[max_work_stack]WorkItem,
    count: *u16,
    item: WorkItem,
) EvalError!void {
    if (count.* >= max_work_stack) return error.StackOverflow;
    stack[count.*] = item;
    count.* += 1;
}

/// Parse a literal token into a Value.
pub fn parseLiteralValue(
    tokens: *const TokenizeResult,
    source: []const u8,
    token_index: u16,
) EvalError!Value {
    const tok = tokens.tokens[token_index];
    const text = source[tok.start..][0..tok.len];

    return switch (tok.token_type) {
        .integer_literal => blk: {
            if (std.fmt.parseInt(i64, text, 10)) |v| {
                break :blk Value{ .i64 = v };
            } else |_| {
                const uv = std.fmt.parseInt(u64, text, 10) catch
                    return error.InvalidLiteral;
                break :blk Value{ .u64 = uv };
            }
        },
        .float_literal => blk: {
            const v = std.fmt.parseFloat(f64, text) catch
                return error.InvalidLiteral;
            break :blk Value{ .f64 = v };
        },
        .string_literal => blk: {
            // Strip surrounding quotes.
            if (text.len >= 2 and text[0] == '"' and text[text.len - 1] == '"') {
                break :blk Value{ .string = text[1 .. text.len - 1] };
            }
            break :blk Value{ .string = text };
        },
        .true_literal => Value{ .bool = true },
        .false_literal => Value{ .bool = false },
        .null_literal => Value{ .null_value = {} },
        else => error.InvalidLiteral,
    };
}

/// Apply a binary operator to two values.
pub fn applyBinaryOp(lhs: Value, rhs: Value, op: TokenType) EvalError!Value {
    if (lhs == .null_value or rhs == .null_value) {
        return switch (op) {
            .plus, .minus, .star, .slash => error.NullArithmeticOperand,
            .equal_equal => applyComparison(lhs, rhs, .eq),
            .not_equal => applyComparison(lhs, rhs, .neq),
            .less_than => applyComparison(lhs, rhs, .lt),
            .less_equal => applyComparison(lhs, rhs, .lte),
            .greater_than => applyComparison(lhs, rhs, .gt),
            .greater_equal => applyComparison(lhs, rhs, .gte),
            .and_and => applyLogical(lhs, rhs, .@"and"),
            .or_or => applyLogical(lhs, rhs, .@"or"),
            else => error.TypeMismatch,
        };
    }

    return switch (op) {
        .plus => applyArithmetic(lhs, rhs, .add),
        .minus => applyArithmetic(lhs, rhs, .sub),
        .star => applyArithmetic(lhs, rhs, .mul),
        .slash => applyArithmetic(lhs, rhs, .div),
        .equal_equal => applyComparison(lhs, rhs, .eq),
        .not_equal => applyComparison(lhs, rhs, .neq),
        .less_than => applyComparison(lhs, rhs, .lt),
        .less_equal => applyComparison(lhs, rhs, .lte),
        .greater_than => applyComparison(lhs, rhs, .gt),
        .greater_equal => applyComparison(lhs, rhs, .gte),
        .and_and => applyLogical(lhs, rhs, .@"and"),
        .or_or => applyLogical(lhs, rhs, .@"or"),
        else => error.TypeMismatch,
    };
}

const ArithOp = enum { add, sub, mul, div };

fn applyArithmetic(lhs: Value, rhs: Value, op: ArithOp) EvalError!Value {
    // Integer arithmetic.
    if (lhs == .i8 and rhs == .i8) {
        const a = lhs.i8;
        const b = rhs.i8;
        return Value{ .i8 = try applySignedArithmetic(i8, a, b, op) };
    }
    if (lhs == .i16 and rhs == .i16) {
        const a = lhs.i16;
        const b = rhs.i16;
        return Value{ .i16 = try applySignedArithmetic(i16, a, b, op) };
    }
    if (lhs == .i32 and rhs == .i32) {
        const a = lhs.i32;
        const b = rhs.i32;
        return Value{ .i32 = try applySignedArithmetic(i32, a, b, op) };
    }
    if (lhs == .i64 and rhs == .i64) {
        const a = lhs.i64;
        const b = rhs.i64;
        return Value{ .i64 = try applySignedArithmetic(i64, a, b, op) };
    }
    if (lhs == .u8 and rhs == .u8) {
        const a = lhs.u8;
        const b = rhs.u8;
        return Value{ .u8 = try applyUnsignedArithmetic(u8, a, b, op) };
    }
    if (lhs == .u16 and rhs == .u16) {
        const a = lhs.u16;
        const b = rhs.u16;
        return Value{ .u16 = try applyUnsignedArithmetic(u16, a, b, op) };
    }
    if (lhs == .u32 and rhs == .u32) {
        const a = lhs.u32;
        const b = rhs.u32;
        return Value{ .u32 = try applyUnsignedArithmetic(u32, a, b, op) };
    }
    if (lhs == .u64 and rhs == .u64) {
        const a = lhs.u64;
        const b = rhs.u64;
        return Value{ .u64 = try applyUnsignedArithmetic(u64, a, b, op) };
    }
    if (isIntegerValue(lhs) and isIntegerValue(rhs)) {
        if (isSignedIntegerValue(lhs) or isSignedIntegerValue(rhs)) {
            const a = try toI64Integer(lhs);
            const b = try toI64Integer(rhs);
            return Value{ .i64 = try applySignedArithmetic(i64, a, b, op) };
        }
        const a = try toU64Integer(lhs);
        const b = try toU64Integer(rhs);
        return Value{ .u64 = try applyUnsignedArithmetic(u64, a, b, op) };
    }

    // Float arithmetic (promote if mixed).
    const a = toFloat(lhs) orelse return error.TypeMismatch;
    const b = toFloat(rhs) orelse return error.TypeMismatch;
    return Value{ .f64 = switch (op) {
        .add => a + b,
        .sub => a - b,
        .mul => a * b,
        .div => blk: {
            if (b == 0.0) return error.DivisionByZero;
            break :blk a / b;
        },
    } };
}

fn applySignedArithmetic(comptime T: type, a: T, b: T, op: ArithOp) EvalError!T {
    return switch (op) {
        .add => std.math.add(T, a, b) catch return error.NumericOverflow,
        .sub => std.math.sub(T, a, b) catch return error.NumericOverflow,
        .mul => std.math.mul(T, a, b) catch return error.NumericOverflow,
        .div => blk: {
            if (b == 0) return error.DivisionByZero;
            if (a == std.math.minInt(T) and b == -1) return error.NumericOverflow;
            break :blk @divTrunc(a, b);
        },
    };
}

fn applyUnsignedArithmetic(comptime T: type, a: T, b: T, op: ArithOp) EvalError!T {
    return switch (op) {
        .add => std.math.add(T, a, b) catch return error.NumericOverflow,
        .sub => std.math.sub(T, a, b) catch return error.NumericOverflow,
        .mul => std.math.mul(T, a, b) catch return error.NumericOverflow,
        .div => blk: {
            if (b == 0) return error.DivisionByZero;
            break :blk @divTrunc(a, b);
        },
    };
}

fn isIntegerValue(v: Value) bool {
    return switch (v) {
        .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64 => true,
        else => false,
    };
}

fn isSignedIntegerValue(v: Value) bool {
    return switch (v) {
        .i8, .i16, .i32, .i64 => true,
        else => false,
    };
}

fn toI64Integer(v: Value) EvalError!i64 {
    return switch (v) {
        .i8 => |x| x,
        .i16 => |x| x,
        .i32 => |x| x,
        .i64 => |x| x,
        .u8 => |x| x,
        .u16 => |x| x,
        .u32 => |x| x,
        .u64 => |x| std.math.cast(i64, x) orelse return error.NumericOverflow,
        else => error.TypeMismatch,
    };
}

fn toU64Integer(v: Value) EvalError!u64 {
    return switch (v) {
        .i8 => |x| std.math.cast(u64, x) orelse return error.NumericOverflow,
        .i16 => |x| std.math.cast(u64, x) orelse return error.NumericOverflow,
        .i32 => |x| std.math.cast(u64, x) orelse return error.NumericOverflow,
        .i64 => |x| std.math.cast(u64, x) orelse return error.NumericOverflow,
        .u8 => |x| x,
        .u16 => |x| x,
        .u32 => |x| x,
        .u64 => |x| x,
        else => error.TypeMismatch,
    };
}

fn toFloat(v: Value) ?f64 {
    return switch (v) {
        .f64 => |f| f,
        .i8 => |i| @as(f64, @floatFromInt(i)),
        .i16 => |i| @as(f64, @floatFromInt(i)),
        .i64 => |i| @as(f64, @floatFromInt(i)),
        .i32 => |i| @as(f64, @floatFromInt(i)),
        .u8 => |i| @as(f64, @floatFromInt(i)),
        .u16 => |i| @as(f64, @floatFromInt(i)),
        .u32 => |i| @as(f64, @floatFromInt(i)),
        .u64 => |i| @as(f64, @floatFromInt(i)),
        else => null,
    };
}

fn toSigned(v: Value) ?i128 {
    return switch (v) {
        .i8 => |x| x,
        .i16 => |x| x,
        .i32 => |x| x,
        .i64 => |x| x,
        else => null,
    };
}

fn toUnsigned(v: Value) ?u128 {
    return switch (v) {
        .u8 => |x| x,
        .u16 => |x| x,
        .u32 => |x| x,
        .u64 => |x| x,
        else => null,
    };
}

fn numericOrder(lhs: Value, rhs: Value) ?std.math.Order {
    if (lhs == .f64 or rhs == .f64) {
        const lf = toFloat(lhs) orelse return null;
        const rf = toFloat(rhs) orelse return null;
        return std.math.order(lf, rf);
    }

    if (toSigned(lhs)) |ls| {
        if (toSigned(rhs)) |rs| return std.math.order(ls, rs);
        if (toUnsigned(rhs)) |ru| {
            if (ls < 0) return .lt;
            return std.math.order(@as(u128, @intCast(ls)), ru);
        }
        return null;
    }
    if (toUnsigned(lhs)) |lu| {
        if (toUnsigned(rhs)) |ru| return std.math.order(lu, ru);
        if (toSigned(rhs)) |rs| {
            if (rs < 0) return .gt;
            return std.math.order(lu, @as(u128, @intCast(rs)));
        }
        return null;
    }
    return null;
}

const CmpOp = enum { eq, neq, lt, lte, gt, gte };

fn applyComparison(lhs: Value, rhs: Value, op: CmpOp) EvalError!Value {
    if (lhs == .null_value or rhs == .null_value) {
        return switch (op) {
            .eq => Value{ .bool = lhs == .null_value and rhs == .null_value },
            .neq => Value{
                .bool = (lhs == .null_value) != (rhs == .null_value),
            },
            else => Value{ .null_value = {} },
        };
    }

    const ord = if (isNumericValue(lhs) and isNumericValue(rhs))
        (numericOrder(lhs, rhs) orelse return error.TypeMismatch)
    else blk: {
        const lhs_type = lhs.columnType() orelse return error.TypeMismatch;
        const rhs_type = rhs.columnType() orelse return error.TypeMismatch;
        if (lhs_type != rhs_type) return error.TypeMismatch;
        break :blk row_mod.compareValues(lhs, rhs);
    };
    return Value{ .bool = switch (op) {
        .eq => ord == .eq,
        .neq => ord != .eq,
        .lt => ord == .lt,
        .lte => ord == .lt or ord == .eq,
        .gt => ord == .gt,
        .gte => ord == .gt or ord == .eq,
    } };
}

const LogicalOp = enum { @"and", @"or" };

fn applyLogical(lhs: Value, rhs: Value, op: LogicalOp) EvalError!Value {
    // SQL three-valued logic: null and false = false, null or true = true.
    const a = toBool(lhs);
    const b = toBool(rhs);

    return switch (op) {
        .@"and" => blk: {
            if (a != null and !a.?) break :blk Value{ .bool = false };
            if (b != null and !b.?) break :blk Value{ .bool = false };
            if (a != null and b != null) break :blk Value{
                .bool = a.? and b.?,
            };
            break :blk Value{ .null_value = {} };
        },
        .@"or" => blk: {
            if (a != null and a.?) break :blk Value{ .bool = true };
            if (b != null and b.?) break :blk Value{ .bool = true };
            if (a != null and b != null) break :blk Value{
                .bool = a.? or b.?,
            };
            break :blk Value{ .null_value = {} };
        },
    };
}

fn toBool(v: Value) ?bool {
    return switch (v) {
        .bool => |b| b,
        .null_value => null,
        else => null,
    };
}

fn isNumericValue(v: Value) bool {
    return switch (v) {
        .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64, .f64 => true,
        else => false,
    };
}

fn membershipElementEquals(needle: Value, element: Value) EvalError!bool {
    if (isNumericValue(needle) and isNumericValue(element)) {
        const ord = numericOrder(needle, element) orelse return error.TypeMismatch;
        return ord == .eq;
    }

    if (needle.columnType()) |needle_type| {
        const element_type = element.columnType() orelse return error.TypeMismatch;
        if (needle_type != element_type) return error.TypeMismatch;
        return row_mod.compareValues(needle, element) == .eq;
    }

    return error.TypeMismatch;
}

fn applyMembershipFunction(
    tokens: *const TokenizeResult,
    source: []const u8,
    token_index: u16,
    needle: Value,
    list_values: []const Value,
) EvalError!Value {
    if (!isMembershipFunction(tokens, source, token_index)) {
        return error.UnknownFunction;
    }

    if (needle == .null_value) return Value{ .null_value = {} };
    var saw_null = false;
    for (list_values) |element| {
        if (element == .null_value) {
            saw_null = true;
            continue;
        }
        if (try membershipElementEquals(needle, element)) {
            return Value{ .bool = true };
        }
    }

    if (saw_null) return Value{ .null_value = {} };
    return Value{ .bool = false };
}

/// Apply a unary operator.
pub fn applyUnaryOp(operand: Value, op: TokenType) EvalError!Value {
    if (operand == .null_value) return Value{ .null_value = {} };

    return switch (op) {
        .bang => blk: {
            if (operand != .bool) return error.TypeMismatch;
            break :blk Value{ .bool = !operand.bool };
        },
        .minus => blk: {
            if (operand == .i8) {
                if (operand.i8 == std.math.minInt(i8)) return error.NumericOverflow;
                break :blk Value{ .i8 = -operand.i8 };
            }
            if (operand == .i16) {
                if (operand.i16 == std.math.minInt(i16)) return error.NumericOverflow;
                break :blk Value{ .i16 = -operand.i16 };
            }
            if (operand == .i64) {
                if (operand.i64 == std.math.minInt(i64)) return error.NumericOverflow;
                break :blk Value{ .i64 = -operand.i64 };
            }
            if (operand == .i32) {
                if (operand.i32 == std.math.minInt(i32)) return error.NumericOverflow;
                break :blk Value{ .i32 = -operand.i32 };
            }
            if (operand == .u8) break :blk Value{ .i64 = -@as(i64, operand.u8) };
            if (operand == .u16) break :blk Value{ .i64 = -@as(i64, operand.u16) };
            if (operand == .u32) break :blk Value{ .i64 = -@as(i64, operand.u32) };
            if (operand == .u64) break :blk Value{ .i64 = try negateUnsignedIntoI64(operand.u64) };
            if (operand == .f64) break :blk Value{ .f64 = -operand.f64 };
            return error.TypeMismatch;
        },
        else => error.TypeMismatch,
    };
}

fn negateUnsignedIntoI64(magnitude: u64) EvalError!i64 {
    const signed_min_magnitude: u64 = @as(u64, std.math.maxInt(i64)) + 1;
    if (magnitude > signed_min_magnitude) return error.NumericOverflow;
    if (magnitude == signed_min_magnitude) return std.math.minInt(i64);
    const narrowed = std.math.cast(i64, magnitude) orelse return error.NumericOverflow;
    return -narrowed;
}

/// Apply a built-in scalar function.
pub fn applyBuiltinFunction(
    fn_type: TokenType,
    args: []const Value,
) EvalError!Value {
    return switch (fn_type) {
        .fn_abs => blk: {
            if (args.len < 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            if (v == .i8) {
                if (v.i8 == std.math.minInt(i8)) return error.NumericOverflow;
                break :blk Value{ .i8 = if (v.i8 < 0) -v.i8 else v.i8 };
            }
            if (v == .i16) {
                if (v.i16 == std.math.minInt(i16)) return error.NumericOverflow;
                break :blk Value{ .i16 = if (v.i16 < 0) -v.i16 else v.i16 };
            }
            if (v == .i64) {
                if (v.i64 == std.math.minInt(i64)) return error.NumericOverflow;
                break :blk Value{
                    .i64 = if (v.i64 < 0) -v.i64 else v.i64,
                };
            }
            if (v == .i32) {
                if (v.i32 == std.math.minInt(i32)) return error.NumericOverflow;
                break :blk Value{
                    .i32 = if (v.i32 < 0) -v.i32 else v.i32,
                };
            }
            if (v == .u8 or v == .u16 or v == .u32 or v == .u64) break :blk v;
            if (v == .f64) break :blk Value{ .f64 = @abs(v.f64) };
            return error.TypeMismatch;
        },
        .fn_sqrt => blk: {
            if (args.len < 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            const f = toFloat(v) orelse return error.TypeMismatch;
            break :blk Value{ .f64 = @sqrt(f) };
        },
        .fn_round => blk: {
            if (args.len < 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            const f = toFloat(v) orelse return error.TypeMismatch;
            break :blk Value{ .f64 = @round(f) };
        },
        .fn_length => blk: {
            if (args.len < 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            if (v != .string) return error.TypeMismatch;
            break :blk Value{ .i64 = @intCast(v.string.len) };
        },
        .fn_coalesce => blk: {
            for (args) |arg| {
                if (arg != .null_value) break :blk arg;
            }
            break :blk Value{ .null_value = {} };
        },
        .fn_lower, .fn_upper, .fn_trim => blk: {
            // String functions return the input string unchanged for M4.
            // Full implementation requires mutable string buffers.
            if (args.len < 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            if (v != .string) return error.TypeMismatch;
            break :blk v;
        },
        .fn_now => Value{ .timestamp = 0 },
        else => error.UnknownFunction,
    };
}

// --- Tests ---

const testing = std.testing;

fn makeAstLiteral(tree: *Ast, tok_idx: u16) !NodeIndex {
    return tree.addNode(.expr_literal, .{ .token = tok_idx });
}

const ParameterFixture = struct {
    bindings: []const ParameterBinding,
};

fn resolveParameterFromBindings(
    raw_ctx: *const anyopaque,
    tokens: *const TokenizeResult,
    source: []const u8,
    token_index: u16,
) EvalError!Value {
    const fixture: *const ParameterFixture = @ptrCast(@alignCast(raw_ctx));
    const raw_name = tokens.getText(token_index, source);
    for (fixture.bindings) |binding| {
        if (std.mem.eql(u8, binding.name, raw_name)) {
            return binding.value;
        }
    }
    return error.UndefinedParameter;
}

test "literal integer evaluation" {
    var tree = Ast{};
    const tokens = tokenizer_mod.tokenize("42");
    const node = try makeAstLiteral(&tree, 0);
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        "42",
        node,
        &.{},
        &schema,
    );
    try testing.expectEqual(@as(i64, 42), result.i64);
}

test "literal f64 evaluation" {
    var tree = Ast{};
    const tokens = tokenizer_mod.tokenize("3.14");
    const node = try makeAstLiteral(&tree, 0);
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        "3.14",
        node,
        &.{},
        &schema,
    );
    try testing.expectEqual(@as(f64, 3.14), result.f64);
}

test "literal string evaluation" {
    var tree = Ast{};
    const source = "\"hello\"";
    const tokens = tokenizer_mod.tokenize(source);
    const node = try makeAstLiteral(&tree, 0);
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        source,
        node,
        &.{},
        &schema,
    );
    try testing.expectEqualSlices(u8, "hello", result.string);
}

test "literal bool evaluation" {
    var tree = Ast{};
    const tokens = tokenizer_mod.tokenize("true");
    const node = try makeAstLiteral(&tree, 0);
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        "true",
        node,
        &.{},
        &schema,
    );
    try testing.expect(result.bool);
}

test "literal null evaluation" {
    var tree = Ast{};
    const tokens = tokenizer_mod.tokenize("null");
    const node = try makeAstLiteral(&tree, 0);
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        "null",
        node,
        &.{},
        &schema,
    );
    try testing.expect(result == .null_value);
}

test "binary addition" {
    var tree = Ast{};
    const source = "10 + 20";
    const tokens = tokenizer_mod.tokenize(source);
    // Build AST manually: binary(lhs=literal(0), rhs=literal(2)), op at token 1.
    const lhs = try tree.addNode(.expr_literal, .{ .token = 0 });
    const rhs = try tree.addNode(.expr_literal, .{ .token = 2 });
    const bin = try tree.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = lhs, .rhs = rhs } },
        1,
        null_node,
    );
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        source,
        bin,
        &.{},
        &schema,
    );
    try testing.expectEqual(@as(i64, 30), result.i64);
}

test "binary comparison" {
    var tree = Ast{};
    const source = "5 == 5";
    const tokens = tokenizer_mod.tokenize(source);
    const lhs = try tree.addNode(.expr_literal, .{ .token = 0 });
    const rhs = try tree.addNode(.expr_literal, .{ .token = 2 });
    const bin = try tree.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = lhs, .rhs = rhs } },
        1,
        null_node,
    );
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        source,
        bin,
        &.{},
        &schema,
    );
    try testing.expect(result.bool);
}

test "logical and" {
    var tree = Ast{};
    const source = "true && false";
    const tokens = tokenizer_mod.tokenize(source);
    const lhs = try tree.addNode(.expr_literal, .{ .token = 0 });
    const rhs = try tree.addNode(.expr_literal, .{ .token = 2 });
    const bin = try tree.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = lhs, .rhs = rhs } },
        1,
        null_node,
    );
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        source,
        bin,
        &.{},
        &schema,
    );
    try testing.expect(!result.bool);
}

test "logical or" {
    var tree = Ast{};
    const source = "true || false";
    const tokens = tokenizer_mod.tokenize(source);
    const lhs = try tree.addNode(.expr_literal, .{ .token = 0 });
    const rhs = try tree.addNode(.expr_literal, .{ .token = 2 });
    const bin = try tree.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = lhs, .rhs = rhs } },
        1,
        null_node,
    );
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        source,
        bin,
        &.{},
        &schema,
    );
    try testing.expect(result.bool);
}

test "column reference evaluation" {
    var tree = Ast{};
    const source = "age";
    const tokens = tokenizer_mod.tokenize(source);
    const node = try tree.addNode(.expr_column_ref, .{ .token = 0 });

    var schema = RowSchema{};
    _ = try schema.addColumn("age", .i64, false);
    const row_values = [_]Value{.{ .i64 = 25 }};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        source,
        node,
        &row_values,
        &schema,
    );
    try testing.expectEqual(@as(i64, 25), result.i64);
}

test "parameter reference evaluation uses explicit binding resolver" {
    var tree = Ast{};
    const source = "$user_id";
    const tokens = tokenizer_mod.tokenize(source);
    const node = try tree.addNode(.expr_parameter, .{ .token = 0 });
    const schema = RowSchema{};
    const bindings = [_]ParameterBinding{
        .{ .name = "$user_id", .value = .{ .i64 = 42 } },
    };
    const fixture = ParameterFixture{ .bindings = bindings[0..] };
    const resolver = ParameterResolver{
        .ctx = &fixture,
        .resolve = resolveParameterFromBindings,
    };

    const result = try evaluateExpressionWithResolvers(
        &tree,
        &tokens,
        source,
        node,
        &.{},
        &schema,
        null,
        &resolver,
    );
    try testing.expectEqual(@as(i64, 42), result.i64);
}

test "parameter reference fails closed when binding is undefined" {
    var tree = Ast{};
    const source = "$missing";
    const tokens = tokenizer_mod.tokenize(source);
    const node = try tree.addNode(.expr_parameter, .{ .token = 0 });
    const schema = RowSchema{};

    const result = evaluateExpression(
        &tree,
        &tokens,
        source,
        node,
        &.{},
        &schema,
    );
    try testing.expectError(error.UndefinedParameter, result);
}

test "function call — abs" {
    const result = try applyBuiltinFunction(.fn_abs, &[_]Value{
        .{ .i64 = -5 },
    });
    try testing.expectEqual(@as(i64, 5), result.i64);
}

test "function call — sqrt" {
    const result = try applyBuiltinFunction(.fn_sqrt, &[_]Value{
        .{ .f64 = 9.0 },
    });
    try testing.expectEqual(@as(f64, 3.0), result.f64);
}

test "function call — length" {
    const result = try applyBuiltinFunction(.fn_length, &[_]Value{
        .{ .string = "hello" },
    });
    try testing.expectEqual(@as(i64, 5), result.i64);
}

test "function call — coalesce" {
    const result = try applyBuiltinFunction(.fn_coalesce, &[_]Value{
        .{ .null_value = {} },
        .{ .i64 = 42 },
    });
    try testing.expectEqual(@as(i64, 42), result.i64);
}

test "membership function returns true for contained value" {
    var tree = Ast{};
    const source = "in(status, [\"active\", \"pending\"])";
    const tokens = tokenizer_mod.tokenize(source);
    const expr = try expression_mod.parseExpression(&tree, &tokens, source, 0);
    var schema = RowSchema{};
    _ = try schema.addColumn("status", .string, true);
    const row_values = [_]Value{.{ .string = "active" }};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        source,
        expr.node,
        &row_values,
        &schema,
    );
    try testing.expect(result == .bool);
    try testing.expect(result.bool);
}

test "membership function returns false for non-matching list without nulls" {
    var tree = Ast{};
    const source = "in(status, [\"active\", \"pending\"])";
    const tokens = tokenizer_mod.tokenize(source);
    const expr = try expression_mod.parseExpression(&tree, &tokens, source, 0);
    var schema = RowSchema{};
    _ = try schema.addColumn("status", .string, true);
    const row_values = [_]Value{.{ .string = "archived" }};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        source,
        expr.node,
        &row_values,
        &schema,
    );
    try testing.expect(result == .bool);
    try testing.expect(!result.bool);
}

test "membership function returns null when list has null and no match" {
    var tree = Ast{};
    const source = "in(status, [\"active\", null])";
    const tokens = tokenizer_mod.tokenize(source);
    const expr = try expression_mod.parseExpression(&tree, &tokens, source, 0);
    var schema = RowSchema{};
    _ = try schema.addColumn("status", .string, true);
    const row_values = [_]Value{.{ .string = "archived" }};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        source,
        expr.node,
        &row_values,
        &schema,
    );
    try testing.expect(result == .null_value);
}

test "membership function returns null when value is null" {
    var tree = Ast{};
    const source = "in(status, [\"active\", null])";
    const tokens = tokenizer_mod.tokenize(source);
    const expr = try expression_mod.parseExpression(&tree, &tokens, source, 0);
    var schema = RowSchema{};
    _ = try schema.addColumn("status", .string, true);
    const row_values = [_]Value{.{ .null_value = {} }};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        source,
        expr.node,
        &row_values,
        &schema,
    );
    try testing.expect(result == .null_value);
}

test "membership function fails closed on type mismatch" {
    var tree = Ast{};
    const source = "in(status, [1, 2])";
    const tokens = tokenizer_mod.tokenize(source);
    const expr = try expression_mod.parseExpression(&tree, &tokens, source, 0);
    var schema = RowSchema{};
    _ = try schema.addColumn("status", .string, true);
    const row_values = [_]Value{.{ .string = "1" }};

    const result = evaluateExpression(
        &tree,
        &tokens,
        source,
        expr.node,
        &row_values,
        &schema,
    );
    try testing.expectError(error.TypeMismatch, result);
}

test "negated membership function propagates null when membership is null" {
    var tree = Ast{};
    const source = "!in(status, [\"active\", null])";
    const tokens = tokenizer_mod.tokenize(source);
    const expr = try expression_mod.parseExpression(&tree, &tokens, source, 0);
    var schema = RowSchema{};
    _ = try schema.addColumn("status", .string, true);
    const row_values = [_]Value{.{ .string = "archived" }};

    const result = try evaluateExpression(
        &tree,
        &tokens,
        source,
        expr.node,
        &row_values,
        &schema,
    );
    try testing.expect(result == .null_value);
}

test "arithmetic rejects null operands" {
    const result = applyBinaryOp(
        .{ .i64 = 5 },
        .{ .null_value = {} },
        .plus,
    );
    try testing.expectError(error.NullArithmeticOperand, result);
}

test "division by zero" {
    const result = applyBinaryOp(
        .{ .i64 = 5 },
        .{ .i64 = 0 },
        .slash,
    );
    try testing.expectError(error.DivisionByZero, result);
}

test "i64 arithmetic overflow returns error" {
    const result = applyBinaryOp(
        .{ .i64 = std.math.maxInt(i64) },
        .{ .i64 = 1 },
        .plus,
    );
    try testing.expectError(error.NumericOverflow, result);
}

test "i32 arithmetic overflow returns error" {
    const result = applyBinaryOp(
        .{ .i32 = std.math.maxInt(i32) },
        .{ .i32 = 2 },
        .star,
    );
    try testing.expectError(error.NumericOverflow, result);
}

test "i8 arithmetic stays i8" {
    const result = try applyBinaryOp(
        .{ .i8 = 12 },
        .{ .i8 = 7 },
        .minus,
    );
    try testing.expect(result == .i8);
    try testing.expectEqual(@as(i8, 5), result.i8);
}

test "u16 arithmetic stays u16" {
    const result = try applyBinaryOp(
        .{ .u16 = 25 },
        .{ .u16 = 5 },
        .plus,
    );
    try testing.expect(result == .u16);
    try testing.expectEqual(@as(u16, 30), result.u16);
}

test "mixed integer arithmetic returns i64 not f64" {
    const result = try applyBinaryOp(
        .{ .u16 = 41 },
        .{ .i64 = 1 },
        .plus,
    );
    try testing.expect(result == .i64);
    try testing.expectEqual(@as(i64, 42), result.i64);
}

test "mixed unsigned arithmetic returns u64" {
    const result = try applyBinaryOp(
        .{ .u8 = 2 },
        .{ .u16 = 5 },
        .star,
    );
    try testing.expect(result == .u64);
    try testing.expectEqual(@as(u64, 10), result.u64);
}

test "mixed signed and large u64 returns overflow" {
    const result = applyBinaryOp(
        .{ .u64 = std.math.maxInt(u64) },
        .{ .i64 = 1 },
        .plus,
    );
    try testing.expectError(error.NumericOverflow, result);
}

test "unary minus overflow returns error" {
    const result = applyUnaryOp(
        .{ .i64 = std.math.minInt(i64) },
        .minus,
    );
    try testing.expectError(error.NumericOverflow, result);
}

test "unary minus promotes u8/u16/u32 to i64" {
    const from_u8 = try applyUnaryOp(
        .{ .u8 = 7 },
        .minus,
    );
    try testing.expect(from_u8 == .i64);
    try testing.expectEqual(@as(i64, -7), from_u8.i64);

    const from_u16 = try applyUnaryOp(
        .{ .u16 = 511 },
        .minus,
    );
    try testing.expect(from_u16 == .i64);
    try testing.expectEqual(@as(i64, -511), from_u16.i64);

    const from_u32 = try applyUnaryOp(
        .{ .u32 = 70_000 },
        .minus,
    );
    try testing.expect(from_u32 == .i64);
    try testing.expectEqual(@as(i64, -70_000), from_u32.i64);
}

test "unary minus supports signed i64 minimum literal through expression path" {
    var tree = Ast{};
    const source = "-9223372036854775808";
    const tokens = tokenizer_mod.tokenize(source);
    const expr = try expression_mod.parseExpression(&tree, &tokens, "", 0);
    const schema = RowSchema{};
    const result = try evaluateExpression(
        &tree,
        &tokens,
        source,
        expr.node,
        &.{},
        &schema,
    );
    try testing.expect(result == .i64);
    try testing.expectEqual(std.math.minInt(i64), result.i64);
}

test "unary minus rejects values below signed i64 minimum" {
    var tree = Ast{};
    const source = "-9223372036854775809";
    const tokens = tokenizer_mod.tokenize(source);
    const expr = try expression_mod.parseExpression(&tree, &tokens, "", 0);
    const schema = RowSchema{};
    const result = evaluateExpression(
        &tree,
        &tokens,
        source,
        expr.node,
        &.{},
        &schema,
    );
    try testing.expectError(error.NumericOverflow, result);
}

test "abs overflow returns error" {
    const result = applyBuiltinFunction(.fn_abs, &[_]Value{
        .{ .i64 = std.math.minInt(i64) },
    });
    try testing.expectError(error.NumericOverflow, result);
}

test "predicate wrapper — true" {
    var tree = Ast{};
    const source = "true";
    const tokens = tokenizer_mod.tokenize(source);
    const node = try tree.addNode(.expr_literal, .{ .token = 0 });
    const schema = RowSchema{};

    const result = try evaluatePredicate(
        &tree,
        &tokens,
        source,
        node,
        &.{},
        &schema,
    );
    try testing.expect(result);
}

test "predicate wrapper — null returns error" {
    var tree = Ast{};
    const source = "null";
    const tokens = tokenizer_mod.tokenize(source);
    const node = try tree.addNode(.expr_literal, .{ .token = 0 });
    const schema = RowSchema{};

    const result = evaluatePredicate(
        &tree,
        &tokens,
        source,
        node,
        &.{},
        &schema,
    );
    try testing.expectError(error.NullInPredicate, result);
}

test "unary not" {
    const result = try applyUnaryOp(.{ .bool = true }, .bang);
    try testing.expect(!result.bool);
}

test "comparison less than" {
    const result = try applyBinaryOp(
        .{ .i64 = 3 },
        .{ .i64 = 5 },
        .less_than,
    );
    try testing.expect(result.bool);
}

test "comparison greater equal" {
    const result = try applyBinaryOp(
        .{ .i64 = 5 },
        .{ .i64 = 5 },
        .greater_equal,
    );
    try testing.expect(result.bool);
}

test "logical or supports null and true" {
    const result = try applyBinaryOp(
        .{ .null_value = {} },
        .{ .bool = true },
        .or_or,
    );
    try testing.expect(result == .bool);
    try testing.expect(result.bool);
}

test "logical and supports null and false" {
    const result = try applyBinaryOp(
        .{ .null_value = {} },
        .{ .bool = false },
        .and_and,
    );
    try testing.expect(result == .bool);
    try testing.expect(!result.bool);
}

test "logical or with null operands yields null" {
    const result = try applyBinaryOp(
        .{ .null_value = {} },
        .{ .null_value = {} },
        .or_or,
    );
    try testing.expect(result == .null_value);
}

test "equality with null operands returns bool for == and !=" {
    const both_null_eq = try applyBinaryOp(
        .{ .null_value = {} },
        .{ .null_value = {} },
        .equal_equal,
    );
    try testing.expect(both_null_eq == .bool);
    try testing.expect(both_null_eq.bool);

    const mixed_neq = try applyBinaryOp(
        .{ .i64 = 42 },
        .{ .null_value = {} },
        .not_equal,
    );
    try testing.expect(mixed_neq == .bool);
    try testing.expect(mixed_neq.bool);
}

test "ordering comparison with null yields null" {
    const result = try applyBinaryOp(
        .{ .i64 = 42 },
        .{ .null_value = {} },
        .less_than,
    );
    try testing.expect(result == .null_value);
}

test "equality rejects incompatible non-numeric types" {
    const result = applyBinaryOp(
        .{ .string = "1" },
        .{ .i64 = 1 },
        .equal_equal,
    );
    try testing.expectError(error.TypeMismatch, result);
}

test "f64 arithmetic" {
    const result = try applyBinaryOp(
        .{ .f64 = 2.5 },
        .{ .f64 = 1.5 },
        .star,
    );
    try testing.expectEqual(@as(f64, 3.75), result.f64);
}

test "string comparison" {
    const result = try applyBinaryOp(
        .{ .string = "abc" },
        .{ .string = "abc" },
        .equal_equal,
    );
    try testing.expect(result.bool);
}
