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
const scan_mod = @import("scan.zig");
const numeric_ops = @import("numeric_ops.zig");
const builtin_functions = @import("builtin_functions.zig");

const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const null_node = ast_mod.null_node;
const TokenType = tokenizer_mod.TokenType;
const TokenizeResult = tokenizer_mod.TokenizeResult;
const Value = row_mod.Value;
const RowSchema = row_mod.RowSchema;

/// Stack capacity for expression evaluation.
const max_eval_stack = 64;
const max_work_stack = 128;
pub const max_string_result_bytes = 1024;

// --- Delegated from numeric_ops module ---
const applyArithmetic = numeric_ops.applyArithmetic;
const applyComparison = numeric_ops.applyComparison;
const isNumericValue = numeric_ops.isNumericValue;
const membershipElementEquals = numeric_ops.membershipElementEquals;
pub const applyUnaryOp = numeric_ops.applyUnaryOp;

// --- Delegated from builtin_functions module ---
pub const applyBuiltinFunction = builtin_functions.applyBuiltinFunction;

pub const EvalError = error{
    StackOverflow,
    StackUnderflow,
    TypeMismatch,
    DivisionByZero,
    NumericOverflow,
    NumericDomain,
    NullArithmeticOperand,
    ColumnNotFound,
    InvalidLiteral,
    UnknownFunction,
    NullInPredicate,
    UndefinedParameter,
    UndefinedVariable,
    AmbiguousIdentifier,
    VariableTypeMismatch,
    VariableStorageRead,
    ClockUnavailable,
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

pub const VariableRef = union(enum) {
    not_found,
    scalar: Value,
    list: []const Value,
    list_spilled,
};

/// Bundles all ambient evaluation state needed during expression evaluation.
///
/// This struct consolidates what were previously individual parameters threaded
/// through ~30+ call sites. Each field is optional so callers only populate what
/// they need; missing capabilities surface as explicit errors (e.g. ClockUnavailable).
pub const EvalContext = struct {
    statement_timestamp_micros: ?i64 = null,
    parameter_resolver: ?*const ParameterResolver = null,
    variable_resolver_ctx: ?*const anyopaque = null,
    resolve_variable: ?*const fn (
        ctx: *const anyopaque,
        tokens: *const TokenizeResult,
        source: []const u8,
        token_index: u16,
    ) EvalError!VariableRef = null,
    resolve_spilled_membership_ctx: ?*const anyopaque = null,
    resolve_spilled_membership: ?*const fn (
        ctx: *const anyopaque,
        tokens: *const TokenizeResult,
        source: []const u8,
        list_token: u16,
        needle: Value,
    ) EvalError!Value = null,
    string_arena: ?*scan_mod.StringArena = null,
};

/// Work items for iterative post-order traversal.
const WorkItem = union(enum) {
    evaluate: NodeIndex,
    apply_binary: u16,
    apply_binary_short_circuit: struct { token_index: u16, rhs: NodeIndex },
    apply_binary_with_lhs: struct { token_index: u16, lhs: Value },
    apply_unary: u16,
    apply_column_ref: u16,
    apply_parameter: u16,
    apply_literal: u16,
    apply_aggregate: NodeIndex,
    apply_function: struct { token_index: u16, arg_count: u16 },
    apply_membership: struct { token_index: u16, list_count: u16 },
    apply_membership_variable: struct { token_index: u16, list_token: u16 },
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
    const empty_ctx = EvalContext{};
    return evaluateExpressionFull(
        tree,
        tokens,
        source,
        node_index,
        row_values,
        schema,
        null,
        &empty_ctx,
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
    const empty_ctx = EvalContext{};
    return evaluateExpressionFull(
        tree,
        tokens,
        source,
        node_index,
        row_values,
        schema,
        resolver,
        &empty_ctx,
    );
}

pub fn evaluateExpressionFull(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    node_index: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
    aggregate_resolver: ?*const AggregateResolver,
    eval_ctx: *const EvalContext,
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
                const val = if (tokens.tokens[tok_idx].token_type == .kw_current_timestamp) blk: {
                    const ts = eval_ctx.statement_timestamp_micros orelse return error.ClockUnavailable;
                    break :blk Value{ .timestamp = ts };
                } else try parseLiteralValue(tokens, source, tok_idx);
                try evalPush(&eval_stack, &eval_count, val);
            },
            .apply_column_ref => |tok_idx| {
                const name = tokens.getText(tok_idx, source);
                const col = schema.findColumn(name);
                const variable_ref = if (eval_ctx.resolve_variable) |resolve_variable|
                    try resolve_variable(
                        eval_ctx.variable_resolver_ctx orelse return error.UndefinedVariable,
                        tokens,
                        source,
                        tok_idx,
                    )
                else
                    VariableRef.not_found;
                if (col != null and variable_ref != .not_found) {
                    return error.AmbiguousIdentifier;
                }
                if (col) |column_idx| {
                    try evalPush(&eval_stack, &eval_count, row_values[column_idx]);
                    continue;
                }
                switch (variable_ref) {
                    .not_found => return error.ColumnNotFound,
                    .scalar => |scalar| try evalPush(&eval_stack, &eval_count, scalar),
                    .list, .list_spilled => return error.VariableTypeMismatch,
                }
            },
            .apply_parameter => |tok_idx| {
                const r = eval_ctx.parameter_resolver orelse return error.UndefinedParameter;
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
            .apply_binary_short_circuit => |info| {
                if (eval_count < 1) return error.StackUnderflow;
                const lhs = evalPop(&eval_stack, &eval_count);
                const op_type = tokens.tokens[info.token_index].token_type;
                switch (op_type) {
                    .and_and => {
                        if (lhs == .bool and lhs.bool == false) {
                            try evalPush(&eval_stack, &eval_count, Value{ .bool = false });
                            continue;
                        }
                    },
                    .or_or => {
                        if (lhs == .bool and lhs.bool == true) {
                            try evalPush(&eval_stack, &eval_count, Value{ .bool = true });
                            continue;
                        }
                    },
                    else => unreachable,
                }
                try workPush(&work_stack, &work_count, .{
                    .apply_binary_with_lhs = .{
                        .token_index = info.token_index,
                        .lhs = lhs,
                    },
                });
                try workPush(&work_stack, &work_count, .{
                    .evaluate = info.rhs,
                });
            },
            .apply_binary_with_lhs => |info| {
                if (eval_count < 1) return error.StackUnderflow;
                const rhs = evalPop(&eval_stack, &eval_count);
                const op_type = tokens.tokens[info.token_index].token_type;
                const result = try applyBinaryOp(info.lhs, rhs, op_type);
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
                    eval_ctx,
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
            .apply_membership_variable => |info| {
                if (eval_count < 1) return error.StackUnderflow;
                const needle = evalPop(&eval_stack, &eval_count);
                const resolve_variable = eval_ctx.resolve_variable orelse return error.UndefinedVariable;
                const variable_ref = try resolve_variable(
                    eval_ctx.variable_resolver_ctx orelse return error.UndefinedVariable,
                    tokens,
                    source,
                    info.list_token,
                );
                const list_values = switch (variable_ref) {
                    .not_found => return error.UndefinedVariable,
                    .scalar => return error.VariableTypeMismatch,
                    .list => |list| list,
                    .list_spilled => {
                        const resolve_spilled_membership = eval_ctx.resolve_spilled_membership orelse
                            return error.VariableStorageRead;
                        const value = try resolve_spilled_membership(
                            eval_ctx.resolve_spilled_membership_ctx orelse return error.VariableStorageRead,
                            tokens,
                            source,
                            info.list_token,
                            needle,
                        );
                        try evalPush(&eval_stack, &eval_count, value);
                        continue;
                    },
                };
                const result = try applyMembershipFunction(
                    tokens,
                    source,
                    info.token_index,
                    needle,
                    list_values,
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
    const empty_ctx = EvalContext{};
    return evaluatePredicateFull(
        tree,
        tokens,
        source,
        node_index,
        row_values,
        schema,
        null,
        &empty_ctx,
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
    const empty_ctx = EvalContext{};
    return evaluatePredicateFull(
        tree,
        tokens,
        source,
        node_index,
        row_values,
        schema,
        resolver,
        &empty_ctx,
    );
}

pub fn evaluatePredicateFull(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    node_index: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
    aggregate_resolver: ?*const AggregateResolver,
    eval_ctx: *const EvalContext,
) EvalError!bool {
    const val = try evaluateExpressionFull(
        tree,
        tokens,
        source,
        node_index,
        row_values,
        schema,
        aggregate_resolver,
        eval_ctx,
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
            const op_tok = node.extra;
            const op_type = tokens.tokens[op_tok].token_type;
            if (op_type == .and_and or op_type == .or_or) {
                // Evaluate lhs first. For short-circuit cases, rhs evaluation
                // is skipped entirely and the result is determined from lhs.
                try workPush(work_stack, work_count, .{
                    .apply_binary_short_circuit = .{
                        .token_index = op_tok,
                        .rhs = node.data.binary.rhs,
                    },
                });
                try workPush(work_stack, work_count, .{
                    .evaluate = node.data.binary.lhs,
                });
                return;
            }

            // Post-order: push apply first (executed last), then children.
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
                const list_arg_node = tree.getNode(list_arg);
                if (list_arg_node.tag == .expr_list) {
                    const list_head = list_arg_node.data.unary;
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
                } else if (list_arg_node.tag == .expr_column_ref) {
                    try workPush(work_stack, work_count, .{
                        .apply_membership_variable = .{
                            .token_index = fn_tok,
                            .list_token = list_arg_node.data.token,
                        },
                    });
                } else {
                    return error.TypeMismatch;
                }
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

test "logical or short-circuits undefined parameter rhs when lhs is true" {
    var tree = Ast{};
    const source = "true || $missing";
    const tokens = tokenizer_mod.tokenize(source);
    const expr = try expression_mod.parseExpression(&tree, &tokens, "", 0);
    const schema = RowSchema{};
    const fixture = ParameterFixture{ .bindings = &.{} };
    const resolver = ParameterResolver{
        .ctx = &fixture,
        .resolve = resolveParameterFromBindings,
    };
    const ctx = EvalContext{ .parameter_resolver = &resolver };

    const result = try evaluateExpressionFull(
        &tree,
        &tokens,
        source,
        expr.node,
        &.{},
        &schema,
        null,
        &ctx,
    );
    try testing.expect(result == .bool);
    try testing.expect(result.bool);
}

test "logical and short-circuits undefined parameter rhs when lhs is false" {
    var tree = Ast{};
    const source = "false && $missing";
    const tokens = tokenizer_mod.tokenize(source);
    const expr = try expression_mod.parseExpression(&tree, &tokens, "", 0);
    const schema = RowSchema{};
    const fixture = ParameterFixture{ .bindings = &.{} };
    const resolver = ParameterResolver{
        .ctx = &fixture,
        .resolve = resolveParameterFromBindings,
    };
    const ctx = EvalContext{ .parameter_resolver = &resolver };

    const result = try evaluateExpressionFull(
        &tree,
        &tokens,
        source,
        expr.node,
        &.{},
        &schema,
        null,
        &ctx,
    );
    try testing.expect(result == .bool);
    try testing.expect(!result.bool);
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

    const ctx = EvalContext{ .parameter_resolver = &resolver };
    const result = try evaluateExpressionFull(
        &tree,
        &tokens,
        source,
        node,
        &.{},
        &schema,
        null,
        &ctx,
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
    const ctx = EvalContext{};
    const result = try applyBuiltinFunction(.fn_abs, &[_]Value{
        .{ .i64 = -5 },
    }, &ctx);
    try testing.expectEqual(@as(i64, 5), result.i64);
}

test "function call — sqrt" {
    const ctx = EvalContext{};
    const result = try applyBuiltinFunction(.fn_sqrt, &[_]Value{
        .{ .f64 = 9.0 },
    }, &ctx);
    try testing.expectEqual(@as(f64, 3.0), result.f64);
}

test "function call — sqrt fails closed on negative input" {
    const ctx = EvalContext{};
    const result = applyBuiltinFunction(.fn_sqrt, &[_]Value{
        .{ .f64 = -1.0 },
    }, &ctx);
    try testing.expectError(error.NumericDomain, result);
}

test "function call — length" {
    const ctx = EvalContext{};
    const result = try applyBuiltinFunction(.fn_length, &[_]Value{
        .{ .string = "hello" },
    }, &ctx);
    try testing.expectEqual(@as(i64, 5), result.i64);
}

test "function call — round applies nearest-even tie handling" {
    const ctx = EvalContext{};
    const positive_tie = try applyBuiltinFunction(.fn_round, &[_]Value{
        .{ .f64 = 2.5 },
    }, &ctx);
    try testing.expectEqual(@as(f64, 2.0), positive_tie.f64);

    const negative_tie = try applyBuiltinFunction(.fn_round, &[_]Value{
        .{ .f64 = -2.5 },
    }, &ctx);
    try testing.expectEqual(@as(f64, -2.0), negative_tie.f64);
}

test "function call — coalesce" {
    const ctx = EvalContext{};
    const result = try applyBuiltinFunction(.fn_coalesce, &[_]Value{
        .{ .null_value = {} },
        .{ .i64 = 42 },
    }, &ctx);
    try testing.expectEqual(@as(i64, 42), result.i64);
}

test "function call — abs fails closed on arity mismatch" {
    const ctx = EvalContext{};
    const result = applyBuiltinFunction(.fn_abs, &[_]Value{
        .{ .i64 = 1 },
        .{ .i64 = 2 },
    }, &ctx);
    try testing.expectError(error.TypeMismatch, result);
}

test "function call — sqrt fails closed on arity mismatch" {
    const ctx = EvalContext{};
    const result = applyBuiltinFunction(.fn_sqrt, &[_]Value{
        .{ .i64 = 9 },
        .{ .i64 = 1 },
    }, &ctx);
    try testing.expectError(error.TypeMismatch, result);
}

test "function call — round fails closed on arity mismatch" {
    const ctx = EvalContext{};
    const result = applyBuiltinFunction(.fn_round, &[_]Value{
        .{ .f64 = 1.5 },
        .{ .f64 = 2.0 },
    }, &ctx);
    try testing.expectError(error.TypeMismatch, result);
}

test "function call — length fails closed on arity mismatch" {
    const ctx = EvalContext{};
    const result = applyBuiltinFunction(.fn_length, &[_]Value{
        .{ .string = "x" },
        .{ .string = "y" },
    }, &ctx);
    try testing.expectError(error.TypeMismatch, result);
}

test "function call — coalesce fails closed on empty args" {
    const ctx = EvalContext{};
    const result = applyBuiltinFunction(.fn_coalesce, &[_]Value{}, &ctx);
    try testing.expectError(error.TypeMismatch, result);
}

test "current_timestamp returns injected microsecond timestamp" {
    var tree = Ast{};
    const source = "CurrentTimestamp";
    const tokens = tokenizer_mod.tokenize(source);
    const node = try makeAstLiteral(&tree, 0);
    const schema = RowSchema{};

    const ctx = EvalContext{ .statement_timestamp_micros = 1700000000123456 };
    const result = try evaluateExpressionFull(
        &tree,
        &tokens,
        source,
        node,
        &.{},
        &schema,
        null,
        &ctx,
    );
    try testing.expect(result == .timestamp);
    try testing.expectEqual(@as(i64, 1700000000123456), result.timestamp);
}

test "current_timestamp fails closed without injected timestamp" {
    var tree = Ast{};
    const source = "CurrentTimestamp";
    const tokens = tokenizer_mod.tokenize(source);
    const node = try makeAstLiteral(&tree, 0);
    const schema = RowSchema{};

    const ctx = EvalContext{};
    const result = evaluateExpressionFull(
        &tree,
        &tokens,
        source,
        node,
        &.{},
        &schema,
        null,
        &ctx,
    );
    try testing.expectError(error.ClockUnavailable, result);
}

test "function call — lower applies ASCII-only folding" {
    var arena_bytes: [128]u8 = undefined;
    var arena = scan_mod.StringArena.init(arena_bytes[0..]);
    const ctx = EvalContext{ .string_arena = &arena };
    const result = try applyBuiltinFunction(
        .fn_lower,
        &[_]Value{.{ .string = "HELLO \xC3\x84\xC3\x96\xC3\x9C" }},
        &ctx,
    );
    try testing.expectEqualStrings(
        "hello \xC3\x84\xC3\x96\xC3\x9C",
        result.string,
    );
}

test "function call — upper applies ASCII-only folding" {
    var arena_bytes: [128]u8 = undefined;
    var arena = scan_mod.StringArena.init(arena_bytes[0..]);
    const ctx = EvalContext{ .string_arena = &arena };
    const result = try applyBuiltinFunction(
        .fn_upper,
        &[_]Value{.{ .string = "hello \xC3\xA4\xC3\xB6\xC3\xBC" }},
        &ctx,
    );
    try testing.expectEqualStrings(
        "HELLO \xC3\xA4\xC3\xB6\xC3\xBC",
        result.string,
    );
}

test "function call — trim removes ASCII spaces only" {
    var arena_bytes: [128]u8 = undefined;
    var arena = scan_mod.StringArena.init(arena_bytes[0..]);
    const ctx = EvalContext{ .string_arena = &arena };
    const result = try applyBuiltinFunction(
        .fn_trim,
        &[_]Value{.{ .string = "  hello\t " }},
        &ctx,
    );
    try testing.expectEqualStrings("hello\t", result.string);
}

test "function call — lower fails closed on arity mismatch" {
    const ctx = EvalContext{};
    const result = applyBuiltinFunction(.fn_lower, &[_]Value{
        .{ .string = "x" },
        .{ .string = "y" },
    }, &ctx);
    try testing.expectError(error.TypeMismatch, result);
}

test "function call — upper fails closed on arity mismatch" {
    const ctx = EvalContext{};
    const result = applyBuiltinFunction(.fn_upper, &[_]Value{
        .{ .string = "x" },
        .{ .string = "y" },
    }, &ctx);
    try testing.expectError(error.TypeMismatch, result);
}

test "function call — trim fails closed on arity mismatch" {
    const ctx = EvalContext{};
    const result = applyBuiltinFunction(.fn_trim, &[_]Value{
        .{ .string = "x" },
        .{ .string = "y" },
    }, &ctx);
    try testing.expectError(error.TypeMismatch, result);
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
    const ctx = EvalContext{};
    const result = applyBuiltinFunction(.fn_abs, &[_]Value{
        .{ .i64 = std.math.minInt(i64) },
    }, &ctx);
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
