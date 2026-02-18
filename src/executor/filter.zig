const std = @import("std");
const ast_mod = @import("../parser/ast.zig");
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
    ColumnNotFound,
    InvalidLiteral,
    UnknownFunction,
    NullInPredicate,
};

/// Work items for iterative post-order traversal.
const WorkItem = union(enum) {
    evaluate: NodeIndex,
    apply_binary: u16,
    apply_unary: u16,
    apply_column_ref: u16,
    apply_literal: u16,
    apply_function: struct { token_index: u16, arg_count: u16 },
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
                tree, tokens, idx, &work_stack, &work_count,
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
                    fn_type, args[0..count],
                );
                try evalPush(&eval_stack, &eval_count, result);
            },
        }
    }

    if (eval_count == 0) return error.StackUnderflow;
    return eval_stack[eval_count - 1];
}

/// Evaluate an expression and assert the result is boolean.
pub fn evaluatePredicate(
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    node_index: NodeIndex,
    row_values: []const Value,
    schema: *const RowSchema,
) EvalError!bool {
    const val = try evaluateExpression(
        tree, tokens, source, node_index, row_values, schema,
    );
    if (val == .null_value) return error.NullInPredicate;
    if (val != .boolean) return error.TypeMismatch;
    return val.boolean;
}

/// Push work items for an AST node (post-order: push apply first, then children).
fn pushNodeWork(
    tree: *const Ast,
    tokens: *const TokenizeResult,
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
            const arg_count = tree.listLen(node.data.unary);
            try workPush(work_stack, work_count, .{
                .apply_function = .{
                    .token_index = fn_tok,
                    .arg_count = arg_count,
                },
            });
            // Push args in reverse order (last arg pushed first onto work stack).
            try pushLinkedListReverse(
                tree, work_stack, work_count, node.data.unary, arg_count,
            );
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
            const v = std.fmt.parseInt(i64, text, 10) catch
                return error.InvalidLiteral;
            break :blk Value{ .bigint = v };
        },
        .float_literal => blk: {
            const v = std.fmt.parseFloat(f64, text) catch
                return error.InvalidLiteral;
            break :blk Value{ .float = v };
        },
        .string_literal => blk: {
            // Strip surrounding quotes.
            if (text.len >= 2 and text[0] == '"' and text[text.len - 1] == '"') {
                break :blk Value{ .string = text[1 .. text.len - 1] };
            }
            break :blk Value{ .string = text };
        },
        .true_literal => Value{ .boolean = true },
        .false_literal => Value{ .boolean = false },
        .null_literal => Value{ .null_value = {} },
        else => error.InvalidLiteral,
    };
}

/// Apply a binary operator to two values.
pub fn applyBinaryOp(lhs: Value, rhs: Value, op: TokenType) EvalError!Value {
    // Null propagation: any op with null yields null (except logical and/or).
    if (op != .kw_and and op != .kw_or) {
        if (lhs == .null_value or rhs == .null_value) {
            return Value{ .null_value = {} };
        }
    }

    return switch (op) {
        .plus => applyArithmetic(lhs, rhs, .add),
        .minus => applyArithmetic(lhs, rhs, .sub),
        .star => applyArithmetic(lhs, rhs, .mul),
        .slash => applyArithmetic(lhs, rhs, .div),
        .equal => applyComparison(lhs, rhs, .eq),
        .not_equal => applyComparison(lhs, rhs, .neq),
        .less_than => applyComparison(lhs, rhs, .lt),
        .less_equal => applyComparison(lhs, rhs, .lte),
        .greater_than => applyComparison(lhs, rhs, .gt),
        .greater_equal => applyComparison(lhs, rhs, .gte),
        .kw_and => applyLogical(lhs, rhs, .@"and"),
        .kw_or => applyLogical(lhs, rhs, .@"or"),
        else => error.TypeMismatch,
    };
}

const ArithOp = enum { add, sub, mul, div };

fn applyArithmetic(lhs: Value, rhs: Value, op: ArithOp) EvalError!Value {
    // Integer arithmetic.
    if (lhs == .bigint and rhs == .bigint) {
        const a = lhs.bigint;
        const b = rhs.bigint;
        return Value{ .bigint = switch (op) {
            .add => std.math.add(i64, a, b) catch return error.NumericOverflow,
            .sub => std.math.sub(i64, a, b) catch return error.NumericOverflow,
            .mul => std.math.mul(i64, a, b) catch return error.NumericOverflow,
            .div => blk: {
                if (b == 0) return error.DivisionByZero;
                if (a == std.math.minInt(i64) and b == -1) return error.NumericOverflow;
                break :blk @divTrunc(a, b);
            },
        } };
    }
    if (lhs == .int and rhs == .int) {
        const a = lhs.int;
        const b = rhs.int;
        return Value{ .int = switch (op) {
            .add => std.math.add(i32, a, b) catch return error.NumericOverflow,
            .sub => std.math.sub(i32, a, b) catch return error.NumericOverflow,
            .mul => std.math.mul(i32, a, b) catch return error.NumericOverflow,
            .div => blk: {
                if (b == 0) return error.DivisionByZero;
                if (a == std.math.minInt(i32) and b == -1) return error.NumericOverflow;
                break :blk @divTrunc(a, b);
            },
        } };
    }
    // Float arithmetic (promote if mixed).
    const a = toFloat(lhs) orelse return error.TypeMismatch;
    const b = toFloat(rhs) orelse return error.TypeMismatch;
    return Value{ .float = switch (op) {
        .add => a + b,
        .sub => a - b,
        .mul => a * b,
        .div => blk: {
            if (b == 0.0) return error.DivisionByZero;
            break :blk a / b;
        },
    } };
}

fn toFloat(v: Value) ?f64 {
    return switch (v) {
        .float => |f| f,
        .bigint => |i| @as(f64, @floatFromInt(i)),
        .int => |i| @as(f64, @floatFromInt(i)),
        else => null,
    };
}

const CmpOp = enum { eq, neq, lt, lte, gt, gte };

fn applyComparison(lhs: Value, rhs: Value, op: CmpOp) EvalError!Value {
    // Handle null = null and null != null specifically.
    if (lhs == .null_value and rhs == .null_value) {
        return Value{ .boolean = op == .eq };
    }
    if (lhs == .null_value or rhs == .null_value) {
        return Value{ .boolean = op == .neq };
    }

    const ord = row_mod.compareValues(lhs, rhs);
    return Value{ .boolean = switch (op) {
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
            if (a != null and !a.?) break :blk Value{ .boolean = false };
            if (b != null and !b.?) break :blk Value{ .boolean = false };
            if (a != null and b != null) break :blk Value{
                .boolean = a.? and b.?,
            };
            break :blk Value{ .null_value = {} };
        },
        .@"or" => blk: {
            if (a != null and a.?) break :blk Value{ .boolean = true };
            if (b != null and b.?) break :blk Value{ .boolean = true };
            if (a != null and b != null) break :blk Value{
                .boolean = a.? or b.?,
            };
            break :blk Value{ .null_value = {} };
        },
    };
}

fn toBool(v: Value) ?bool {
    return switch (v) {
        .boolean => |b| b,
        .null_value => null,
        else => null,
    };
}

/// Apply a unary operator.
pub fn applyUnaryOp(operand: Value, op: TokenType) EvalError!Value {
    if (operand == .null_value) return Value{ .null_value = {} };

    return switch (op) {
        .kw_not => blk: {
            if (operand != .boolean) return error.TypeMismatch;
            break :blk Value{ .boolean = !operand.boolean };
        },
        .minus => blk: {
            if (operand == .bigint) {
                if (operand.bigint == std.math.minInt(i64)) return error.NumericOverflow;
                break :blk Value{ .bigint = -operand.bigint };
            }
            if (operand == .int) {
                if (operand.int == std.math.minInt(i32)) return error.NumericOverflow;
                break :blk Value{ .int = -operand.int };
            }
            if (operand == .float) break :blk Value{ .float = -operand.float };
            return error.TypeMismatch;
        },
        else => error.TypeMismatch,
    };
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
            if (v == .bigint) {
                if (v.bigint == std.math.minInt(i64)) return error.NumericOverflow;
                break :blk Value{
                    .bigint = if (v.bigint < 0) -v.bigint else v.bigint,
                };
            }
            if (v == .int) {
                if (v.int == std.math.minInt(i32)) return error.NumericOverflow;
                break :blk Value{
                    .int = if (v.int < 0) -v.int else v.int,
                };
            }
            if (v == .float) break :blk Value{ .float = @abs(v.float) };
            return error.TypeMismatch;
        },
        .fn_sqrt => blk: {
            if (args.len < 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            const f = toFloat(v) orelse return error.TypeMismatch;
            break :blk Value{ .float = @sqrt(f) };
        },
        .fn_round => blk: {
            if (args.len < 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            const f = toFloat(v) orelse return error.TypeMismatch;
            break :blk Value{ .float = @round(f) };
        },
        .fn_length => blk: {
            if (args.len < 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            if (v != .string) return error.TypeMismatch;
            break :blk Value{ .bigint = @intCast(v.string.len) };
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

test "literal integer evaluation" {
    var tree = Ast{};
    const tokens = tokenizer_mod.tokenize("42");
    const node = try makeAstLiteral(&tree, 0);
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree, &tokens, "42", node, &.{}, &schema,
    );
    try testing.expectEqual(@as(i64, 42), result.bigint);
}

test "literal float evaluation" {
    var tree = Ast{};
    const tokens = tokenizer_mod.tokenize("3.14");
    const node = try makeAstLiteral(&tree, 0);
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree, &tokens, "3.14", node, &.{}, &schema,
    );
    try testing.expectEqual(@as(f64, 3.14), result.float);
}

test "literal string evaluation" {
    var tree = Ast{};
    const source = "\"hello\"";
    const tokens = tokenizer_mod.tokenize(source);
    const node = try makeAstLiteral(&tree, 0);
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree, &tokens, source, node, &.{}, &schema,
    );
    try testing.expectEqualSlices(u8, "hello", result.string);
}

test "literal boolean evaluation" {
    var tree = Ast{};
    const tokens = tokenizer_mod.tokenize("true");
    const node = try makeAstLiteral(&tree, 0);
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree, &tokens, "true", node, &.{}, &schema,
    );
    try testing.expect(result.boolean);
}

test "literal null evaluation" {
    var tree = Ast{};
    const tokens = tokenizer_mod.tokenize("null");
    const node = try makeAstLiteral(&tree, 0);
    const schema = RowSchema{};

    const result = try evaluateExpression(
        &tree, &tokens, "null", node, &.{}, &schema,
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
        &tree, &tokens, source, bin, &.{}, &schema,
    );
    try testing.expectEqual(@as(i64, 30), result.bigint);
}

test "binary comparison" {
    var tree = Ast{};
    const source = "5 = 5";
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
        &tree, &tokens, source, bin, &.{}, &schema,
    );
    try testing.expect(result.boolean);
}

test "logical and" {
    var tree = Ast{};
    const source = "true and false";
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
        &tree, &tokens, source, bin, &.{}, &schema,
    );
    try testing.expect(!result.boolean);
}

test "logical or" {
    var tree = Ast{};
    const source = "true or false";
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
        &tree, &tokens, source, bin, &.{}, &schema,
    );
    try testing.expect(result.boolean);
}

test "column reference evaluation" {
    var tree = Ast{};
    const source = "age";
    const tokens = tokenizer_mod.tokenize(source);
    const node = try tree.addNode(.expr_column_ref, .{ .token = 0 });

    var schema = RowSchema{};
    _ = try schema.addColumn("age", .bigint, false);
    const row_values = [_]Value{.{ .bigint = 25 }};

    const result = try evaluateExpression(
        &tree, &tokens, source, node, &row_values, &schema,
    );
    try testing.expectEqual(@as(i64, 25), result.bigint);
}

test "function call — abs" {
    const result = try applyBuiltinFunction(.fn_abs, &[_]Value{
        .{ .bigint = -5 },
    });
    try testing.expectEqual(@as(i64, 5), result.bigint);
}

test "function call — sqrt" {
    const result = try applyBuiltinFunction(.fn_sqrt, &[_]Value{
        .{ .float = 9.0 },
    });
    try testing.expectEqual(@as(f64, 3.0), result.float);
}

test "function call — length" {
    const result = try applyBuiltinFunction(.fn_length, &[_]Value{
        .{ .string = "hello" },
    });
    try testing.expectEqual(@as(i64, 5), result.bigint);
}

test "function call — coalesce" {
    const result = try applyBuiltinFunction(.fn_coalesce, &[_]Value{
        .{ .null_value = {} },
        .{ .bigint = 42 },
    });
    try testing.expectEqual(@as(i64, 42), result.bigint);
}

test "null propagation in arithmetic" {
    const result = try applyBinaryOp(
        .{ .bigint = 5 },
        .{ .null_value = {} },
        .plus,
    );
    try testing.expect(result == .null_value);
}

test "division by zero" {
    const result = applyBinaryOp(
        .{ .bigint = 5 },
        .{ .bigint = 0 },
        .slash,
    );
    try testing.expectError(error.DivisionByZero, result);
}

test "bigint arithmetic overflow returns error" {
    const result = applyBinaryOp(
        .{ .bigint = std.math.maxInt(i64) },
        .{ .bigint = 1 },
        .plus,
    );
    try testing.expectError(error.NumericOverflow, result);
}

test "int arithmetic overflow returns error" {
    const result = applyBinaryOp(
        .{ .int = std.math.maxInt(i32) },
        .{ .int = 2 },
        .star,
    );
    try testing.expectError(error.NumericOverflow, result);
}

test "unary minus overflow returns error" {
    const result = applyUnaryOp(
        .{ .bigint = std.math.minInt(i64) },
        .minus,
    );
    try testing.expectError(error.NumericOverflow, result);
}

test "abs overflow returns error" {
    const result = applyBuiltinFunction(.fn_abs, &[_]Value{
        .{ .bigint = std.math.minInt(i64) },
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
        &tree, &tokens, source, node, &.{}, &schema,
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
        &tree, &tokens, source, node, &.{}, &schema,
    );
    try testing.expectError(error.NullInPredicate, result);
}

test "unary not" {
    const result = try applyUnaryOp(.{ .boolean = true }, .kw_not);
    try testing.expect(!result.boolean);
}

test "comparison less than" {
    const result = try applyBinaryOp(
        .{ .bigint = 3 },
        .{ .bigint = 5 },
        .less_than,
    );
    try testing.expect(result.boolean);
}

test "comparison greater equal" {
    const result = try applyBinaryOp(
        .{ .bigint = 5 },
        .{ .bigint = 5 },
        .greater_equal,
    );
    try testing.expect(result.boolean);
}

test "float arithmetic" {
    const result = try applyBinaryOp(
        .{ .float = 2.5 },
        .{ .float = 1.5 },
        .star,
    );
    try testing.expectEqual(@as(f64, 3.75), result.float);
}

test "string comparison" {
    const result = try applyBinaryOp(
        .{ .string = "abc" },
        .{ .string = "abc" },
        .equal,
    );
    try testing.expect(result.boolean);
}
