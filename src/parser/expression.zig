//! Expression parser (shunting-yard style) for pg2 syntax.
//!
//! Responsibilities in this file:
//! - Parses expression tokens into AST nodes with precedence handling.
//! - Supports literals, refs, lists, function calls, and aggregate calls.
//! - Produces deterministic parse outputs with explicit stack limits.
//! - Fails closed on malformed syntax (mismatched delimiters, bad arity, etc.).
const std = @import("std");
const ast_mod = @import("ast.zig");
const tokenizer_mod = @import("tokenizer.zig");

const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const NodeTag = ast_mod.NodeTag;
const NodeData = ast_mod.NodeData;
const null_node = ast_mod.null_node;
const Token = tokenizer_mod.Token;
const TokenType = tokenizer_mod.TokenType;
const TokenizeResult = tokenizer_mod.TokenizeResult;

/// Maximum depth of operator stack for shunting-yard.
const max_operator_stack = 64;
/// Maximum depth of output stack for shunting-yard.
const max_output_stack = 128;
/// Maximum nested list/function/aggregate container depth.
const max_expression_nesting = 32;

pub const ExprError = error{
    AstFull,
    UnexpectedToken,
    StackOverflow,
    StackUnderflow,
    MismatchedParentheses,
};

/// Operator precedence (lower number = higher precedence, binds tighter).
fn precedence(tok_type: TokenType) u8 {
    return switch (tok_type) {
        .bang => 1,
        .star, .slash => 2,
        .plus, .minus => 3,
        .equal_equal, .not_equal, .less_than, .greater_than, .less_equal, .greater_equal => 4,
        .and_and => 5,
        .or_or => 6,
        else => 255,
    };
}

fn isRightAssociative(tok_type: TokenType) bool {
    return tok_type == .bang;
}

fn isOperator(tok_type: TokenType) bool {
    return switch (tok_type) {
        .plus,
        .minus,
        .star,
        .slash,
        .equal_equal,
        .not_equal,
        .less_than,
        .greater_than,
        .less_equal,
        .greater_equal,
        .and_and,
        .or_or,
        => true,
        else => false,
    };
}

fn isEndOfExpression(tok_type: TokenType) bool {
    return switch (tok_type) {
        .pipe_arrow,
        .left_brace,
        .right_brace,
        .kw_asc,
        .kw_desc,
        .comma,
        .right_bracket,
        .end_of_input,
        => true,
        else => false,
    };
}

fn isUnaryPrefix(tok_type: TokenType) bool {
    return tok_type == .bang or tok_type == .minus;
}

/// An entry on the operator stack during shunting-yard.
const OpEntry = struct {
    tok_type: TokenType,
    tok_index: u16,
    is_unary: bool,
};

const ParseExpressionResult = struct {
    node: NodeIndex,
    pos: u16,
};

const ContainerKind = enum {
    list,
    function_call,
    aggregate_call,
};

const ContainerFrame = struct {
    kind: ContainerKind,
    token_index: u16,
    base_op_count: u16,
    base_output_count: u16,
    first: NodeIndex = null_node,
    last: NodeIndex = null_node,
    item_count: u16 = 0,
    saw_star: bool = false,
};

/// Parse an expression starting at `pos`, writing nodes into `ast`.
/// Returns the root NodeIndex of the expression and the new token position.
pub fn parseExpression(
    ast: *Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    start_pos: u16,
) ExprError!ParseExpressionResult {
    var op_stack: [max_operator_stack]OpEntry = undefined;
    var op_count: u16 = 0;
    var output_stack: [max_output_stack]NodeIndex = undefined;
    var output_count: u16 = 0;
    var container_stack: [max_expression_nesting]ContainerFrame = undefined;
    var container_count: u16 = 0;

    var pos = start_pos;
    var expect_operand = true;

    while (pos < tokens.count) {
        const tok = tokens.tokens[pos];

        if (expect_operand) {
            if (isUnaryPrefix(tok.token_type)) {
                if (op_count >= max_operator_stack) return error.StackOverflow;
                op_stack[op_count] = .{
                    .tok_type = tok.token_type,
                    .tok_index = pos,
                    .is_unary = true,
                };
                op_count += 1;
                pos += 1;
                continue;
            }

            if (container_count > 0) {
                const top = &container_stack[container_count - 1];
                if (isContainerClose(top.kind, tok.token_type)) {
                    if (top.item_count > 0 and !top.saw_star) return error.UnexpectedToken;
                    try closeContainer(
                        ast,
                        top.*,
                        tokens,
                        source,
                        &op_stack,
                        &op_count,
                        &output_stack,
                        &output_count,
                        &container_count,
                    );
                    pos += 1;
                    expect_operand = false;
                    continue;
                }
                if (tok.token_type == .star and top.kind == .aggregate_call) {
                    if (top.saw_star or top.item_count > 0) return error.UnexpectedToken;
                    if (output_count != top.base_output_count) return error.UnexpectedToken;
                    top.saw_star = true;
                    top.item_count = 1;
                    pos += 1;
                    expect_operand = false;
                    continue;
                }
            }

            if (tok.token_type == .left_bracket) {
                if (container_count >= max_expression_nesting) return error.StackOverflow;
                container_stack[container_count] = .{
                    .kind = .list,
                    .token_index = pos,
                    .base_op_count = op_count,
                    .base_output_count = output_count,
                };
                container_count += 1;
                pos += 1;
                continue;
            }

            if (tok.token_type == .left_brace) {
                const obj = try parseObjectLiteral(ast, tokens, source, pos);
                if (output_count >= max_output_stack) return error.StackOverflow;
                output_stack[output_count] = obj.node;
                output_count += 1;
                pos = obj.pos;
                expect_operand = false;
                continue;
            }

            if (isFunctionStart(tokens, pos) and
                pos + 1 < tokens.count and
                tokens.tokens[pos + 1].token_type == .left_paren)
            {
                if (container_count >= max_expression_nesting) return error.StackOverflow;
                container_stack[container_count] = .{
                    .kind = .function_call,
                    .token_index = pos,
                    .base_op_count = op_count,
                    .base_output_count = output_count,
                };
                container_count += 1;
                pos += 2;
                continue;
            }

            if (isAggregateToken(tok.token_type) and
                pos + 1 < tokens.count and
                tokens.tokens[pos + 1].token_type == .left_paren)
            {
                if (container_count >= max_expression_nesting) return error.StackOverflow;
                container_stack[container_count] = .{
                    .kind = .aggregate_call,
                    .token_index = pos,
                    .base_op_count = op_count,
                    .base_output_count = output_count,
                };
                container_count += 1;
                pos += 2;
                continue;
            }

            if (tok.token_type == .left_paren) {
                if (op_count >= max_operator_stack) return error.StackOverflow;
                op_stack[op_count] = .{
                    .tok_type = .left_paren,
                    .tok_index = pos,
                    .is_unary = false,
                };
                op_count += 1;
                pos += 1;
                continue;
            }

            if (isLiteral(tok.token_type)) {
                const node = try ast.addNode(.expr_literal, .{ .token = pos });
                if (output_count >= max_output_stack) return error.StackOverflow;
                output_stack[output_count] = node;
                output_count += 1;
                pos += 1;
                expect_operand = false;
                continue;
            }

            if (tokenizer_mod.isContextualIdentifier(tok.token_type) or tok.token_type == .model_name) {
                const node = try ast.addNode(.expr_column_ref, .{ .token = pos });
                if (output_count >= max_output_stack) return error.StackOverflow;
                output_stack[output_count] = node;
                output_count += 1;
                pos += 1;
                expect_operand = false;
                continue;
            }

            if (tok.token_type == .parameter) {
                const node = try ast.addNode(.expr_parameter, .{ .token = pos });
                if (output_count >= max_output_stack) return error.StackOverflow;
                output_stack[output_count] = node;
                output_count += 1;
                pos += 1;
                expect_operand = false;
                continue;
            }

            if (tok.token_type == .star) {
                const node = try ast.addNode(.expr_literal, .{ .token = pos });
                if (output_count >= max_output_stack) return error.StackOverflow;
                output_stack[output_count] = node;
                output_count += 1;
                pos += 1;
                expect_operand = false;
                continue;
            }

            break;
        }

        if (container_count > 0) {
            const top = &container_stack[container_count - 1];
            if (tok.token_type == .comma) {
                if (top.kind == .aggregate_call) return error.UnexpectedToken;
                try flushToBase(
                    &op_stack,
                    &op_count,
                    &output_stack,
                    &output_count,
                    ast,
                    top.base_op_count,
                );
                try captureContainerItem(ast, top, &output_stack, &output_count);
                pos += 1;
                expect_operand = true;
                continue;
            }
            if (isContainerClose(top.kind, tok.token_type)) {
                try closeContainer(
                    ast,
                    top.*,
                    tokens,
                    source,
                    &op_stack,
                    &op_count,
                    &output_stack,
                    &output_count,
                    &container_count,
                );
                pos += 1;
                expect_operand = false;
                continue;
            }
        }

        if (isEndOfExpression(tok.token_type) and !expect_operand) break;

        if (tok.token_type == .right_paren) {
            while (op_count > 0 and op_stack[op_count - 1].tok_type != .left_paren) {
                try popOperator(&op_stack, &op_count, &output_stack, &output_count, ast);
            }
            if (op_count == 0) {
                break;
            }
            op_count -= 1;
            pos += 1;
            continue;
        }

        if (isOperator(tok.token_type)) {
            const prec = precedence(tok.token_type);
            try flushOperators(&op_stack, &op_count, &output_stack, &output_count, ast, prec);

            if (op_count >= max_operator_stack) return error.StackOverflow;
            op_stack[op_count] = .{
                .tok_type = tok.token_type,
                .tok_index = pos,
                .is_unary = false,
            };
            op_count += 1;
            pos += 1;
            expect_operand = true;
            continue;
        }

        break;
    }

    if (container_count != 0) return error.MismatchedParentheses;

    while (op_count > 0) {
        if (op_stack[op_count - 1].tok_type == .left_paren) {
            return error.MismatchedParentheses;
        }
        try popOperator(&op_stack, &op_count, &output_stack, &output_count, ast);
    }

    if (output_count == 0) return error.UnexpectedToken;
    if (output_count != 1) return error.UnexpectedToken;
    return .{ .node = output_stack[output_count - 1], .pos = pos };
}

fn isContainerClose(kind: ContainerKind, tok_type: TokenType) bool {
    return switch (kind) {
        .list => tok_type == .right_bracket,
        .function_call, .aggregate_call => tok_type == .right_paren,
    };
}

fn parseObjectLiteral(
    ast: *Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    start_pos: u16,
) ExprError!ParseExpressionResult {
    var pos = start_pos + 1; // skip '{'
    var first_field: NodeIndex = null_node;
    var last_field: NodeIndex = null_node;

    while (pos < tokens.count) {
        const tok_type = tokens.tokens[pos].token_type;
        if (tok_type == .right_brace) {
            pos += 1;
            const obj = try ast.addNode(.expr_object, .{ .unary = first_field });
            return .{ .node = obj, .pos = pos };
        }

        const key_tok = pos;
        if (!tokenizer_mod.isContextualIdentifier(tok_type) and tok_type != .string_literal) {
            return error.UnexpectedToken;
        }
        pos += 1;

        if (pos >= tokens.count or tokens.tokens[pos].token_type != .colon) {
            return error.UnexpectedToken;
        }
        pos += 1;

        const value_expr = try parseExpression(ast, tokens, source, pos);
        const field = try ast.addNodeFull(
            .expr_object_field,
            .{ .unary = value_expr.node },
            key_tok,
            null_node,
        );
        if (first_field == null_node) {
            first_field = field;
            last_field = field;
        } else {
            ast.setNext(last_field, field);
            last_field = field;
        }
        pos = value_expr.pos;
        if (pos >= tokens.count) return error.UnexpectedToken;

        if (tokens.tokens[pos].token_type == .comma) {
            pos += 1;
            continue;
        }
        // Comma is optional; whitespace-separated pairs are allowed.
    }

    return error.MismatchedParentheses;
}

fn flushToBase(
    op_stack: *[max_operator_stack]OpEntry,
    op_count: *u16,
    output_stack: *[max_output_stack]NodeIndex,
    output_count: *u16,
    ast: *Ast,
    base_op_count: u16,
) ExprError!void {
    while (op_count.* > base_op_count) {
        if (op_stack[op_count.* - 1].tok_type == .left_paren) return error.MismatchedParentheses;
        try popOperator(op_stack, op_count, output_stack, output_count, ast);
    }
}

fn captureContainerItem(
    ast: *Ast,
    frame: *ContainerFrame,
    output_stack: *[max_output_stack]NodeIndex,
    output_count: *u16,
) ExprError!void {
    if (output_count.* <= frame.base_output_count) return error.UnexpectedToken;
    if (output_count.* != frame.base_output_count + 1) return error.UnexpectedToken;
    output_count.* -= 1;
    const item = output_stack[output_count.*];

    if (frame.first == null_node) {
        frame.first = item;
        frame.last = item;
    } else {
        ast.setNext(frame.last, item);
        frame.last = item;
    }
    frame.item_count += 1;
}

fn closeContainer(
    ast: *Ast,
    frame_value: ContainerFrame,
    tokens: *const TokenizeResult,
    source: []const u8,
    op_stack: *[max_operator_stack]OpEntry,
    op_count: *u16,
    output_stack: *[max_output_stack]NodeIndex,
    output_count: *u16,
    container_count: *u16,
) ExprError!void {
    var frame = frame_value;
    try flushToBase(op_stack, op_count, output_stack, output_count, ast, frame.base_op_count);

    if (frame.saw_star) {
        if (frame.kind != .aggregate_call) return error.UnexpectedToken;
        if (output_count.* != frame.base_output_count) return error.UnexpectedToken;
    } else if (output_count.* > frame.base_output_count) {
        try captureContainerItem(ast, &frame, output_stack, output_count);
    }

    if (frame.kind == .aggregate_call and frame.item_count > 1) return error.UnexpectedToken;
    if (frame.kind == .function_call and isMembershipCall(tokens, source, frame.token_index)) {
        if (frame.item_count != 2) return error.UnexpectedToken;
        const value_arg = frame.first;
        if (value_arg == null_node) return error.UnexpectedToken;
        const list_arg = ast.getNode(value_arg).next;
        if (list_arg == null_node) return error.UnexpectedToken;
        if (ast.getNode(list_arg).next != null_node) return error.UnexpectedToken;
        if (ast.getNode(list_arg).tag != .expr_list) return error.UnexpectedToken;
    }

    const node = switch (frame.kind) {
        .list => ast.addNode(.expr_list, .{ .unary = frame.first }) catch return error.AstFull,
        .function_call => ast.addNodeFull(
            .expr_function_call,
            .{ .unary = frame.first },
            frame.token_index,
            null_node,
        ) catch return error.AstFull,
        .aggregate_call => blk: {
            const arg: NodeIndex = if (frame.saw_star or frame.item_count == 0)
                null_node
            else
                frame.first;
            break :blk ast.addNodeFull(
                .expr_aggregate,
                .{ .unary = arg },
                frame.token_index,
                null_node,
            ) catch return error.AstFull;
        },
    };

    if (output_count.* >= max_output_stack) return error.StackOverflow;
    output_stack[output_count.*] = node;
    output_count.* += 1;

    if (container_count.* == 0) return error.StackUnderflow;
    container_count.* -= 1;
}

fn isMembershipCall(tokens: *const TokenizeResult, source: []const u8, token_index: u16) bool {
    if (source.len == 0) return false;
    if (token_index >= tokens.count) return false;
    if (tokens.tokens[token_index].token_type != .identifier) return false;
    const name = tokens.getText(token_index, source);
    return std.mem.eql(u8, name, "in");
}

fn flushOperators(
    op_stack: *[max_operator_stack]OpEntry,
    op_count: *u16,
    output_stack: *[max_output_stack]NodeIndex,
    output_count: *u16,
    ast: *Ast,
    prec: u8,
) ExprError!void {
    while (op_count.* > 0) {
        const top = op_stack[op_count.* - 1];
        if (top.tok_type == .left_paren) break;
        const top_prec: u8 = if (top.is_unary)
            1
        else
            precedence(top.tok_type);
        if (top_prec > prec) break;
        if (top_prec == prec and top.is_unary) break;
        if (top_prec == prec and isRightAssociative(top.tok_type)) break;
        try popOperator(op_stack, op_count, output_stack, output_count, ast);
    }
}

fn popOperator(
    op_stack: *[max_operator_stack]OpEntry,
    op_count: *u16,
    output_stack: *[max_output_stack]NodeIndex,
    output_count: *u16,
    ast: *Ast,
) ExprError!void {
    if (op_count.* == 0) return error.StackUnderflow;
    op_count.* -= 1;
    const op = op_stack[op_count.*];

    if (op.is_unary) {
        if (output_count.* == 0) return error.StackUnderflow;
        output_count.* -= 1;
        const operand = output_stack[output_count.*];
        const node = ast.addNodeFull(.expr_unary, .{ .unary = operand }, op.tok_index, null_node) catch
            return error.AstFull;
        if (output_count.* >= max_output_stack) return error.StackOverflow;
        output_stack[output_count.*] = node;
        output_count.* += 1;
    } else {
        // Binary operator.
        if (output_count.* < 2) return error.StackUnderflow;
        output_count.* -= 1;
        const rhs = output_stack[output_count.*];
        output_count.* -= 1;
        const lhs = output_stack[output_count.*];
        const node = ast.addNodeFull(
            .expr_binary,
            .{ .binary = .{ .lhs = lhs, .rhs = rhs } },
            op.tok_index,
            null_node,
        ) catch return error.AstFull;
        if (output_count.* >= max_output_stack) return error.StackOverflow;
        output_stack[output_count.*] = node;
        output_count.* += 1;
    }
}

fn isFunctionToken(tok_type: TokenType) bool {
    return switch (tok_type) {
        .fn_lower,
        .fn_upper,
        .fn_trim,
        .fn_length,
        .fn_abs,
        .fn_sqrt,
        .fn_round,
        .fn_coalesce,
        => true,
        else => false,
    };
}

fn isFunctionStart(tokens: *const TokenizeResult, pos: u16) bool {
    if (pos >= tokens.count) return false;
    const tok_type = tokens.tokens[pos].token_type;
    return tok_type == .identifier or isFunctionToken(tok_type);
}

fn isAggregateToken(tok_type: TokenType) bool {
    return switch (tok_type) {
        .agg_count, .agg_sum, .agg_avg, .agg_min, .agg_max => true,
        else => false,
    };
}

fn isLiteral(tok_type: TokenType) bool {
    return switch (tok_type) {
        .integer_literal,
        .float_literal,
        .string_literal,
        .true_literal,
        .false_literal,
        .null_literal,
        .kw_current_timestamp,
        => true,
        else => false,
    };
}

// --- Tests ---

const testing = std.testing;

fn expectBinaryOperator(
    ast: *const Ast,
    tokens: *const TokenizeResult,
    node_index: NodeIndex,
    expected: TokenType,
) !void {
    const node = ast.getNode(node_index);
    try testing.expectEqual(NodeTag.expr_binary, node.tag);
    try testing.expectEqual(expected, tokens.tokens[node.extra].token_type);
}

test "simple literal expression" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("42");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_literal, node.tag);
}

test "binary addition" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("1 + 2");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_binary, node.tag);

    const lhs = ast.getNode(node.data.binary.lhs);
    const rhs = ast.getNode(node.data.binary.rhs);
    try testing.expectEqual(NodeTag.expr_literal, lhs.tag);
    try testing.expectEqual(NodeTag.expr_literal, rhs.tag);
}

test "precedence: multiply before add" {
    var ast = Ast{};
    // 1 + 2 * 3 should parse as 1 + (2 * 3)
    const tokens = tokenizer_mod.tokenize("1 + 2 * 3");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_binary, node.tag);

    // Top node is +, rhs should be *
    const rhs = ast.getNode(node.data.binary.rhs);
    try testing.expectEqual(NodeTag.expr_binary, rhs.tag);
}

test "parentheses override precedence" {
    var ast = Ast{};
    // (1 + 2) * 3 should have * at top with + on lhs
    const tokens = tokenizer_mod.tokenize("(1 + 2) * 3");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_binary, node.tag);

    const lhs = ast.getNode(node.data.binary.lhs);
    try testing.expectEqual(NodeTag.expr_binary, lhs.tag);
}

test "unary bang" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("!true");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_unary, node.tag);

    const operand = ast.getNode(node.data.unary);
    try testing.expectEqual(NodeTag.expr_literal, operand.tag);
}

test "unary minus parses as unary expression" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("-5");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_unary, node.tag);
    const operand = ast.getNode(node.data.unary);
    try testing.expectEqual(NodeTag.expr_literal, operand.tag);
}

test "contextual keyword parses as column reference in expression" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("offset == 1");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_binary, node.tag);
    const lhs = ast.getNode(node.data.binary.lhs);
    try testing.expectEqual(NodeTag.expr_column_ref, lhs.tag);
}

test "comparison operators" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("x == 5");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_binary, node.tag);
}

test "logical &&/||" {
    var ast = Ast{};
    // a == 1 && b == 2 || c == 3
    // && binds tighter than ||, so: (a==1 && b==2) || c==3
    const tokens = tokenizer_mod.tokenize("a == 1 && b == 2 || c == 3");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const root = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_binary, root.tag);
    try testing.expectEqual(TokenType.or_or, tokens.tokens[root.extra].token_type);

    const lhs = ast.getNode(root.data.binary.lhs);
    try testing.expectEqual(NodeTag.expr_binary, lhs.tag);
    try testing.expectEqual(TokenType.and_and, tokens.tokens[lhs.extra].token_type);

    try expectBinaryOperator(&ast, &tokens, lhs.data.binary.lhs, .equal_equal);
    try expectBinaryOperator(&ast, &tokens, lhs.data.binary.rhs, .equal_equal);
    try expectBinaryOperator(&ast, &tokens, root.data.binary.rhs, .equal_equal);
}

test "precedence: unary bang binds tighter than comparison" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("!active == enabled");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const root = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_binary, root.tag);
    try testing.expectEqual(TokenType.equal_equal, tokens.tokens[root.extra].token_type);

    const lhs = ast.getNode(root.data.binary.lhs);
    try testing.expectEqual(NodeTag.expr_unary, lhs.tag);
    try testing.expectEqual(TokenType.bang, tokens.tokens[lhs.extra].token_type);

    const unary_operand = ast.getNode(lhs.data.unary);
    try testing.expectEqual(NodeTag.expr_column_ref, unary_operand.tag);
}

test "parentheses override && and || precedence" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("(a == 1 || b == 2) && c == 3");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const root = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_binary, root.tag);
    try testing.expectEqual(TokenType.and_and, tokens.tokens[root.extra].token_type);

    const lhs = ast.getNode(root.data.binary.lhs);
    try testing.expectEqual(NodeTag.expr_binary, lhs.tag);
    try testing.expectEqual(TokenType.or_or, tokens.tokens[lhs.extra].token_type);

    try expectBinaryOperator(&ast, &tokens, lhs.data.binary.lhs, .equal_equal);
    try expectBinaryOperator(&ast, &tokens, lhs.data.binary.rhs, .equal_equal);
    try expectBinaryOperator(&ast, &tokens, root.data.binary.rhs, .equal_equal);
}

test "function call" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("lower(email)");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_function_call, node.tag);
}

test "aggregate count star" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("count(*)");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_aggregate, node.tag);
    try testing.expectEqual(null_node, node.data.unary); // count(*) has no arg
}

test "aggregate sum" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("sum(amount)");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_aggregate, node.tag);
    try testing.expect(node.data.unary != null_node); // has argument
}

test "list literal" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("[1, 2, 3]");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_list, node.tag);
    // Should have 3 elements linked by next.
    try testing.expectEqual(@as(u16, 3), ast.listLen(node.data.unary));
}

test "object literal with commas" {
    var ast = Ast{};
    const source = "{ total: 1, name: \"alice\" }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = try parseExpression(&ast, &tokens, source, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_object, node.tag);
    try testing.expectEqual(@as(u16, 2), ast.listLen(node.data.unary));
}

test "object literal allows whitespace-separated fields" {
    var ast = Ast{};
    const source = "{ total: 1 name: 2 }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = try parseExpression(&ast, &tokens, source, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_object, node.tag);
    try testing.expectEqual(@as(u16, 2), ast.listLen(node.data.unary));
}

test "column reference" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("email");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_column_ref, node.tag);
}

test "parameter reference" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("$user_id");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_parameter, node.tag);
}

test "nested expression" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("(a + b) * (c - d)");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_binary, node.tag);

    const lhs = ast.getNode(node.data.binary.lhs);
    const rhs = ast.getNode(node.data.binary.rhs);
    try testing.expectEqual(NodeTag.expr_binary, lhs.tag);
    try testing.expectEqual(NodeTag.expr_binary, rhs.tag);
}

test "expression stops at pipe arrow" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("x == 5 |> sort");
    const result = try parseExpression(&ast, &tokens, "", 0);
    // Should stop before |>
    try testing.expectEqual(@as(u16, 3), result.pos); // consumed "x == 5" (tokens 0,1,2), stopped at |> (token 3)
}

test "expression stops at right brace" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("x == 5 }");
    const result = try parseExpression(&ast, &tokens, "", 0);
    try testing.expectEqual(@as(u16, 3), result.pos);
}

test "in function call with list" {
    var ast = Ast{};
    const source = "in(status, [1, 2, 3])";
    const tokens = tokenizer_mod.tokenize(source);
    const result = try parseExpression(&ast, &tokens, source, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_function_call, node.tag);
    try testing.expectEqual(@as(u16, 2), ast.listLen(node.data.unary));
}

test "negated in function call with list" {
    var ast = Ast{};
    const source = "!in(status, [1, 2, 3])";
    const tokens = tokenizer_mod.tokenize(source);
    const result = try parseExpression(&ast, &tokens, source, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_unary, node.tag);
    const operand = ast.getNode(node.data.unary);
    try testing.expectEqual(NodeTag.expr_function_call, operand.tag);
}

test "membership function rejects non-list second argument" {
    var ast = Ast{};
    const source = "in(status, status_list)";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parseExpression(&ast, &tokens, source, 0);
    try testing.expectError(error.UnexpectedToken, result);
}

test "membership function rejects wrong arity" {
    var ast = Ast{};
    const source = "in(status)";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parseExpression(&ast, &tokens, source, 0);
    try testing.expectError(error.UnexpectedToken, result);
}

test "membership function rejects more than two arguments" {
    var ast = Ast{};
    const source = "in(status, [1, 2], 3)";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parseExpression(&ast, &tokens, source, 0);
    try testing.expectError(error.UnexpectedToken, result);
}

test "multi-arg function" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("coalesce(a, b)");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_function_call, node.tag);
    // Should have 2 args linked by next.
    try testing.expectEqual(@as(u16, 2), ast.listLen(node.data.unary));
}

test "nested containers parse iteratively" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("coalesce(lower(a), [1, 2, 3])");
    const result = try parseExpression(&ast, &tokens, "", 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_function_call, node.tag);
    try testing.expectEqual(@as(u16, 2), ast.listLen(node.data.unary));
}
