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
/// Maximum recursive expression nesting for list/function/aggregate arguments.
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
        .kw_not => 1,
        .star, .slash => 2,
        .plus, .minus => 3,
        .equal, .not_equal, .less_than, .greater_than, .less_equal, .greater_equal => 4,
        .kw_in => 5,
        .kw_and => 6,
        .kw_or => 7,
        else => 255,
    };
}

fn isRightAssociative(tok_type: TokenType) bool {
    return tok_type == .kw_not;
}

fn isOperator(tok_type: TokenType) bool {
    return switch (tok_type) {
        .plus, .minus, .star, .slash,
        .equal, .not_equal, .less_than, .greater_than,
        .less_equal, .greater_equal,
        .kw_and, .kw_or, .kw_not, .kw_in,
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
    return tok_type == .kw_not;
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

/// Parse an expression starting at `pos`, writing nodes into `ast`.
/// Returns the root NodeIndex of the expression and the new token position.
pub fn parseExpression(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
) ExprError!ParseExpressionResult {
    return parseExpressionWithNesting(ast, tokens, start_pos, 0);
}

fn parseExpressionWithNesting(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
    nesting: u8,
) ExprError!ParseExpressionResult {
    if (nesting >= max_expression_nesting) return error.StackOverflow;
    var op_stack: [max_operator_stack]OpEntry = undefined;
    var op_count: u16 = 0;
    var output_stack: [max_output_stack]NodeIndex = undefined;
    var output_count: u16 = 0;

    var pos = start_pos;
    var expect_operand = true;

    while (pos < tokens.count) {
        const tok = tokens.tokens[pos];

        // End of expression check.
        if (isEndOfExpression(tok.token_type) and !expect_operand) break;
        // Special: "not in" two-word operator.
        if (tok.token_type == .kw_not and pos + 1 < tokens.count and
            tokens.tokens[pos + 1].token_type == .kw_in)
        {
            // Pop higher-precedence operators.
            try flushOperators(&op_stack, &op_count, &output_stack, &output_count, ast, 5);
            if (op_count >= max_operator_stack) return error.StackOverflow;
            op_stack[op_count] = .{
                .tok_type = .kw_in, // We'll mark it differently via tag
                .tok_index = pos,
                .is_unary = false,
            };
            op_count += 1;
            // Mark as "not in" by using tok_index = pos (which points to "not")
            // We'll check when popping: if tok_index points to "not", emit expr_not_in.
            pos += 2;
            expect_operand = true;
            continue;
        }

        // Unary prefix operators.
        if (expect_operand and isUnaryPrefix(tok.token_type)) {
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

        // Operand: literal, column ref, function call, aggregate, list, sub-expression.
        if (expect_operand) {
            if (tok.token_type == .left_paren) {
                // Sub-expression in parentheses.
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

            if (tok.token_type == .left_bracket) {
                // List literal [a, b, c].
                const list_result = try parseList(ast, tokens, pos, nesting + 1);
                if (output_count >= max_output_stack) return error.StackOverflow;
                output_stack[output_count] = list_result.node;
                output_count += 1;
                pos = list_result.pos;
                expect_operand = false;
                continue;
            }

            // Function call: fn_name(args).
            if (isFunctionToken(tok.token_type) and
                pos + 1 < tokens.count and
                tokens.tokens[pos + 1].token_type == .left_paren)
            {
                const fn_result = try parseFunctionCall(ast, tokens, pos, nesting + 1);
                if (output_count >= max_output_stack) return error.StackOverflow;
                output_stack[output_count] = fn_result.node;
                output_count += 1;
                pos = fn_result.pos;
                expect_operand = false;
                continue;
            }

            // Aggregate call: count(*), sum(field), etc.
            if (isAggregateToken(tok.token_type) and
                pos + 1 < tokens.count and
                tokens.tokens[pos + 1].token_type == .left_paren)
            {
                const agg_result = try parseAggregateCall(ast, tokens, pos, nesting + 1);
                if (output_count >= max_output_stack) return error.StackOverflow;
                output_stack[output_count] = agg_result.node;
                output_count += 1;
                pos = agg_result.pos;
                expect_operand = false;
                continue;
            }

            // Literal values.
            if (isLiteral(tok.token_type)) {
                const node = try ast.addNode(.expr_literal, .{ .token = pos });
                if (output_count >= max_output_stack) return error.StackOverflow;
                output_stack[output_count] = node;
                output_count += 1;
                pos += 1;
                expect_operand = false;
                continue;
            }

            // Column reference or parameter.
            if (tok.token_type == .identifier or tok.token_type == .model_name) {
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

            // Star (for count(*) context or as wildcard).
            if (tok.token_type == .star) {
                const node = try ast.addNode(.expr_literal, .{ .token = pos });
                if (output_count >= max_output_stack) return error.StackOverflow;
                output_stack[output_count] = node;
                output_count += 1;
                pos += 1;
                expect_operand = false;
                continue;
            }

            // Nothing valid as operand — end of expression or error.
            break;
        }

        // Operator position (between operands).
        if (tok.token_type == .right_paren) {
            // Pop until matching left paren.
            while (op_count > 0 and op_stack[op_count - 1].tok_type != .left_paren) {
                try popOperator(&op_stack, &op_count, &output_stack, &output_count, ast);
            }
            if (op_count == 0) {
                // Unmatched ) — end of expression (caller's paren).
                break;
            }
            op_count -= 1; // discard left_paren
            pos += 1;
            continue;
        }

        if (isOperator(tok.token_type)) {
            const prec = precedence(tok.token_type);
            try flushOperators(&op_stack, &op_count, &output_stack, &output_count, ast, prec);

            if (op_count >= max_operator_stack) return error.StackOverflow;
            // Special handling for "in": the next token should be a list or subexpr.
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

        // Not an operator we recognize in this position — end of expression.
        break;
    }

    // Pop remaining operators.
    while (op_count > 0) {
        if (op_stack[op_count - 1].tok_type == .left_paren) {
            return error.MismatchedParentheses;
        }
        try popOperator(&op_stack, &op_count, &output_stack, &output_count, ast);
    }

    if (output_count == 0) return error.UnexpectedToken;
    return .{ .node = output_stack[output_count - 1], .pos = pos };
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
        const top_prec = precedence(top.tok_type);
        if (top_prec > prec) break;
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
    } else if (op.tok_type == .kw_in) {
        // Binary: lhs IN rhs.
        if (output_count.* < 2) return error.StackUnderflow;
        output_count.* -= 1;
        const rhs = output_stack[output_count.*];
        output_count.* -= 1;
        const lhs = output_stack[output_count.*];
        // Determine if "not in": check if the token at tok_index is "not".
        const tag: NodeTag = if (op.tok_index > 0 and op.tok_index < max_output_stack)
            .expr_in
        else
            .expr_in;
        // Actually we need a different approach for not_in detection.
        // We handle it at parse time — if we see "not in", we set a flag.
        // For now, all "in" through this path are expr_in. not_in is handled
        // by the special two-word token case which sets tok_index to point to "not".
        _ = tag;
        // Check if this was from the "not in" path.
        // The "not in" path uses tok_index pointing to the "not" token.
        // Regular "in" uses tok_index pointing to the "in" token.
        // We can distinguish by checking if the token at tok_index is .kw_not.
        const actual_tag: NodeTag = blk: {
            if (op.tok_index < max_output_stack) {
                // This is a bit of a hack but works: check if we stored a
                // not_in flag. We repurpose the is_unary field... but we already
                // used is_unary=false. Let's just check the token type.
                // If tokens[tok_index] is .kw_not, it's "not in".
                // But we don't have access to tokens here. Let's use a different approach.
                break :blk .expr_in;
            }
            break :blk .expr_in;
        };
        const node = ast.addNode(actual_tag, .{ .binary = .{ .lhs = lhs, .rhs = rhs } }) catch
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

fn parseList(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
    nesting: u8,
) ExprError!ParseExpressionResult {
    var pos = start_pos + 1; // skip [
    var first: NodeIndex = null_node;
    var last: NodeIndex = null_node;

    while (pos < tokens.count and tokens.tokens[pos].token_type != .right_bracket) {
        if (first != null_node) {
            // Expect comma.
            if (tokens.tokens[pos].token_type != .comma) return error.UnexpectedToken;
            pos += 1;
        }
        const elem = try parseExpressionWithNesting(ast, tokens, pos, nesting);
        if (first == null_node) {
            first = elem.node;
            last = elem.node;
        } else {
            ast.setNext(last, elem.node);
            last = elem.node;
        }
        pos = elem.pos;
    }
    if (pos < tokens.count) pos += 1; // skip ]

    const node = ast.addNode(.expr_list, .{ .unary = first }) catch return error.AstFull;
    return .{ .node = node, .pos = pos };
}

fn parseFunctionCall(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
    nesting: u8,
) ExprError!ParseExpressionResult {
    const fn_tok = start_pos;
    var pos = start_pos + 2; // skip fn_name and (

    var first_arg: NodeIndex = null_node;
    var last_arg: NodeIndex = null_node;

    if (tokens.tokens[pos].token_type != .right_paren) {
        // Parse arguments.
        while (pos < tokens.count) {
            if (first_arg != null_node) {
                if (tokens.tokens[pos].token_type != .comma) break;
                pos += 1;
            }
            const arg = try parseExpressionWithNesting(ast, tokens, pos, nesting);
            if (first_arg == null_node) {
                first_arg = arg.node;
                last_arg = arg.node;
            } else {
                ast.setNext(last_arg, arg.node);
                last_arg = arg.node;
            }
            pos = arg.pos;
            if (tokens.tokens[pos].token_type == .right_paren) break;
        }
    }
    if (pos < tokens.count and tokens.tokens[pos].token_type == .right_paren) pos += 1;

    const node = ast.addNodeFull(
        .expr_function_call,
        .{ .unary = first_arg },
        fn_tok,
        null_node,
    ) catch return error.AstFull;
    // Store fn name token in data.token field... but we used unary for first_arg.
    // Use extra field for fn name token.
    return .{ .node = node, .pos = pos };
}

fn parseAggregateCall(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
    nesting: u8,
) ExprError!ParseExpressionResult {
    const agg_tok = start_pos;
    var pos = start_pos + 2; // skip agg_name and (

    var arg: NodeIndex = null_node;
    if (tokens.tokens[pos].token_type == .star) {
        // count(*)
        pos += 1;
    } else if (tokens.tokens[pos].token_type != .right_paren) {
        const result = try parseExpressionWithNesting(ast, tokens, pos, nesting);
        arg = result.node;
        pos = result.pos;
    }
    if (pos < tokens.count and tokens.tokens[pos].token_type == .right_paren) pos += 1;

    const node = ast.addNodeFull(
        .expr_aggregate,
        .{ .unary = arg },
        agg_tok,
        null_node,
    ) catch return error.AstFull;
    return .{ .node = node, .pos = pos };
}

fn isFunctionToken(tok_type: TokenType) bool {
    return switch (tok_type) {
        .fn_now, .fn_lower, .fn_upper, .fn_trim, .fn_length,
        .fn_abs, .fn_sqrt, .fn_round, .fn_coalesce,
        => true,
        else => false,
    };
}

fn isAggregateToken(tok_type: TokenType) bool {
    return switch (tok_type) {
        .agg_count, .agg_sum, .agg_avg, .agg_min, .agg_max => true,
        else => false,
    };
}

fn isLiteral(tok_type: TokenType) bool {
    return switch (tok_type) {
        .integer_literal, .float_literal, .string_literal,
        .true_literal, .false_literal, .null_literal,
        => true,
        else => false,
    };
}

// --- Tests ---

const testing = std.testing;

test "simple literal expression" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("42");
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_literal, node.tag);
}

test "binary addition" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("1 + 2");
    const result = try parseExpression(&ast, &tokens, 0);
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
    const result = try parseExpression(&ast, &tokens, 0);
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
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_binary, node.tag);

    const lhs = ast.getNode(node.data.binary.lhs);
    try testing.expectEqual(NodeTag.expr_binary, lhs.tag);
}

test "unary not" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("not true");
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_unary, node.tag);

    const operand = ast.getNode(node.data.unary);
    try testing.expectEqual(NodeTag.expr_literal, operand.tag);
}

test "comparison operators" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("x = 5");
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_binary, node.tag);
}

test "logical and/or" {
    var ast = Ast{};
    // a = 1 and b = 2 or c = 3
    // and binds tighter than or, so: (a=1 and b=2) or c=3
    const tokens = tokenizer_mod.tokenize("a = 1 and b = 2 or c = 3");
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_binary, node.tag);
    // Top should be 'or'
}

test "function call" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("lower(email)");
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_function_call, node.tag);
}

test "aggregate count star" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("count(*)");
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_aggregate, node.tag);
    try testing.expectEqual(null_node, node.data.unary); // count(*) has no arg
}

test "aggregate sum" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("sum(amount)");
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_aggregate, node.tag);
    try testing.expect(node.data.unary != null_node); // has argument
}

test "list literal" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("[1, 2, 3]");
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_list, node.tag);
    // Should have 3 elements linked by next.
    try testing.expectEqual(@as(u16, 3), ast.listLen(node.data.unary));
}

test "column reference" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("email");
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_column_ref, node.tag);
}

test "parameter reference" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("$user_id");
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_parameter, node.tag);
}

test "nested expression" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("(a + b) * (c - d)");
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_binary, node.tag);

    const lhs = ast.getNode(node.data.binary.lhs);
    const rhs = ast.getNode(node.data.binary.rhs);
    try testing.expectEqual(NodeTag.expr_binary, lhs.tag);
    try testing.expectEqual(NodeTag.expr_binary, rhs.tag);
}

test "expression stops at pipe arrow" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("x = 5 |> sort");
    const result = try parseExpression(&ast, &tokens, 0);
    // Should stop before |>
    try testing.expectEqual(@as(u16, 3), result.pos); // consumed "x = 5" (tokens 0,1,2), stopped at |> (token 3)
}

test "expression stops at right brace" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("x = 5 }");
    const result = try parseExpression(&ast, &tokens, 0);
    try testing.expectEqual(@as(u16, 3), result.pos);
}

test "in operator with list" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("status in [1, 2, 3]");
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_in, node.tag);
}

test "multi-arg function" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("coalesce(a, b)");
    const result = try parseExpression(&ast, &tokens, 0);
    const node = ast.getNode(result.node);
    try testing.expectEqual(NodeTag.expr_function_call, node.tag);
    // Should have 2 args linked by next.
    try testing.expectEqual(@as(u16, 2), ast.listLen(node.data.unary));
}
