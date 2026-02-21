//! Pipeline operator parser functions.
const ast_mod = @import("ast.zig");
const tokenizer_mod = @import("tokenizer.zig");
const expression_mod = @import("expression.zig");
const shared = @import("parser_shared.zig");

const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const NodeTag = ast_mod.NodeTag;
const null_node = ast_mod.null_node;
const TokenType = tokenizer_mod.TokenType;
const TokenizeResult = tokenizer_mod.TokenizeResult;

const ParseError = shared.ParseError;
const NodeResult = shared.NodeResult;

const sort_key_desc_mask: u16 = 0x0001;
const sort_key_expr_mask: u16 = 0x8000;

pub fn parseOperator(
    ast: *Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    pos: u16,
) ParseError!NodeResult {
    const tok_type = tokens.tokens[pos].token_type;

    if (tok_type == .kw_where) return parseWhereOp(ast, tokens, source, pos);
    if (tok_type == .kw_sort) return parseSortOp(ast, tokens, source, pos);
    if (tok_type == .kw_limit) return parseSingleExprOp(ast, tokens, source, pos, .op_limit);
    if (tok_type == .kw_offset) return parseSingleExprOp(ast, tokens, source, pos, .op_offset);
    if (tok_type == .kw_group) return parseGroupOp(ast, tokens, pos);
    if (tok_type == .kw_having) return parseSingleExprOp(ast, tokens, source, pos, .op_having);
    if (tok_type == .kw_insert) return parseMutationOp(ast, tokens, source, pos, .op_insert);
    if (tok_type == .kw_update) return parseMutationOp(ast, tokens, source, pos, .op_update);
    if (tok_type == .identifier) {
        const node = try ast.addNode(.op_scope_ref, .{ .token = pos });
        return .{ .node = node, .pos = pos + 1 };
    }
    if (tok_type == .kw_unique) {
        const node = try ast.addNode(.op_unique, .{ .unary = null_node });
        return .{ .node = node, .pos = pos + 1 };
    }
    if (tok_type == .kw_delete) {
        const node = try ast.addNode(.op_delete, .{ .unary = null_node });
        return .{ .node = node, .pos = pos + 1 };
    }
    if (tok_type == .kw_inspect) {
        const node = try ast.addNode(.op_inspect, .{ .unary = null_node });
        return .{ .node = node, .pos = pos + 1 };
    }

    return error.UnexpectedToken;
}

fn parseWhereOp(
    ast: *Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    start_pos: u16,
) ParseError!NodeResult {
    var pos = start_pos + 1;

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .left_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;

    const expr = try expression_mod.parseExpression(ast, tokens, source, pos);
    pos = expr.pos;

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .right_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;

    const node = try ast.addNode(.op_where, .{ .unary = expr.node });
    return .{ .node = node, .pos = pos };
}

fn parseSortOp(
    ast: *Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    start_pos: u16,
) ParseError!NodeResult {
    var pos = start_pos + 1;

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .left_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;

    var first_key: NodeIndex = null_node;
    var last_key: NodeIndex = null_node;

    while (pos < tokens.count and tokens.tokens[pos].token_type != .right_paren) {
        if (first_key != null_node) {
            if (tokens.tokens[pos].token_type != .comma) break;
            pos += 1;
        }

        const key_node = if (isBareSortColumnKey(tokens, pos))
            try parseSortColumnKey(ast, tokens, &pos)
        else
            try parseSortExpressionKey(ast, tokens, source, &pos);

        if (first_key == null_node) {
            first_key = key_node;
            last_key = key_node;
        } else {
            ast.setNext(last_key, key_node);
            last_key = key_node;
        }
    }

    if (pos < tokens.count and tokens.tokens[pos].token_type == .right_paren) pos += 1;

    const node = try ast.addNode(.op_sort, .{ .unary = first_key });
    return .{ .node = node, .pos = pos };
}

fn isBareSortColumnKey(tokens: *const TokenizeResult, pos: u16) bool {
    if (pos >= tokens.count) return false;
    if (!tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type)) return false;
    const next = pos + 1;
    if (next >= tokens.count) return true;
    return switch (tokens.tokens[next].token_type) {
        .kw_asc, .kw_desc, .comma, .right_paren => true,
        else => false,
    };
}

fn parseSortDirection(tokens: *const TokenizeResult, pos: *u16) u16 {
    var direction: u16 = 0;
    if (pos.* < tokens.count and tokens.tokens[pos.*].token_type == .kw_asc) {
        pos.* += 1;
    } else if (pos.* < tokens.count and tokens.tokens[pos.*].token_type == .kw_desc) {
        direction = 1;
        pos.* += 1;
    }
    return direction;
}

fn parseSortColumnKey(
    ast: *Ast,
    tokens: *const TokenizeResult,
    pos: *u16,
) ParseError!NodeIndex {
    if (!tokenizer_mod.isContextualIdentifier(tokens.tokens[pos.*].token_type)) {
        return error.UnexpectedToken;
    }
    const field_tok = pos.*;
    pos.* += 1;
    const direction = parseSortDirection(tokens, pos);
    return ast.addNodeFull(
        .sort_key,
        .{ .token = field_tok },
        direction & sort_key_desc_mask,
        null_node,
    );
}

fn parseSortExpressionKey(
    ast: *Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    pos: *u16,
) ParseError!NodeIndex {
    const expr = try expression_mod.parseExpression(ast, tokens, source, pos.*);
    pos.* = expr.pos;
    const direction = parseSortDirection(tokens, pos);
    return ast.addNodeFull(
        .sort_key,
        .{ .unary = expr.node },
        direction | sort_key_expr_mask,
        null_node,
    );
}

fn parseSingleExprOp(
    ast: *Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    start_pos: u16,
    tag: NodeTag,
) ParseError!NodeResult {
    var pos = start_pos + 1;

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .left_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;

    const expr = try expression_mod.parseExpression(ast, tokens, source, pos);
    pos = expr.pos;

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .right_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;

    const node = try ast.addNode(tag, .{ .unary = expr.node });
    return .{ .node = node, .pos = pos };
}

fn parseGroupOp(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
) ParseError!NodeResult {
    var pos = start_pos + 1;

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .left_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;

    var first_field: NodeIndex = null_node;
    var last_field: NodeIndex = null_node;

    while (pos < tokens.count and tokens.tokens[pos].token_type != .right_paren) {
        if (first_field != null_node) {
            if (tokens.tokens[pos].token_type != .comma) break;
            pos += 1;
        }
        if (!tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type)) return error.UnexpectedToken;
        const field_node = try ast.addNode(.expr_column_ref, .{ .token = pos });
        pos += 1;

        if (first_field == null_node) {
            first_field = field_node;
            last_field = field_node;
        } else {
            ast.setNext(last_field, field_node);
            last_field = field_node;
        }
    }

    if (pos < tokens.count and tokens.tokens[pos].token_type == .right_paren) pos += 1;

    const node = try ast.addNode(.op_group, .{ .unary = first_field });
    return .{ .node = node, .pos = pos };
}

fn parseMutationOp(
    ast: *Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    start_pos: u16,
    tag: NodeTag,
) ParseError!NodeResult {
    var pos = start_pos + 1;

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .left_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;

    var first_assign: NodeIndex = null_node;
    var last_assign: NodeIndex = null_node;

    while (pos < tokens.count and tokens.tokens[pos].token_type != .right_paren) {
        if (first_assign != null_node) {
            if (tokens.tokens[pos].token_type != .comma) break;
            pos += 1;
        }
        if (!tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type)) return error.UnexpectedToken;
        const field_tok = pos;
        pos += 1;

        if (pos >= tokens.count or tokens.tokens[pos].token_type != .equal) {
            return error.UnexpectedToken;
        }
        pos += 1;

        const expr = try expression_mod.parseExpression(ast, tokens, source, pos);
        pos = expr.pos;

        const assign_node = try ast.addNodeFull(
            .assignment,
            .{ .unary = expr.node },
            field_tok,
            null_node,
        );

        if (first_assign == null_node) {
            first_assign = assign_node;
            last_assign = assign_node;
        } else {
            ast.setNext(last_assign, assign_node);
            last_assign = assign_node;
        }
    }

    if (pos < tokens.count and tokens.tokens[pos].token_type == .right_paren) pos += 1;

    const node = try ast.addNode(tag, .{ .unary = first_assign });
    return .{ .node = node, .pos = pos };
}

pub fn isAggOrFn(tok_type: TokenType) bool {
    return switch (tok_type) {
        .agg_count,
        .agg_sum,
        .agg_avg,
        .agg_min,
        .agg_max,
        .fn_now,
        .fn_lower,
        .fn_upper,
        .fn_trim,
        .fn_length,
        .fn_coalesce,
        => true,
        else => false,
    };
}
