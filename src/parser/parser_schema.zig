//! Schema declaration parser functions.
const std = @import("std");
const ast_mod = @import("ast.zig");
const tokenizer_mod = @import("tokenizer.zig");
const expression_mod = @import("expression.zig");
const parser_ops = @import("parser_ops.zig");
const shared = @import("parser_shared.zig");

const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const NodeTag = ast_mod.NodeTag;
const null_node = ast_mod.null_node;
const TokenType = tokenizer_mod.TokenType;
const TokenizeResult = tokenizer_mod.TokenizeResult;

const ParseError = shared.ParseError;
const NodeResult = shared.NodeResult;

const null_token_index: u16 = std.math.maxInt(u16);

pub fn isSchemaKeyword(tok_type: TokenType) bool {
    return switch (tok_type) {
        .kw_field,
        .kw_has_many,
        .kw_has_one,
        .kw_belongs_to,
        .kw_index,
        .kw_scope,
        .kw_reference,
        => true,
        else => false,
    };
}

pub fn parseSchemaDefinition(
    ast: *Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    start_pos: u16,
) ParseError!NodeResult {
    const model_tok = start_pos;
    var pos = start_pos + 1;

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .left_brace) {
        return error.UnexpectedToken;
    }
    pos += 1;

    var first_member: NodeIndex = null_node;
    var last_member: NodeIndex = null_node;

    while (pos < tokens.count and tokens.tokens[pos].token_type != .right_brace) {
        const member = try parseSchemaMember(ast, tokens, source, pos);
        if (first_member == null_node) {
            first_member = member.node;
            last_member = member.node;
        } else {
            ast.setNext(last_member, member.node);
            last_member = member.node;
        }
        pos = member.pos;
    }

    if (pos < tokens.count and tokens.tokens[pos].token_type == .right_brace) pos += 1;

    const node = try ast.addNodeFull(
        .schema_def,
        .{ .unary = first_member },
        model_tok,
        null_node,
    );
    return .{ .node = node, .pos = pos };
}

fn parseSchemaMember(
    ast: *Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    start_pos: u16,
) ParseError!NodeResult {
    const tok_type = tokens.tokens[start_pos].token_type;
    var pos = start_pos + 1;

    if (tok_type == .kw_field) return parseSchemaField(ast, tokens, pos - 1);
    if (tok_type == .kw_has_many) return parseSchemaRelation(ast, tokens, pos - 1, .schema_has_many);
    if (tok_type == .kw_has_one) return parseSchemaRelation(ast, tokens, pos - 1, .schema_has_one);
    if (tok_type == .kw_belongs_to) return parseSchemaRelation(ast, tokens, pos - 1, .schema_belongs_to);
    if (tok_type == .kw_index) return parseSchemaIndex(ast, tokens, pos - 1, .schema_index);
    if (tok_type == .kw_reference) return parseSchemaReference(ast, tokens, pos - 1);

    if (tok_type == .kw_scope) {
        if (pos >= tokens.count) return error.UnexpectedToken;
        const scope_name_tok = pos;
        if (!tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type)) return error.UnexpectedToken;
        pos += 1;

        var first_op: NodeIndex = null_node;
        var last_op: NodeIndex = null_node;
        while (pos < tokens.count and tokens.tokens[pos].token_type == .pipe_arrow) {
            pos += 1;
            const op = try parser_ops.parseOperator(ast, tokens, source, pos);
            if (first_op == null_node) {
                first_op = op.node;
                last_op = op.node;
            } else {
                ast.setNext(last_op, op.node);
                last_op = op.node;
            }
            pos = op.pos;
        }
        const node = try ast.addNodeFull(
            .schema_scope,
            .{ .unary = first_op },
            scope_name_tok,
            null_node,
        );
        return .{ .node = node, .pos = pos };
    }

    return error.UnexpectedToken;
}

fn parseSchemaField(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
) ParseError!NodeResult {
    var pos = start_pos + 1; // skip 'field'
    if (pos >= tokens.count or tokens.tokens[pos].token_type != .left_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;

    if (pos >= tokens.count) return error.UnexpectedToken;
    const name_tok = pos;
    if (!tokenizer_mod.isContextualIdentifier(tokens.tokens[name_tok].token_type)) {
        return error.UnexpectedToken;
    }
    pos += 1;
    if (pos >= tokens.count or tokens.tokens[pos].token_type != .comma) {
        return error.UnexpectedToken;
    }
    pos += 1;

    if (pos >= tokens.count) return error.UnexpectedToken;
    const type_tok = pos;
    pos += 1;

    while (pos < tokens.count) {
        const ct = tokens.tokens[pos].token_type;
        if (ct == .comma) {
            pos += 1;
            continue;
        }
        if (ct == .kw_primary_key or ct == .kw_not_null or ct == .kw_nullable) {
            pos += 1;
            continue;
        }
        if (ct == .kw_default) {
            pos += 1;
            if (pos < tokens.count and tokens.tokens[pos].token_type == .comma) {
                pos += 1;
            }
            if (pos < tokens.count) pos += 1;
            continue;
        }
        break;
    }

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .right_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;

    const node = try ast.addNodeFull(
        .schema_field,
        .{ .token = name_tok },
        type_tok,
        null_node,
    );
    return .{ .node = node, .pos = pos };
}

fn parseSchemaRelation(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
    tag: NodeTag,
) ParseError!NodeResult {
    const pos = start_pos + 1;
    if (pos >= tokens.count) return error.UnexpectedToken;
    const node = try ast.addNode(tag, .{ .token = pos });
    return .{ .node = node, .pos = pos + 1 };
}

fn parseSchemaReference(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
) ParseError!NodeResult {
    var pos = start_pos + 1;
    if (pos >= tokens.count or tokens.tokens[pos].token_type != .left_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;

    var first_payload: NodeIndex = null_node;

    const alias_tok = pos;
    if (!tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type)) return error.UnexpectedToken;
    pos += 1;
    if (tokens.tokens[pos].token_type != .comma) return error.UnexpectedToken;
    pos += 1;

    const local_field_tok = pos;
    if (!tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type)) return error.UnexpectedToken;
    pos += 1;
    if (tokens.tokens[pos].token_type != .comma) return error.UnexpectedToken;
    pos += 1;

    const target_model_tok = pos;
    if (tokens.tokens[pos].token_type != .model_name and
        !tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type))
    {
        return error.UnexpectedToken;
    }
    pos += 1;
    if (tokens.tokens[pos].token_type != .dot) return error.UnexpectedToken;
    pos += 1;

    const target_field_tok = pos;
    if (!tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type)) return error.UnexpectedToken;
    pos += 1;
    if (tokens.tokens[pos].token_type != .comma) return error.UnexpectedToken;
    pos += 1;

    const ri_mode_tok = pos;
    if (tokens.tokens[pos].token_type == .kw_without_referential_integrity) {
        pos += 1;
    } else if (tokens.tokens[pos].token_type == .kw_with_referential_integrity) {
        pos += 1;
        if (tokens.tokens[pos].token_type != .left_paren) return error.UnexpectedToken;
        pos += 1;
        const on_delete_tok = pos;
        if (!isOnDeleteAction(tokens.tokens[pos].token_type)) return error.UnexpectedToken;
        pos += 1;
        if (tokens.tokens[pos].token_type != .comma) return error.UnexpectedToken;
        pos += 1;
        const on_update_tok = pos;
        if (!isOnUpdateAction(tokens.tokens[pos].token_type)) return error.UnexpectedToken;
        pos += 1;
        if (tokens.tokens[pos].token_type != .right_paren) return error.UnexpectedToken;
        pos += 1;

        const on_delete_node = try ast.addNode(.expr_literal, .{ .token = on_delete_tok });
        const on_update_node = try ast.addNode(.expr_literal, .{ .token = on_update_tok });
        ast.setNext(on_delete_node, on_update_node);
        first_payload = on_delete_node;
    } else {
        return error.UnexpectedToken;
    }

    if (tokens.tokens[pos].token_type != .right_paren) return error.UnexpectedToken;
    pos += 1;

    const alias_node = try ast.addNode(.expr_literal, .{ .token = alias_tok });
    const local_node = try ast.addNode(.expr_literal, .{ .token = local_field_tok });
    const target_model_node = try ast.addNode(.expr_literal, .{ .token = target_model_tok });
    const target_field_node = try ast.addNode(.expr_literal, .{ .token = target_field_tok });
    const ri_mode_node = try ast.addNode(.expr_literal, .{ .token = ri_mode_tok });

    ast.setNext(alias_node, local_node);
    ast.setNext(local_node, target_model_node);
    ast.setNext(target_model_node, target_field_node);
    ast.setNext(target_field_node, ri_mode_node);
    if (first_payload != null_node) {
        ast.setNext(ri_mode_node, first_payload);
    }

    const node = try ast.addNode(.schema_reference, .{ .unary = alias_node });
    return .{ .node = node, .pos = pos };
}

fn isOnDeleteAction(tok_type: TokenType) bool {
    return switch (tok_type) {
        .kw_on_delete_restrict,
        .kw_on_delete_cascade,
        .kw_on_delete_set_null,
        .kw_on_delete_set_default,
        => true,
        else => false,
    };
}

fn isOnUpdateAction(tok_type: TokenType) bool {
    return switch (tok_type) {
        .kw_on_update_restrict,
        .kw_on_update_cascade,
        .kw_on_update_set_null,
        .kw_on_update_set_default,
        => true,
        else => false,
    };
}

fn parseSchemaIndex(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
    tag: NodeTag,
) ParseError!NodeResult {
    var pos = start_pos + 1;
    var index_name_tok: u16 = null_token_index;
    var index_tag = tag;

    var first_col: NodeIndex = null_node;
    var last_col: NodeIndex = null_node;

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .left_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;
    if (pos >= tokens.count or !tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type)) {
        return error.UnexpectedToken;
    }
    index_name_tok = pos;
    pos += 1;
    if (pos >= tokens.count or tokens.tokens[pos].token_type != .comma) {
        return error.UnexpectedToken;
    }
    pos += 1;
    if (pos >= tokens.count or tokens.tokens[pos].token_type != .left_bracket) {
        return error.UnexpectedToken;
    }
    pos += 1;

    var expect_column = true;
    while (pos < tokens.count and tokens.tokens[pos].token_type != .right_bracket) {
        const tt = tokens.tokens[pos].token_type;
        if (expect_column) {
            if (!tokenizer_mod.isContextualIdentifier(tt)) return error.UnexpectedToken;
            const col_node = try ast.addNode(.expr_column_ref, .{ .token = pos });
            pos += 1;
            if (first_col == null_node) {
                first_col = col_node;
                last_col = col_node;
            } else {
                ast.setNext(last_col, col_node);
                last_col = col_node;
            }
            expect_column = false;
            continue;
        }
        if (tt == .comma) {
            pos += 1;
            expect_column = true;
            continue;
        }
        return error.UnexpectedToken;
    }

    if (first_col == null_node) return error.UnexpectedToken;
    if (expect_column) return error.UnexpectedToken;
    if (pos >= tokens.count or tokens.tokens[pos].token_type != .right_bracket) {
        return error.UnexpectedToken;
    }
    pos += 1;

    if (pos < tokens.count and tokens.tokens[pos].token_type == .comma) {
        pos += 1;
        if (pos >= tokens.count or tokens.tokens[pos].token_type != .kw_unique) {
            return error.UnexpectedToken;
        }
        if (tag == .schema_index) index_tag = .schema_unique_index;
        pos += 1;
    }

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .right_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;

    const node = try ast.addNodeFull(
        index_tag,
        .{ .unary = first_col },
        index_name_tok,
        null_node,
    );
    return .{ .node = node, .pos = pos };
}
