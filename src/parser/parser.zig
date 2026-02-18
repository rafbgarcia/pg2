const std = @import("std");
const ast_mod = @import("ast.zig");
const tokenizer_mod = @import("tokenizer.zig");
const expression_mod = @import("expression.zig");

const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const NodeTag = ast_mod.NodeTag;
const null_node = ast_mod.null_node;
const TokenType = tokenizer_mod.TokenType;
const TokenizeResult = tokenizer_mod.TokenizeResult;

/// Maximum nesting depth for selection sets.
const max_nesting_depth = 16;
const sort_key_desc_mask: u16 = 0x0001;
const sort_key_expr_mask: u16 = 0x8000;

pub const ParseError = error{
    AstFull,
    UnexpectedToken,
    NestingTooDeep,
    StackOverflow,
    StackUnderflow,
    MismatchedParentheses,
};

/// Returned by all internal parse functions: a node index and the new token position.
const NodeResult = struct {
    node: NodeIndex,
    pos: u16,
};

/// Error information for diagnostics.
pub const ParseResult = struct {
    ast: Ast,
    has_error: bool,
    error_line: u16,
    error_message: [128]u8,

    pub fn getError(self: *const ParseResult) ?[]const u8 {
        if (!self.has_error) return null;
        const len = std.mem.indexOfScalar(u8, &self.error_message, 0) orelse
            self.error_message.len;
        return self.error_message[0..len];
    }
};

/// Parse a tokenized source into an AST.
pub fn parse(tokens: *const TokenizeResult, source: []const u8) ParseResult {
    _ = source;
    var result = ParseResult{
        .ast = Ast{},
        .has_error = false,
        .error_line = 0,
        .error_message = std.mem.zeroes([128]u8),
    };

    var pos: u16 = 0;
    var first_stmt: NodeIndex = null_node;
    var last_stmt: NodeIndex = null_node;

    while (pos < tokens.count and tokens.tokens[pos].token_type != .end_of_input) {
        const stmt = parseStatement(&result.ast, tokens, pos) catch |err| {
            setParseError(&result, tokens, pos, err);
            return result;
        };
        if (first_stmt == null_node) {
            first_stmt = stmt.node;
            last_stmt = stmt.node;
        } else {
            result.ast.setNext(last_stmt, stmt.node);
            last_stmt = stmt.node;
        }
        pos = stmt.pos;
    }

    if (first_stmt != null_node) {
        result.ast.root = result.ast.addNode(.root, .{ .unary = first_stmt }) catch {
            result.has_error = true;
            return result;
        };
    }

    return result;
}

fn parseStatement(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
) ParseError!NodeResult {
    const tok = tokens.tokens[start_pos];
    if (tok.token_type == .kw_let) return parseLetBinding(ast, tokens, start_pos);
    if (tok.token_type == .model_name) return parseModelStatement(ast, tokens, start_pos);
    return error.UnexpectedToken;
}

fn parseLetBinding(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
) ParseError!NodeResult {
    var pos = start_pos + 1; // skip 'let'

    if (pos >= tokens.count) return error.UnexpectedToken;
    const name_tok = pos;
    if (tokens.tokens[pos].token_type != .identifier) return error.UnexpectedToken;
    pos += 1;

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .equal) {
        return error.UnexpectedToken;
    }
    pos += 1;

    if (pos < tokens.count and tokens.tokens[pos].token_type == .model_name) {
        const pipeline = try parsePipeline(ast, tokens, pos);
        const node = try ast.addNodeFull(
            .let_binding,
            .{ .unary = pipeline.node },
            name_tok,
            null_node,
        );
        return .{ .node = node, .pos = pipeline.pos };
    }

    const expr = try expression_mod.parseExpression(ast, tokens, pos);
    const node = try ast.addNodeFull(
        .let_binding,
        .{ .unary = expr.node },
        name_tok,
        null_node,
    );
    return .{ .node = node, .pos = expr.pos };
}

fn parseModelStatement(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
) ParseError!NodeResult {
    var lookahead = start_pos + 1;

    // Skip optional dot access for lookahead.
    if (lookahead < tokens.count and tokens.tokens[lookahead].token_type == .dot) {
        lookahead += 1;
        if (lookahead < tokens.count) lookahead += 1;
    }

    // Check if schema definition.
    if (lookahead < tokens.count and tokens.tokens[lookahead].token_type == .left_brace) {
        if (lookahead + 1 < tokens.count and
            isSchemaKeyword(tokens.tokens[lookahead + 1].token_type))
        {
            return parseSchemaDefinition(ast, tokens, start_pos);
        }
    }

    return parsePipeline(ast, tokens, start_pos);
}

fn isSchemaKeyword(tok_type: TokenType) bool {
    return switch (tok_type) {
        .kw_field,
        .kw_has_many,
        .kw_has_one,
        .kw_belongs_to,
        .kw_index,
        .kw_unique_index,
        .kw_scope,
        => true,
        else => false,
    };
}

fn parsePipeline(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
) ParseError!NodeResult {
    var pos = start_pos;

    const source_tok = pos;
    if (tokens.tokens[pos].token_type != .model_name and
        tokens.tokens[pos].token_type != .identifier)
    {
        return error.UnexpectedToken;
    }
    pos += 1;

    const source_node = try ast.addNode(.pipe_source, .{ .token = source_tok });

    // Check for dot access (Model.index_name).
    if (pos < tokens.count and tokens.tokens[pos].token_type == .dot) {
        pos += 1;
        if (pos < tokens.count and
            (tokens.tokens[pos].token_type == .identifier or
                tokens.tokens[pos].token_type == .model_name))
        {
            ast.getNodeMut(source_node).extra = pos;
            pos += 1;
        }
    }

    // Parse pipeline operators (|> op ...).
    var first_op: NodeIndex = null_node;
    var last_op: NodeIndex = null_node;

    while (pos < tokens.count) {
        if (tokens.tokens[pos].token_type == .pipe_arrow) {
            pos += 1;
            const op = try parseOperator(ast, tokens, pos);
            if (first_op == null_node) {
                first_op = op.node;
                last_op = op.node;
            } else {
                ast.setNext(last_op, op.node);
                last_op = op.node;
            }
            pos = op.pos;
        } else if (tokens.tokens[pos].token_type == .identifier and
            first_op == null_node)
        {
            // Scope reference before any |> operator.
            const scope_node = try ast.addNode(.op_scope_ref, .{ .token = pos });
            first_op = scope_node;
            last_op = scope_node;
            pos += 1;
        } else {
            break;
        }
    }

    // Parse optional selection set.
    var selection: NodeIndex = null_node;
    if (pos < tokens.count and tokens.tokens[pos].token_type == .left_brace) {
        const sel = try parseSelectionSet(ast, tokens, pos, 0);
        selection = sel.node;
        pos = sel.pos;
    }

    const pipeline = try ast.addNode(.pipeline, .{
        .binary = .{ .lhs = source_node, .rhs = first_op },
    });
    if (selection != null_node) {
        ast.getNodeMut(pipeline).extra = selection;
    }

    return .{ .node = pipeline, .pos = pos };
}

fn parseOperator(
    ast: *Ast,
    tokens: *const TokenizeResult,
    pos: u16,
) ParseError!NodeResult {
    const tok_type = tokens.tokens[pos].token_type;

    if (tok_type == .kw_where) return parseWhereOp(ast, tokens, pos);
    if (tok_type == .kw_sort) return parseSortOp(ast, tokens, pos);
    if (tok_type == .kw_limit) return parseSingleExprOp(ast, tokens, pos, .op_limit);
    if (tok_type == .kw_offset) return parseSingleExprOp(ast, tokens, pos, .op_offset);
    if (tok_type == .kw_group) return parseGroupOp(ast, tokens, pos);
    if (tok_type == .kw_insert) return parseMutationOp(ast, tokens, pos, .op_insert);
    if (tok_type == .kw_update) return parseMutationOp(ast, tokens, pos, .op_update);
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
    start_pos: u16,
) ParseError!NodeResult {
    var pos = start_pos + 1;

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .left_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;

    const expr = try expression_mod.parseExpression(ast, tokens, pos);
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

        // Could be a simple identifier or an aggregate expression.
        if (isAggOrFn(tokens.tokens[pos].token_type)) {
            const expr = try expression_mod.parseExpression(ast, tokens, pos);
            pos = expr.pos;
            var direction: u16 = 0;
            if (pos < tokens.count and tokens.tokens[pos].token_type == .kw_desc) {
                direction = 1;
                pos += 1;
            } else if (pos < tokens.count and tokens.tokens[pos].token_type == .kw_asc) {
                pos += 1;
            }
            const key_node = try ast.addNodeFull(
                .sort_key,
                .{ .unary = expr.node },
                direction | sort_key_expr_mask,
                null_node,
            );
            if (first_key == null_node) {
                first_key = key_node;
                last_key = key_node;
            } else {
                ast.setNext(last_key, key_node);
                last_key = key_node;
            }
            continue;
        }

        if (tokens.tokens[pos].token_type != .identifier) return error.UnexpectedToken;
        const field_tok = pos;
        pos += 1;

        var direction: u16 = 0;
        if (pos < tokens.count and tokens.tokens[pos].token_type == .kw_asc) {
            pos += 1;
        } else if (pos < tokens.count and tokens.tokens[pos].token_type == .kw_desc) {
            direction = 1;
            pos += 1;
        }

        const key_node = try ast.addNodeFull(
            .sort_key,
            .{ .token = field_tok },
            direction & sort_key_desc_mask,
            null_node,
        );
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

fn parseSingleExprOp(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
    tag: NodeTag,
) ParseError!NodeResult {
    var pos = start_pos + 1;

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .left_paren) {
        return error.UnexpectedToken;
    }
    pos += 1;

    const expr = try expression_mod.parseExpression(ast, tokens, pos);
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
        if (tokens.tokens[pos].token_type != .identifier) return error.UnexpectedToken;
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
        if (tokens.tokens[pos].token_type != .identifier) return error.UnexpectedToken;
        const field_tok = pos;
        pos += 1;

        if (pos >= tokens.count or tokens.tokens[pos].token_type != .equal) {
            return error.UnexpectedToken;
        }
        pos += 1;

        const expr = try expression_mod.parseExpression(ast, tokens, pos);
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

fn parseSelectionSet(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
    depth: u16,
) ParseError!NodeResult {
    if (depth >= max_nesting_depth) return error.NestingTooDeep;

    var pos = start_pos + 1; // skip {
    var first_field: NodeIndex = null_node;
    var last_field: NodeIndex = null_node;

    while (pos < tokens.count and tokens.tokens[pos].token_type != .right_brace) {
        const field = try parseSelectionField(ast, tokens, pos, depth);

        if (first_field == null_node) {
            first_field = field.node;
            last_field = field.node;
        } else {
            ast.setNext(last_field, field.node);
            last_field = field.node;
        }
        pos = field.pos;

        if (pos < tokens.count and tokens.tokens[pos].token_type == .comma) {
            pos += 1;
        }
    }

    if (pos < tokens.count and tokens.tokens[pos].token_type == .right_brace) pos += 1;

    const node = try ast.addNode(.selection_set, .{ .unary = first_field });
    return .{ .node = node, .pos = pos };
}

fn parseSelectionField(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
    depth: u16,
) ParseError!NodeResult {
    var pos = start_pos;

    // Computed field: alias: expr.
    if (pos + 1 < tokens.count and
        tokens.tokens[pos].token_type == .identifier and
        tokens.tokens[pos + 1].token_type == .colon)
    {
        const alias_tok = pos;
        pos += 2;
        const expr = try expression_mod.parseExpression(ast, tokens, pos);
        pos = expr.pos;
        const node = try ast.addNodeFull(
            .select_computed,
            .{ .unary = expr.node },
            alias_tok,
            null_node,
        );
        return .{ .node = node, .pos = pos };
    }

    // Nested relation or simple field.
    if (tokens.tokens[pos].token_type == .identifier) {
        const next_pos = pos + 1;
        if (next_pos < tokens.count and
            (tokens.tokens[next_pos].token_type == .pipe_arrow or
                tokens.tokens[next_pos].token_type == .left_brace))
        {
            return parseNestedRelation(ast, tokens, pos, depth);
        }

        const node = try ast.addNode(.select_field, .{ .token = pos });
        return .{ .node = node, .pos = pos + 1 };
    }

    // Aggregate in selection.
    if (isAggOrFn(tokens.tokens[pos].token_type)) {
        const expr = try expression_mod.parseExpression(ast, tokens, pos);
        const node = try ast.addNodeFull(
            .select_computed,
            .{ .unary = expr.node },
            pos,
            null_node,
        );
        return .{ .node = node, .pos = expr.pos };
    }

    return error.UnexpectedToken;
}

fn parseNestedRelation(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
    depth: u16,
) ParseError!NodeResult {
    var pos = start_pos;
    const relation_tok = pos;
    pos += 1;

    var first_op: NodeIndex = null_node;
    var last_op: NodeIndex = null_node;

    while (pos < tokens.count and tokens.tokens[pos].token_type == .pipe_arrow) {
        pos += 1;
        const op = try parseOperator(ast, tokens, pos);
        if (first_op == null_node) {
            first_op = op.node;
            last_op = op.node;
        } else {
            ast.setNext(last_op, op.node);
            last_op = op.node;
        }
        pos = op.pos;
    }

    var selection: NodeIndex = null_node;
    if (pos < tokens.count and tokens.tokens[pos].token_type == .left_brace) {
        const sel = try parseSelectionSet(ast, tokens, pos, depth + 1);
        selection = sel.node;
        pos = sel.pos;
    }

    // Build nested relation: extra = relation name token.
    // data.unary = inner pipeline node (holds ops + selection).
    var inner_node: NodeIndex = null_node;
    if (first_op != null_node or selection != null_node) {
        inner_node = try ast.addNode(.pipeline, .{
            .binary = .{ .lhs = null_node, .rhs = first_op },
        });
        if (selection != null_node) {
            ast.getNodeMut(inner_node).extra = selection;
        }
    }

    const node = try ast.addNodeFull(
        .select_nested,
        .{ .unary = inner_node },
        relation_tok,
        null_node,
    );
    return .{ .node = node, .pos = pos };
}

fn parseSchemaDefinition(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
) ParseError!NodeResult {
    var pos = start_pos;
    const model_tok = pos;
    pos += 1;

    if (pos >= tokens.count or tokens.tokens[pos].token_type != .left_brace) {
        return error.UnexpectedToken;
    }
    pos += 1;

    var first_member: NodeIndex = null_node;
    var last_member: NodeIndex = null_node;

    while (pos < tokens.count and tokens.tokens[pos].token_type != .right_brace) {
        const member = try parseSchemaMember(ast, tokens, pos);
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
    start_pos: u16,
) ParseError!NodeResult {
    var pos = start_pos;
    const tok_type = tokens.tokens[pos].token_type;

    if (tok_type == .kw_field) return parseSchemaField(ast, tokens, pos);
    if (tok_type == .kw_has_many) return parseSchemaRelation(ast, tokens, pos, .schema_has_many);
    if (tok_type == .kw_has_one) return parseSchemaRelation(ast, tokens, pos, .schema_has_one);
    if (tok_type == .kw_belongs_to) return parseSchemaRelation(ast, tokens, pos, .schema_belongs_to);
    if (tok_type == .kw_index) return parseSchemaIndex(ast, tokens, pos, .schema_index);
    if (tok_type == .kw_unique_index) return parseSchemaIndex(ast, tokens, pos, .schema_unique_index);

    if (tok_type == .kw_scope) {
        pos += 1;
        if (pos >= tokens.count) return error.UnexpectedToken;
        const scope_name_tok = pos;
        pos += 1;

        var first_op: NodeIndex = null_node;
        var last_op: NodeIndex = null_node;
        while (pos < tokens.count and tokens.tokens[pos].token_type == .pipe_arrow) {
            pos += 1;
            const op = try parseOperator(ast, tokens, pos);
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

    if (pos >= tokens.count) return error.UnexpectedToken;
    const name_tok = pos;
    pos += 1;

    if (pos >= tokens.count) return error.UnexpectedToken;
    const type_tok = pos;
    pos += 1;

    // Parse optional constraints.
    while (pos < tokens.count) {
        const ct = tokens.tokens[pos].token_type;
        if (ct == .kw_primary_key or ct == .kw_not_null) {
            pos += 1;
        } else if (ct == .kw_default) {
            pos += 1;
            if (pos < tokens.count) pos += 1; // skip default value
        } else {
            break;
        }
    }

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

fn parseSchemaIndex(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
    tag: NodeTag,
) ParseError!NodeResult {
    var pos = start_pos + 1;

    var first_col: NodeIndex = null_node;
    var last_col: NodeIndex = null_node;
    while (pos < tokens.count and tokens.tokens[pos].token_type == .identifier) {
        const col_node = try ast.addNode(.expr_column_ref, .{ .token = pos });
        pos += 1;
        if (first_col == null_node) {
            first_col = col_node;
            last_col = col_node;
        } else {
            ast.setNext(last_col, col_node);
            last_col = col_node;
        }
        if (pos < tokens.count and tokens.tokens[pos].token_type == .comma) pos += 1;
    }

    const node = try ast.addNode(tag, .{ .unary = first_col });
    return .{ .node = node, .pos = pos };
}

fn isAggOrFn(tok_type: TokenType) bool {
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
        .fn_abs,
        .fn_sqrt,
        .fn_round,
        .fn_coalesce,
        => true,
        else => false,
    };
}

fn setParseError(
    result: *ParseResult,
    tokens: *const TokenizeResult,
    pos: u16,
    err: ParseError,
) void {
    result.has_error = true;
    if (pos < tokens.count) {
        result.error_line = tokens.tokens[pos].line;
    }
    const msg: []const u8 = switch (err) {
        error.AstFull => "AST capacity exceeded",
        error.UnexpectedToken => "unexpected token",
        error.NestingTooDeep => "selection set nesting too deep",
        error.StackOverflow => "expression too complex",
        error.StackUnderflow => "malformed expression",
        error.MismatchedParentheses => "mismatched parentheses",
    };
    const copy_len = @min(msg.len, result.error_message.len);
    @memcpy(result.error_message[0..copy_len], msg[0..copy_len]);
}

// --- Tests ---

const testing = std.testing;

test "parse simple query" {
    const source = "User { id email name }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
    try testing.expect(result.ast.root != null_node);
}

test "parse pipeline query" {
    const source =
        "User |> where(active = true) |> sort(name asc) |> limit(10) { id email }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);

    const root = result.ast.getNode(result.ast.root);
    try testing.expectEqual(NodeTag.root, root.tag);

    const pipeline = result.ast.getNode(root.data.unary);
    try testing.expectEqual(NodeTag.pipeline, pipeline.tag);

    const first_op = pipeline.data.binary.rhs;
    try testing.expect(first_op != null_node);
    try testing.expectEqual(NodeTag.op_where, result.ast.getNode(first_op).tag);
}

test "parse schema definition" {
    const source =
        \\User {
        \\  field id bigint primaryKey
        \\  field email string notNull
        \\  field name string
        \\  hasMany posts
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);

    const root = result.ast.getNode(result.ast.root);
    const schema = result.ast.getNode(root.data.unary);
    try testing.expectEqual(NodeTag.schema_def, schema.tag);
}

test "parse let binding" {
    const source = "let active_users = User |> where(active = true) { id }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse mutation insert" {
    const source =
        \\User |> insert(email = "a@b.com", name = "Alice") { id email }
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);

    const root = result.ast.getNode(result.ast.root);
    const pipeline = result.ast.getNode(root.data.unary);
    try testing.expectEqual(NodeTag.pipeline, pipeline.tag);

    const insert_op = result.ast.getNode(pipeline.data.binary.rhs);
    try testing.expectEqual(NodeTag.op_insert, insert_op.tag);
}

test "parse mutation delete" {
    const source = "User |> where(id = 1) |> delete";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse nested selection" {
    const source = "User { id posts { id title } }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse nested selection with pipeline" {
    const source =
        "User { id posts |> where(published = true) { id title } }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse group and aggregation" {
    const source =
        "Post |> group(author_id) |> sort(count(*) desc) |> limit(10) { author_id }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse computed field" {
    const source = "User { id full_name: lower(name) }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse error on invalid syntax" {
    const source = "|> where";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(result.has_error);
}

test "parse sort with multiple keys" {
    const source = "User |> sort(name asc, created_at desc) { id }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse update mutation" {
    const source =
        \\User |> where(id = 1) |> update(name = "Bob") { id name }
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse scope in schema" {
    const source =
        \\User {
        \\  field id bigint primaryKey
        \\  field active boolean
        \\  scope active |> where(active = true)
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}
