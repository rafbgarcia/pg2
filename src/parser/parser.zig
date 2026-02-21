//! Top-level parser from tokens to full query/schema AST.
//!
//! Responsibilities in this file:
//! - Parses statement/pipeline wiring and selection sets.
//! - Delegates operator and schema sub-grammars to focused submodules.
//! - Returns bounded parse diagnostics without heap allocation in core flow.
const std = @import("std");
const ast_mod = @import("ast.zig");
const tokenizer_mod = @import("tokenizer.zig");
const expression_mod = @import("expression.zig");
const parser_ops = @import("parser_ops.zig");
const parser_schema = @import("parser_schema.zig");
const shared = @import("parser_shared.zig");

const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const null_node = ast_mod.null_node;
const TokenizeResult = tokenizer_mod.TokenizeResult;

const NodeResult = shared.NodeResult;
const max_nesting_depth = shared.max_nesting_depth;

pub const ParseError = shared.ParseError;

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
    if (!tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type)) return error.UnexpectedToken;
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
            parser_schema.isSchemaKeyword(tokens.tokens[lookahead + 1].token_type))
        {
            return parser_schema.parseSchemaDefinition(ast, tokens, start_pos);
        }
    }

    return parsePipeline(ast, tokens, start_pos);
}

fn parsePipeline(
    ast: *Ast,
    tokens: *const TokenizeResult,
    start_pos: u16,
) ParseError!NodeResult {
    var pos = start_pos;

    const source_tok = pos;
    if (tokens.tokens[pos].token_type != .model_name and
        !tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type))
    {
        return error.UnexpectedToken;
    }
    pos += 1;

    const source_node = try ast.addNode(.pipe_source, .{ .token = source_tok });

    // Check for dot access (Model.index_name).
    if (pos < tokens.count and tokens.tokens[pos].token_type == .dot) {
        pos += 1;
        if (pos < tokens.count and
            (tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type) or
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
            const op = try parser_ops.parseOperator(ast, tokens, pos);
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
        tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type) and
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
    if (tokenizer_mod.isContextualIdentifier(tokens.tokens[pos].token_type)) {
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
    if (parser_ops.isAggOrFn(tokens.tokens[pos].token_type)) {
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
        const op = try parser_ops.parseOperator(ast, tokens, pos);
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
