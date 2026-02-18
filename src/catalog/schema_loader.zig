const std = @import("std");
const catalog_mod = @import("catalog.zig");
const ast_mod = @import("../parser/ast.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");

const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const AssociationKind = catalog_mod.AssociationKind;
const Ast = ast_mod.Ast;
const NodeTag = ast_mod.NodeTag;
const NodeIndex = ast_mod.NodeIndex;
const null_node = ast_mod.null_node;
const TokenType = tokenizer_mod.TokenType;
const TokenizeResult = tokenizer_mod.TokenizeResult;

pub const LoadError = error{
    CatalogSealed,
    TooManyModels,
    TooManyColumns,
    TooManyIndexes,
    TooManyAssociations,
    TooManyScopes,
    NameBufferFull,
    DuplicateName,
    ModelNotFound,
    ColumnNotFound,
    InvalidSchema,
    UnexpectedNodeTag,
};

/// Load all schema definitions from a parsed AST into the catalog.
/// Source is needed to extract token text.
pub fn loadSchema(
    catalog: *Catalog,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
) LoadError!void {
    if (tree.root == null_node) return;

    const root = tree.getNode(tree.root);
    if (root.tag != .root) return error.UnexpectedNodeTag;

    // First pass: register all models (names only) so associations can resolve.
    var stmt = root.data.unary;
    while (stmt != null_node) {
        const node = tree.getNode(stmt);
        if (node.tag == .schema_def) {
            const model_name = tokens.getText(node.extra, source);
            _ = try catalog.addModel(model_name);
        }
        stmt = node.next;
    }

    // Second pass: populate columns, associations, indexes, scopes.
    stmt = root.data.unary;
    while (stmt != null_node) {
        const node = tree.getNode(stmt);
        if (node.tag == .schema_def) {
            const model_name = tokens.getText(node.extra, source);
            const model_id = catalog.findModel(model_name) orelse
                return error.ModelNotFound;
            try loadModelMembers(catalog, tree, tokens, source, model_id, node.data.unary);
        }
        stmt = node.next;
    }

    try catalog.resolveAssociations();
}

fn loadModelMembers(
    catalog: *Catalog,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    model_id: ModelId,
    first_member: NodeIndex,
) LoadError!void {
    var member = first_member;
    while (member != null_node) {
        const node = tree.getNode(member);
        switch (node.tag) {
            .schema_field => try loadField(catalog, tokens, source, model_id, node),
            .schema_has_many => try loadAssociation(
                catalog, tokens, source, model_id, node, .has_many,
            ),
            .schema_has_one => try loadAssociation(
                catalog, tokens, source, model_id, node, .has_one,
            ),
            .schema_belongs_to => try loadAssociation(
                catalog, tokens, source, model_id, node, .belongs_to,
            ),
            .schema_index => try loadIndex(catalog, tree, tokens, source, model_id, node, false),
            .schema_unique_index => try loadIndex(
                catalog, tree, tokens, source, model_id, node, true,
            ),
            .schema_scope => try loadScope(catalog, tokens, source, model_id, node),
            else => return error.UnexpectedNodeTag,
        }
        member = node.next;
    }
}

fn loadField(
    catalog: *Catalog,
    tokens: *const TokenizeResult,
    source: []const u8,
    model_id: ModelId,
    node: *const ast_mod.AstNode,
) LoadError!void {
    // node.data.token = field name token index, node.extra = type token index.
    const name = tokens.getText(node.data.token, source);
    const type_tok = tokens.tokens[node.extra];
    const col_type = tokenToColumnType(type_tok.token_type) orelse
        return error.InvalidSchema;

    // Determine nullable: default true unless notNull constraint present.
    // Constraints are encoded after the type token. We scan tokens after
    // extra (type_tok) until we hit a non-constraint token. However, the
    // parser already consumed constraints — we need to check the next
    // member's position or look at consecutive tokens.
    // For now, use a heuristic: check tokens after type_tok for constraints.
    const type_tok_idx = node.extra;
    var nullable = true;
    var is_primary_key = false;
    var scan = type_tok_idx + 1;
    while (scan < tokens.count) {
        const tt = tokens.tokens[scan].token_type;
        if (tt == .kw_primary_key) {
            is_primary_key = true;
            scan += 1;
        } else if (tt == .kw_not_null) {
            nullable = false;
            scan += 1;
        } else if (tt == .kw_default) {
            scan += 2; // skip default + value
        } else {
            break;
        }
    }

    // Primary key implies not null.
    if (is_primary_key) nullable = false;

    const col_id = try catalog.addColumn(model_id, name, col_type, nullable);
    if (is_primary_key) {
        catalog.setColumnPrimaryKey(model_id, col_id);
    }
}

fn loadAssociation(
    catalog: *Catalog,
    tokens: *const TokenizeResult,
    source: []const u8,
    model_id: ModelId,
    node: *const ast_mod.AstNode,
    kind: AssociationKind,
) LoadError!void {
    // node.data.token = relation target name token.
    const target_text = tokens.getText(node.data.token, source);
    // Association name is lowercase version (e.g. "posts" for hasMany posts).
    _ = try catalog.addAssociation(model_id, target_text, kind, target_text);
}

fn loadIndex(
    catalog: *Catalog,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    model_id: ModelId,
    node: *const ast_mod.AstNode,
    is_unique: bool,
) LoadError!void {
    // node.data.unary = first column ref node (linked by next).
    var col_ids: [16]catalog_mod.ColumnId = undefined;
    var col_count: u8 = 0;
    var name_buf: [256]u8 = undefined;
    var name_len: usize = 0;

    // Build index name from column names: "idx_col1_col2".
    const prefix = "idx_";
    @memcpy(name_buf[0..prefix.len], prefix);
    name_len = prefix.len;

    var col_ref = node.data.unary;
    while (col_ref != null_node and col_count < 16) {
        const col_node = tree.getNode(col_ref);
        const col_name = tokens.getText(col_node.data.token, source);
        const col_id = catalog.findColumn(model_id, col_name) orelse
            return error.ColumnNotFound;
        col_ids[col_count] = col_id;
        col_count += 1;

        // Append to name.
        if (name_len > prefix.len) {
            name_buf[name_len] = '_';
            name_len += 1;
        }
        const copy_len = @min(col_name.len, name_buf.len - name_len);
        @memcpy(name_buf[name_len..][0..copy_len], col_name[0..copy_len]);
        name_len += copy_len;

        col_ref = col_node.next;
    }

    _ = try catalog.addIndex(model_id, name_buf[0..name_len], col_ids[0..col_count], is_unique);
}

fn loadScope(
    catalog: *Catalog,
    tokens: *const TokenizeResult,
    source: []const u8,
    model_id: ModelId,
    node: *const ast_mod.AstNode,
) LoadError!void {
    // node.extra = scope name token, node.data.unary = first pipeline op node.
    const name = tokens.getText(node.extra, source);
    const first_op: u16 = if (node.data.unary != null_node)
        node.data.unary
    else
        0;
    _ = try catalog.addScope(model_id, name, first_op);
}

fn tokenToColumnType(tok_type: TokenType) ?@import("../storage/row.zig").ColumnType {
    return switch (tok_type) {
        .kw_bigint => .bigint,
        .kw_int => .int,
        .kw_float => .float,
        .kw_boolean => .boolean,
        .kw_string => .string,
        .kw_timestamp => .timestamp,
        else => null,
    };
}

// --- Tests ---

const testing = std.testing;
const parser_mod = @import("../parser/parser.zig");

test "load simple schema" {
    const source =
        \\User {
        \\  field id bigint primaryKey
        \\  field email string notNull
        \\  field name string
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try testing.expect(!parsed.has_error);

    var catalog = Catalog{};
    try loadSchema(&catalog, &parsed.ast, &tokens, source);

    try testing.expectEqual(@as(u16, 1), catalog.model_count);
    try testing.expectEqualSlices(u8, "User", catalog.getModelName(0));

    const uid: ModelId = 0;
    try testing.expectEqual(@as(u16, 3), catalog.models[uid].column_count);
    try testing.expect(catalog.models[uid].columns[0].is_primary_key);
    try testing.expect(!catalog.models[uid].columns[0].nullable);
    try testing.expect(!catalog.models[uid].columns[1].nullable); // notNull
    try testing.expect(catalog.models[uid].columns[2].nullable); // default nullable
}

test "load schema with associations" {
    const source =
        \\User {
        \\  field id bigint primaryKey
        \\  hasMany Post
        \\}
        \\Post {
        \\  field id bigint primaryKey
        \\  field user_id bigint notNull
        \\  belongsTo User
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try testing.expect(!parsed.has_error);

    var catalog = Catalog{};
    try loadSchema(&catalog, &parsed.ast, &tokens, source);

    try testing.expectEqual(@as(u16, 2), catalog.model_count);

    const user_id: ModelId = 0;
    const post_id: ModelId = 1;
    try testing.expectEqual(@as(u16, 1), catalog.models[user_id].association_count);
    try testing.expectEqual(@as(u16, 1), catalog.models[post_id].association_count);

    // Association targets resolved.
    try testing.expectEqual(post_id, catalog.models[user_id].associations[0].target_model_id);
    try testing.expectEqual(user_id, catalog.models[post_id].associations[0].target_model_id);
}

test "load schema with index" {
    const source =
        \\User {
        \\  field id bigint primaryKey
        \\  field email string notNull
        \\  uniqueIndex email
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try testing.expect(!parsed.has_error);

    var catalog = Catalog{};
    try loadSchema(&catalog, &parsed.ast, &tokens, source);

    const uid: ModelId = 0;
    try testing.expectEqual(@as(u16, 1), catalog.models[uid].index_count);
    try testing.expect(catalog.models[uid].indexes[0].is_unique);
    try testing.expectEqual(@as(u8, 1), catalog.models[uid].indexes[0].column_count);
}

test "load schema with scope" {
    const source =
        \\User {
        \\  field id bigint primaryKey
        \\  field active boolean
        \\  scope active |> where(active = true)
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try testing.expect(!parsed.has_error);

    var catalog = Catalog{};
    try loadSchema(&catalog, &parsed.ast, &tokens, source);

    const uid: ModelId = 0;
    try testing.expectEqual(@as(u16, 1), catalog.models[uid].scope_count);
    try testing.expect(catalog.findScope(uid, "active") != null);
}

test "missing association target fails" {
    const source =
        \\User {
        \\  field id bigint primaryKey
        \\  hasMany Comment
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try testing.expect(!parsed.has_error);

    var catalog = Catalog{};
    try testing.expectError(error.ModelNotFound, loadSchema(&catalog, &parsed.ast, &tokens, source));
}

test "row schema mirrors catalog columns" {
    const source =
        \\User {
        \\  field id bigint primaryKey
        \\  field name string
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try testing.expect(!parsed.has_error);

    var catalog = Catalog{};
    try loadSchema(&catalog, &parsed.ast, &tokens, source);

    const schema = &catalog.models[0].row_schema;
    try testing.expectEqual(@as(u16, 2), schema.column_count);
    try testing.expectEqualSlices(u8, "id", schema.getColumnName(0));
    try testing.expectEqualSlices(u8, "name", schema.getColumnName(1));
}
