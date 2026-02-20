//! Schema AST-to-catalog loader.
//!
//! Responsibilities in this file:
//! - Walks parsed schema nodes and populates catalog metadata.
//! - Resolves model/column/index/association/scope references.
//! - Validates fail-closed schema constraints and association wiring.
//! - Keeps loader behavior deterministic for the same token/AST input.
const std = @import("std");
const catalog_mod = @import("catalog.zig");
const ast_mod = @import("../parser/ast.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const row_mod = @import("../storage/row.zig");

const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const AssociationKind = catalog_mod.AssociationKind;
const ReferentialAction = catalog_mod.ReferentialAction;
const Ast = ast_mod.Ast;
const NodeTag = ast_mod.NodeTag;
const NodeIndex = ast_mod.NodeIndex;
const null_node = ast_mod.null_node;
const TokenType = tokenizer_mod.TokenType;
const TokenizeResult = tokenizer_mod.TokenizeResult;
const ColumnType = row_mod.ColumnType;
const Value = row_mod.Value;
const null_token_index: u16 = std.math.maxInt(u16);

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
    InvalidAssociationConfig,
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
    std.debug.assert(tokens.count <= tokens.tokens.len);
    std.debug.assert(tree.root < ast_mod.max_ast_nodes or tree.root == null_node);
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
                catalog,
                tokens,
                source,
                model_id,
                node,
                .has_many,
            ),
            .schema_has_one => try loadAssociation(
                catalog,
                tokens,
                source,
                model_id,
                node,
                .has_one,
            ),
            .schema_belongs_to => try loadAssociation(
                catalog,
                tokens,
                source,
                model_id,
                node,
                .belongs_to,
            ),
            .schema_reference => try loadReference(
                catalog,
                tree,
                tokens,
                source,
                model_id,
                node,
            ),
            .schema_index => try loadIndex(catalog, tree, tokens, source, model_id, node, false),
            .schema_unique_index => try loadIndex(
                catalog,
                tree,
                tokens,
                source,
                model_id,
                node,
                true,
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
    const max_constraint_scan_tokens: usize = 64;
    std.debug.assert(model_id < catalog.model_count);
    std.debug.assert(node.extra < tokens.count);
    // node.data.token = field name token index, node.extra = type token index.
    const name = tokens.getText(node.data.token, source);
    const type_tok = tokens.tokens[node.extra];
    const col_type = tokenToColumnType(type_tok.token_type) orelse
        return error.InvalidSchema;

    // Nullability must be explicitly declared per field (`notNull` or `nullable`).
    // We scan tokens after type token until non-constraint token.
    const type_tok_idx = node.extra;
    var nullable = false;
    var saw_not_null = false;
    var saw_nullable = false;
    var is_primary_key = false;
    var has_default = false;
    var default_token_idx: u16 = null_token_index;
    var scan = type_tok_idx + 1;
    var scanned_tokens: usize = 0;
    while (scan < tokens.count and scanned_tokens < max_constraint_scan_tokens) : (scanned_tokens += 1) {
        const tt = tokens.tokens[scan].token_type;
        if (tt == .comma) {
            scan += 1;
        } else if (tt == .kw_primary_key) {
            is_primary_key = true;
            scan += 1;
        } else if (tt == .kw_not_null) {
            if (saw_nullable or saw_not_null) return error.InvalidSchema;
            saw_not_null = true;
            nullable = false;
            scan += 1;
        } else if (tt == .kw_nullable) {
            if (saw_not_null or saw_nullable) return error.InvalidSchema;
            saw_nullable = true;
            nullable = true;
            scan += 1;
        } else if (tt == .kw_default) {
            scan += 1;
            if (scan < tokens.count and tokens.tokens[scan].token_type == .comma) {
                scan += 1;
            }
            if (scan >= tokens.count) return error.InvalidSchema;
            const literal_tt = tokens.tokens[scan].token_type;
            if (!isLiteralToken(literal_tt)) return error.InvalidSchema;
            has_default = true;
            default_token_idx = scan;
            scan += 1;
        } else if (tt == .right_paren) {
            scan += 1;
            break;
        } else {
            break;
        }
    }
    if (scan < tokens.count and scanned_tokens == max_constraint_scan_tokens) {
        return error.InvalidSchema;
    }

    if (!saw_not_null and !saw_nullable) return error.InvalidSchema;
    if (is_primary_key and saw_nullable) return error.InvalidSchema;

    const col_id = try catalog.addColumn(model_id, name, col_type, nullable);
    if (is_primary_key) {
        catalog.setColumnPrimaryKey(model_id, col_id);
    }
    if (has_default) {
        const default_value = try parseColumnDefaultValue(
            tokens,
            source,
            default_token_idx,
            col_type,
            nullable,
        );
        try catalog.setColumnDefault(model_id, col_id, default_value);
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

fn loadReference(
    catalog: *Catalog,
    tree: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    model_id: ModelId,
    node: *const ast_mod.AstNode,
) LoadError!void {
    var current = node.data.unary;
    if (current == null_node) return error.InvalidSchema;
    const alias_tok = current;

    current = tree.getNode(alias_tok).next;
    if (current == null_node) return error.InvalidSchema;
    const local_tok = current;

    current = tree.getNode(local_tok).next;
    if (current == null_node) return error.InvalidSchema;
    const target_model_tok = current;

    current = tree.getNode(target_model_tok).next;
    if (current == null_node) return error.InvalidSchema;
    const target_field_tok = current;

    current = tree.getNode(target_field_tok).next;
    if (current == null_node) return error.InvalidSchema;
    const mode_tok = current;

    const alias = tokens.getText(tree.getNode(alias_tok).data.token, source);
    const local_field = tokens.getText(tree.getNode(local_tok).data.token, source);
    const target_model = tokens.getText(tree.getNode(target_model_tok).data.token, source);
    const target_field = tokens.getText(tree.getNode(target_field_tok).data.token, source);
    const mode_token_type = tokens.tokens[tree.getNode(mode_tok).data.token].token_type;

    const assoc_id = try catalog.addAssociation(
        model_id,
        alias,
        .has_many,
        target_model,
    );
    try catalog.setAssociationKeyNames(
        model_id,
        assoc_id,
        local_field,
        target_field,
    );

    switch (mode_token_type) {
        .kw_without_referential_integrity => {
            try catalog.setAssociationReferentialIntegrity(
                model_id,
                assoc_id,
                .without_referential_integrity,
                .unspecified,
                .unspecified,
            );
        },
        .kw_with_referential_integrity => {
            current = tree.getNode(mode_tok).next;
            if (current == null_node) return error.InvalidSchema;
            const on_delete_token_type = tokens.tokens[tree.getNode(current).data.token].token_type;
            const on_delete = mapDeleteAction(on_delete_token_type) orelse
                return error.InvalidSchema;

            const on_update_node = tree.getNode(current).next;
            if (on_update_node == null_node) return error.InvalidSchema;
            const on_update_token_type = tokens.tokens[tree.getNode(on_update_node).data.token].token_type;
            const on_update = mapUpdateAction(on_update_token_type) orelse
                return error.InvalidSchema;

            try catalog.setAssociationReferentialIntegrity(
                model_id,
                assoc_id,
                .with_referential_integrity,
                on_delete,
                on_update,
            );
        },
        else => return error.InvalidSchema,
    }
}

fn mapDeleteAction(tok_type: TokenType) ?ReferentialAction {
    return switch (tok_type) {
        .kw_on_delete_restrict => .restrict,
        .kw_on_delete_cascade => .cascade,
        .kw_on_delete_set_null => .set_null,
        .kw_on_delete_set_default => .set_default,
        else => null,
    };
}

fn mapUpdateAction(tok_type: TokenType) ?ReferentialAction {
    return switch (tok_type) {
        .kw_on_update_restrict => .restrict,
        .kw_on_update_cascade => .cascade,
        .kw_on_update_set_null => .set_null,
        .kw_on_update_set_default => .set_default,
        else => null,
    };
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
    std.debug.assert(model_id < catalog.model_count);
    // node.data.unary = first column ref node (linked by next).
    var col_ids: [16]catalog_mod.ColumnId = undefined;
    var col_count: u8 = 0;
    var name_buf: [256]u8 = undefined;
    var name_len: usize = 0;
    var has_explicit_name = false;

    if (node.extra != null_token_index) {
        const name = tokens.getText(node.extra, source);
        const copy_len = @min(name.len, name_buf.len);
        @memcpy(name_buf[0..copy_len], name[0..copy_len]);
        name_len = copy_len;
        has_explicit_name = true;
    } else {
        // Build index name from column names: "idx_col1_col2".
        const prefix = "idx_";
        @memcpy(name_buf[0..prefix.len], prefix);
        name_len = prefix.len;
    }

    var col_ref = node.data.unary;
    while (col_ref != null_node and col_count < 16) {
        std.debug.assert(col_count < col_ids.len);
        const col_node = tree.getNode(col_ref);
        const col_name = tokens.getText(col_node.data.token, source);
        const col_id = catalog.findColumn(model_id, col_name) orelse
            return error.ColumnNotFound;
        col_ids[col_count] = col_id;
        col_count += 1;

        if (!has_explicit_name) {
            // Append to autogenerated name.
            if (name_len > 4) {
                name_buf[name_len] = '_';
                name_len += 1;
            }
            const copy_len = @min(col_name.len, name_buf.len - name_len);
            @memcpy(name_buf[name_len..][0..copy_len], col_name[0..copy_len]);
            name_len += copy_len;
        }

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

fn isLiteralToken(tt: TokenType) bool {
    return switch (tt) {
        .integer_literal,
        .float_literal,
        .string_literal,
        .true_literal,
        .false_literal,
        .null_literal,
        => true,
        else => false,
    };
}

fn parseColumnDefaultValue(
    tokens: *const TokenizeResult,
    source: []const u8,
    token_idx: u16,
    column_type: ColumnType,
    nullable: bool,
) LoadError!Value {
    std.debug.assert(token_idx < tokens.count);
    const tok = tokens.tokens[token_idx];
    const text = tokens.getText(token_idx, source);

    if (tok.token_type == .null_literal) {
        if (!nullable) return error.InvalidSchema;
        return .{ .null_value = {} };
    }

    return switch (column_type) {
        .bigint => switch (tok.token_type) {
            .integer_literal => Value{
                .bigint = std.fmt.parseInt(i64, text, 10) catch return error.InvalidSchema,
            },
            else => error.InvalidSchema,
        },
        .int => switch (tok.token_type) {
            .integer_literal => blk: {
                const parsed = std.fmt.parseInt(i64, text, 10) catch return error.InvalidSchema;
                const narrowed = std.math.cast(i32, parsed) orelse return error.InvalidSchema;
                break :blk Value{ .int = narrowed };
            },
            else => error.InvalidSchema,
        },
        .float => switch (tok.token_type) {
            .integer_literal, .float_literal => Value{
                .float = std.fmt.parseFloat(f64, text) catch return error.InvalidSchema,
            },
            else => error.InvalidSchema,
        },
        .boolean => switch (tok.token_type) {
            .true_literal => Value{ .boolean = true },
            .false_literal => Value{ .boolean = false },
            else => error.InvalidSchema,
        },
        .string => switch (tok.token_type) {
            .string_literal => blk: {
                if (text.len < 2 or text[0] != '"' or text[text.len - 1] != '"') {
                    return error.InvalidSchema;
                }
                break :blk Value{ .string = text[1 .. text.len - 1] };
            },
            else => error.InvalidSchema,
        },
        .timestamp => switch (tok.token_type) {
            .integer_literal => Value{
                .timestamp = std.fmt.parseInt(i64, text, 10) catch return error.InvalidSchema,
            },
            else => error.InvalidSchema,
        },
    };
}

// --- Tests ---

const testing = std.testing;
const parser_mod = @import("../parser/parser.zig");

test "load simple schema" {
    const source =
        \\User {
        \\  field id bigint notNull primaryKey
        \\  field email string notNull
        \\  field name string nullable
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
    try testing.expect(catalog.models[uid].columns[2].nullable); // explicit nullable
}

test "load schema parses typed column defaults" {
    const source =
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(plan, string, notNull, default, "free")
        \\  field(login_count, int, notNull, default, 0)
        \\  field(enabled, boolean, notNull, default, true)
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try testing.expect(!parsed.has_error);

    var catalog = Catalog{};
    try loadSchema(&catalog, &parsed.ast, &tokens, source);

    const plan_default = catalog.getColumnDefault(0, 1).?;
    const count_default = catalog.getColumnDefault(0, 2).?;
    const enabled_default = catalog.getColumnDefault(0, 3).?;
    try testing.expectEqualSlices(u8, "free", plan_default.string);
    try testing.expectEqual(@as(i32, 0), count_default.int);
    try testing.expect(enabled_default.boolean);
}

test "load schema rejects non-null column default null" {
    const source =
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(name, string, notNull, default, null)
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try testing.expect(!parsed.has_error);

    var catalog = Catalog{};
    try testing.expectError(
        error.InvalidSchema,
        loadSchema(&catalog, &parsed.ast, &tokens, source),
    );
}

test "load schema rejects field without explicit nullability constraint" {
    const source =
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(name, string)
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try testing.expect(!parsed.has_error);

    var catalog = Catalog{};
    try testing.expectError(
        error.InvalidSchema,
        loadSchema(&catalog, &parsed.ast, &tokens, source),
    );
}

test "load schema rejects field with conflicting nullability constraints" {
    const source =
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(name, string, notNull, nullable)
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try testing.expect(!parsed.has_error);

    var catalog = Catalog{};
    try testing.expectError(
        error.InvalidSchema,
        loadSchema(&catalog, &parsed.ast, &tokens, source),
    );
}

test "load schema rejects belongsTo without explicit RI config" {
    const source =
        \\User {
        \\  field id bigint notNull primaryKey
        \\  hasMany Post
        \\}
        \\Post {
        \\  field id bigint notNull primaryKey
        \\  field user_id bigint notNull
        \\  belongsTo User
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try testing.expect(!parsed.has_error);

    var catalog = Catalog{};
    try testing.expectError(
        error.InvalidAssociationConfig,
        loadSchema(&catalog, &parsed.ast, &tokens, source),
    );
}

test "load schema with reference and explicit RI config" {
    const source =
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  reference(posts, id, Post.user_id, withoutReferentialIntegrity)
        \\}
        \\Post {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(user_id, bigint, notNull)
        \\  reference(author, user_id, User.id, withReferentialIntegrity(onDeleteRestrict, onUpdateCascade))
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try testing.expect(!parsed.has_error);

    var catalog = Catalog{};
    try loadSchema(&catalog, &parsed.ast, &tokens, source);

    const user_id: ModelId = 0;
    const post_id: ModelId = 1;
    const user_posts = catalog.models[user_id].associations[0];
    const post_author = catalog.models[post_id].associations[0];

    try testing.expectEqual(@as(u16, 0), user_posts.local_column_id);
    try testing.expectEqual(@as(u16, 1), user_posts.foreign_key_column_id);
    try testing.expectEqual(
        catalog_mod.ReferentialIntegrityMode.without_referential_integrity,
        user_posts.referential_integrity_mode,
    );

    try testing.expectEqual(@as(u16, 1), post_author.local_column_id);
    try testing.expectEqual(@as(u16, 0), post_author.foreign_key_column_id);
    try testing.expectEqual(
        catalog_mod.ReferentialIntegrityMode.with_referential_integrity,
        post_author.referential_integrity_mode,
    );
    try testing.expectEqual(catalog_mod.ReferentialAction.restrict, post_author.on_delete);
    try testing.expectEqual(catalog_mod.ReferentialAction.cascade, post_author.on_update);
}

test "load schema rejects unsupported referential set default actions" {
    const source =
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\}
        \\Post {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(user_id, bigint, notNull)
        \\  reference(author, user_id, User.id, withReferentialIntegrity(onDeleteSetDefault, onUpdateRestrict))
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const parsed = parser_mod.parse(&tokens, source);
    try testing.expect(!parsed.has_error);

    var catalog = Catalog{};
    try testing.expectError(
        error.InvalidAssociationConfig,
        loadSchema(&catalog, &parsed.ast, &tokens, source),
    );
}

test "load schema with index" {
    const source =
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(email, string, notNull)
        \\  index(idx_email, [email], unique)
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
    try testing.expectEqualSlices(
        u8,
        "idx_email",
        catalog.getName(
            catalog.models[uid].indexes[0].name_offset,
            catalog.models[uid].indexes[0].name_len,
        ),
    );
}

test "load schema with scope" {
    const source =
        \\User {
        \\  field id bigint notNull primaryKey
        \\  field active boolean nullable
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
        \\  field id bigint notNull primaryKey
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
        \\  field id bigint notNull primaryKey
        \\  field name string nullable
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
