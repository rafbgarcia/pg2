//! Parser scenario tests.
const std = @import("std");
const ast_mod = @import("ast.zig");
const parser_mod = @import("parser.zig");
const tokenizer_mod = @import("tokenizer.zig");

const NodeTag = ast_mod.NodeTag;
const null_node = ast_mod.null_node;
const parse = parser_mod.parse;
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
        \\  field(id, i64, notNull, primaryKey)
        \\  field(email, string, notNull)
        \\  field(name, string, nullable)
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

test "parse schema field default accepts comma-separated literal" {
    const source =
        \\User {
        \\  field(status, string, notNull, default, "pending")
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
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

test "parse where membership call with list literal" {
    const source = "User |> where(in(status, [\"active\", \"pending\"])) { id }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse where membership call rejects non-list second argument" {
    const source = "User |> where(in(status, status_list)) { id }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(result.has_error);
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

test "parse contextual keyword identifiers in query positions" {
    const source =
        "User |> where(offset = 1) |> sort(offset desc) |> update(offset = offset + 1) { offset }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse scope in schema" {
    const source =
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(active, bool, nullable)
        \\  scope active |> where(active = true)
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse reference syntax in schema" {
    const source =
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  reference(posts, id, Post.user_id, withoutReferentialIntegrity)
        \\}
        \\Post {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(user_id, i64, notNull)
        \\  reference(author, user_id, User.id, withReferentialIntegrity(onDeleteRestrict, onUpdateCascade))
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse parenthesized index syntax in schema" {
    const source =
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(email, string, notNull)
        \\  index(idx_email, [email], unique)
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse schema index rejects empty field array" {
    const source =
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  index(idx_empty, [], unique)
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(result.has_error);
}

test "parse schema index rejects missing field array brackets" {
    const source =
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  index(idx_email, email, unique)
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(result.has_error);
}

test "parse schema field rejects non-parenthesized syntax" {
    const source =
        \\User {
        \\  field id i64 notNull primaryKey
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(result.has_error);
}

test "parse schema index rejects non-parenthesized syntax" {
    const source =
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  index email
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(result.has_error);
}

test "parse schema index rejects trailing comma in field array" {
    const source =
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  index(idx_email, [email,], unique)
        \\}
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(result.has_error);
}
