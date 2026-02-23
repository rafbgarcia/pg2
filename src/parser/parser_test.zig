//! Parser scenario tests.
const std = @import("std");
const ast_mod = @import("ast.zig");
const parser_mod = @import("parser.zig");
const tokenizer_mod = @import("tokenizer.zig");

const NodeTag = ast_mod.NodeTag;
const null_node = ast_mod.null_node;
const parse = parser_mod.parse;
const testing = std.testing;

fn whereExprRoot(ast: *const ast_mod.Ast) ast_mod.NodeIndex {
    const root = ast.getNode(ast.root);
    const pipeline = ast.getNode(root.data.unary);
    const where_op = ast.getNode(pipeline.data.binary.rhs);
    return where_op.data.unary;
}

test "parse simple query" {
    const source = "User { id email name }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
    try testing.expect(result.ast.root != null_node);
}

test "parse pipeline query" {
    const source =
        "User |> where(active == true) |> sort(name asc) |> limit(10) { id email }";
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
    const source = "let active_users = User |> where(active == true) { id }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse multiple statements links statement list in root order" {
    const source =
        \\User { id }
        \\User |> where(id == 1) { id }
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);

    const root = result.ast.getNode(result.ast.root);
    try testing.expectEqual(NodeTag.root, root.tag);
    const first_stmt = root.data.unary;
    try testing.expect(first_stmt != null_node);
    const second_stmt = result.ast.getNode(first_stmt).next;
    try testing.expect(second_stmt != null_node);
    try testing.expectEqual(NodeTag.pipeline, result.ast.getNode(first_stmt).tag);
    try testing.expectEqual(NodeTag.pipeline, result.ast.getNode(second_stmt).tag);
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
    const first_assignment = result.ast.getNode(insert_op.data.unary);
    try testing.expectEqual(NodeTag.assignment, first_assignment.tag);
}

test "parse multi-row mutation insert produces row-group chain" {
    const source =
        \\User |> insert((id = 1, name = "Alice"), (id = 2, name = "Bob")) { id name }
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);

    const root = result.ast.getNode(result.ast.root);
    const pipeline = result.ast.getNode(root.data.unary);
    const insert_op = result.ast.getNode(pipeline.data.binary.rhs);
    try testing.expectEqual(NodeTag.op_insert, insert_op.tag);

    const row_group_a_idx = insert_op.data.unary;
    try testing.expect(row_group_a_idx != null_node);
    const row_group_a = result.ast.getNode(row_group_a_idx);
    try testing.expectEqual(NodeTag.insert_row_group, row_group_a.tag);
    try testing.expectEqual(@as(u16, 2), result.ast.listLen(row_group_a.data.unary));

    const row_group_b_idx = row_group_a.next;
    try testing.expect(row_group_b_idx != null_node);
    const row_group_b = result.ast.getNode(row_group_b_idx);
    try testing.expectEqual(NodeTag.insert_row_group, row_group_b.tag);
    try testing.expectEqual(@as(u16, 2), result.ast.listLen(row_group_b.data.unary));
}

test "parse single-row insert with parenthesized expression stays single-row" {
    const source = "User |> insert(id = (1 + 2), name = \"Alice\") { id }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);

    const root = result.ast.getNode(result.ast.root);
    const pipeline = result.ast.getNode(root.data.unary);
    const insert_op = result.ast.getNode(pipeline.data.binary.rhs);
    try testing.expectEqual(NodeTag.op_insert, insert_op.tag);
    try testing.expect(insert_op.data.unary != null_node);

    const first = result.ast.getNode(insert_op.data.unary);
    try testing.expectEqual(NodeTag.assignment, first.tag);
}

test "parse mutation delete" {
    const source = "User |> where(id == 1) |> delete";
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
        "User { id posts |> where(published == true) { id title } }";
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

test "parse having with aggregate predicate" {
    const source =
        "Post |> group(author_id) |> having(count(*) > 1 && sum(score) >= 10) { author_id }";
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

test "parse where symbolic boolean precedence with explicit parentheses" {
    const source = "User |> where((a == 1 || b == 2) && !archived) { id }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);

    const expr_root = result.ast.getNode(whereExprRoot(&result.ast));
    try testing.expectEqual(NodeTag.expr_binary, expr_root.tag);
    try testing.expectEqual(tokenizer_mod.TokenType.and_and, tokens.tokens[expr_root.extra].token_type);

    const lhs = result.ast.getNode(expr_root.data.binary.lhs);
    try testing.expectEqual(NodeTag.expr_binary, lhs.tag);
    try testing.expectEqual(tokenizer_mod.TokenType.or_or, tokens.tokens[lhs.extra].token_type);

    const rhs = result.ast.getNode(expr_root.data.binary.rhs);
    try testing.expectEqual(NodeTag.expr_unary, rhs.tag);
    try testing.expectEqual(tokenizer_mod.TokenType.bang, tokens.tokens[rhs.extra].token_type);
}

test "parse where rejects legacy textual logical forms" {
    const and_source = "User |> where(active and verified) { id }";
    const and_tokens = tokenizer_mod.tokenize(and_source);
    const and_result = parse(&and_tokens, and_source);
    try testing.expect(and_result.has_error);

    const or_source = "User |> where(active or verified) { id }";
    const or_tokens = tokenizer_mod.tokenize(or_source);
    const or_result = parse(&or_tokens, or_source);
    try testing.expect(or_result.has_error);

    const not_source = "User |> where(not active) { id }";
    const not_tokens = tokenizer_mod.tokenize(not_source);
    const not_result = parse(&not_tokens, not_source);
    try testing.expect(not_result.has_error);
}

test "parse rejects single equals in expression contexts" {
    const where_source = "User |> where(id = 1) { id }";
    const where_tokens = tokenizer_mod.tokenize(where_source);
    const where_result = parse(&where_tokens, where_source);
    try testing.expect(where_result.has_error);

    const select_source = "User { id is_admin: active = true }";
    const select_tokens = tokenizer_mod.tokenize(select_source);
    const select_result = parse(&select_tokens, select_source);
    try testing.expect(select_result.has_error);

    const sort_source = "User |> sort(id = 1 asc) { id }";
    const sort_tokens = tokenizer_mod.tokenize(sort_source);
    const sort_result = parse(&sort_tokens, sort_source);
    try testing.expect(sort_result.has_error);

    const update_rhs_source = "User |> update(name = id = 1) { id }";
    const update_rhs_tokens = tokenizer_mod.tokenize(update_rhs_source);
    const update_rhs_result = parse(&update_rhs_tokens, update_rhs_source);
    try testing.expect(update_rhs_result.has_error);
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

test "parse sort with arithmetic expression key" {
    const source = "User |> sort(score + bonus desc, id asc) { id }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse update mutation" {
    const source =
        \\User |> where(id == 1) |> update(name = "Bob") { id name }
    ;
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse contextual keyword identifiers in query positions" {
    const source =
        "User |> where(offset == 1) |> sort(offset desc) |> update(offset = offset + 1) { offset }";
    const tokens = tokenizer_mod.tokenize(source);
    const result = parse(&tokens, source);
    try testing.expect(!result.has_error);
}

test "parse scope in schema" {
    const source =
        \\User {
        \\  field(id, i64, notNull, primaryKey)
        \\  field(active, bool, nullable)
        \\  scope active |> where(active == true)
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
