//! PK index scan planner — analyzes WHERE clauses for index-eligible patterns.
//!
//! Responsibilities in this file:
//! - Inspects the WHERE predicate AST for PK-indexable conditions.
//! - Returns a scan strategy (table_scan, pk_point_lookup, pk_range_scan).
//! - Encodes PK boundary values into sort-preserving key bytes.
//!
//! Why this exists:
//! - Separates scan-strategy analysis from executor control flow.
//! - Enables O(log n) reads on PK columns via existing B+ tree infrastructure.

const std = @import("std");
const ast_mod = @import("../parser/ast.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const filter_mod = @import("filter.zig");
const index_key_mod = @import("../storage/index_key.zig");
const row_mod = @import("../storage/row.zig");

const Ast = ast_mod.Ast;
const NodeIndex = ast_mod.NodeIndex;
const null_node = ast_mod.null_node;
const TokenType = tokenizer_mod.TokenType;
const TokenizeResult = tokenizer_mod.TokenizeResult;
const Value = row_mod.Value;

pub const ScanStrategy = enum {
    table_scan,
    pk_point_lookup,
    pk_range_scan,
};

pub const max_key_buf = 1024;

pub const PkScanPlan = struct {
    strategy: ScanStrategy = .table_scan,
    /// Point lookup: the equality value (for encoding by caller).
    eq_value: Value = .{ .null_value = {} },
    /// Range scan: pre-encoded key bytes in caller-owned buffers.
    lo_key_buf: [max_key_buf]u8 = undefined,
    lo_key_len: u16 = 0,
    hi_key_buf: [max_key_buf]u8 = undefined,
    hi_key_len: u16 = 0,

    pub fn loKey(self: *const PkScanPlan) ?[]const u8 {
        if (self.lo_key_len == 0) return null;
        return self.lo_key_buf[0..self.lo_key_len];
    }

    pub fn hiKey(self: *const PkScanPlan) ?[]const u8 {
        if (self.hi_key_len == 0) return null;
        return self.hi_key_buf[0..self.hi_key_len];
    }
};

/// Analyze a WHERE clause predicate to determine whether a PK index scan can
/// replace a full table scan. Returns a `PkScanPlan` describing the chosen
/// strategy and any encoded key boundaries.
pub fn analyze(
    ast: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    predicate_node: NodeIndex,
    pk_col_name: []const u8,
) PkScanPlan {
    if (predicate_node == null_node) return PkScanPlan{};

    const node = ast.getNode(predicate_node);

    if (node.tag != .expr_binary) return PkScanPlan{};

    const op = tokens.tokens[node.extra].token_type;
    const lhs_idx = node.data.binary.lhs;
    const rhs_idx = node.data.binary.rhs;

    // --- Equality: column == literal  or  literal == column ---
    if (op == .equal_equal) {
        if (isPkColumnRef(ast, tokens, source, lhs_idx, pk_col_name)) {
            if (tryParseLiteral(ast, tokens, source, rhs_idx)) |val| {
                return PkScanPlan{
                    .strategy = .pk_point_lookup,
                    .eq_value = val,
                };
            }
        }
        if (isPkColumnRef(ast, tokens, source, rhs_idx, pk_col_name)) {
            if (tryParseLiteral(ast, tokens, source, lhs_idx)) |val| {
                return PkScanPlan{
                    .strategy = .pk_point_lookup,
                    .eq_value = val,
                };
            }
        }
        return PkScanPlan{};
    }

    // --- Range comparisons ---
    if (op == .less_than or op == .less_equal or op == .greater_than or op == .greater_equal) {
        return analyzeComparison(ast, tokens, source, lhs_idx, rhs_idx, op, pk_col_name);
    }

    // --- AND: merge two sub-plans ---
    if (op == .and_and) {
        const left_plan = analyze(ast, tokens, source, lhs_idx, pk_col_name);
        const right_plan = analyze(ast, tokens, source, rhs_idx, pk_col_name);
        return mergePlans(left_plan, right_plan);
    }

    return PkScanPlan{};
}

/// Analyze a single comparison operator where one side is a PK column ref
/// and the other is a literal. Handles both orientations (column op literal
/// and literal op column).
fn analyzeComparison(
    ast: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    lhs_idx: NodeIndex,
    rhs_idx: NodeIndex,
    op: TokenType,
    pk_col_name: []const u8,
) PkScanPlan {
    const col_on_left = isPkColumnRef(ast, tokens, source, lhs_idx, pk_col_name);
    const col_on_right = isPkColumnRef(ast, tokens, source, rhs_idx, pk_col_name);

    if (!col_on_left and !col_on_right) return PkScanPlan{};

    const literal_idx = if (col_on_left) rhs_idx else lhs_idx;
    const val = tryParseLiteral(ast, tokens, source, literal_idx) orelse return PkScanPlan{};

    // Encode the literal value into a temporary buffer.
    var encode_buf: [max_key_buf]u8 = undefined;
    const encoded = index_key_mod.encodeValue(val, &encode_buf);

    // Determine the effective operator from the column's perspective.
    // If the column is on the right, we flip the comparison direction:
    //   literal < column  ⟹  column > literal
    //   literal <= column ⟹  column >= literal
    //   literal > column  ⟹  column < literal
    //   literal >= column ⟹  column <= literal
    const effective_op: TokenType = if (col_on_left) op else switch (op) {
        .less_than => .greater_than,
        .less_equal => .greater_equal,
        .greater_than => .less_than,
        .greater_equal => .less_equal,
        else => unreachable,
    };

    var plan = PkScanPlan{ .strategy = .pk_range_scan };

    switch (effective_op) {
        // column > literal → lower bound is successor(encoded) (exclusive lower)
        .greater_than => {
            var succ_buf: [max_key_buf]u8 = undefined;
            if (successorKey(encoded, &succ_buf)) |succ| {
                @memcpy(plan.lo_key_buf[0..succ.len], succ);
                plan.lo_key_len = @intCast(succ.len);
            } else {
                // All 0xFF — no valid lower bound (no key can be strictly greater).
                return PkScanPlan{};
            }
        },
        // column >= literal → lower bound is encoded directly (inclusive)
        .greater_equal => {
            @memcpy(plan.lo_key_buf[0..encoded.len], encoded);
            plan.lo_key_len = @intCast(encoded.len);
        },
        // column < literal → upper bound is encoded directly (exclusive)
        .less_than => {
            @memcpy(plan.hi_key_buf[0..encoded.len], encoded);
            plan.hi_key_len = @intCast(encoded.len);
        },
        // column <= literal → upper bound is successor(encoded) (inclusive → exclusive)
        .less_equal => {
            var succ_buf: [max_key_buf]u8 = undefined;
            if (successorKey(encoded, &succ_buf)) |succ| {
                @memcpy(plan.hi_key_buf[0..succ.len], succ);
                plan.hi_key_len = @intCast(succ.len);
            } else {
                // All 0xFF — successor overflow means "up to the end", so no
                // upper bound needed; the range extends to +infinity.
                plan.hi_key_len = 0;
            }
        },
        else => unreachable,
    }

    return plan;
}

/// Merge two sub-plans produced from an AND conjunction.
fn mergePlans(left: PkScanPlan, right: PkScanPlan) PkScanPlan {
    // Equality wins — if either side found a point lookup, return it.
    if (left.strategy == .pk_point_lookup) return left;
    if (right.strategy == .pk_point_lookup) return right;

    // Both table_scan → table_scan.
    if (left.strategy == .table_scan and right.strategy == .table_scan) return PkScanPlan{};

    // One or both sides have range bounds — combine them.
    var plan = PkScanPlan{ .strategy = .pk_range_scan };

    // Take lower bound from whichever side provides one.
    if (left.lo_key_len > 0) {
        @memcpy(plan.lo_key_buf[0..left.lo_key_len], left.lo_key_buf[0..left.lo_key_len]);
        plan.lo_key_len = left.lo_key_len;
    } else if (right.lo_key_len > 0) {
        @memcpy(plan.lo_key_buf[0..right.lo_key_len], right.lo_key_buf[0..right.lo_key_len]);
        plan.lo_key_len = right.lo_key_len;
    }

    // Take upper bound from whichever side provides one.
    if (left.hi_key_len > 0) {
        @memcpy(plan.hi_key_buf[0..left.hi_key_len], left.hi_key_buf[0..left.hi_key_len]);
        plan.hi_key_len = left.hi_key_len;
    } else if (right.hi_key_len > 0) {
        @memcpy(plan.hi_key_buf[0..right.hi_key_len], right.hi_key_buf[0..right.hi_key_len]);
        plan.hi_key_len = right.hi_key_len;
    }

    return plan;
}

/// Check whether the AST node at `node_idx` is a column reference whose name
/// matches the given PK column name.
fn isPkColumnRef(
    ast: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    node_idx: NodeIndex,
    pk_col_name: []const u8,
) bool {
    if (node_idx == null_node) return false;
    const node = ast.getNode(node_idx);
    if (node.tag != .expr_column_ref) return false;
    const tok = tokens.tokens[node.data.token];
    const col_name = source[tok.start..][0..tok.len];
    return std.mem.eql(u8, col_name, pk_col_name);
}

/// Try to parse a literal value from the AST node. Returns null if the node
/// is not a literal or if parsing fails.
fn tryParseLiteral(
    ast: *const Ast,
    tokens: *const TokenizeResult,
    source: []const u8,
    node_idx: NodeIndex,
) ?Value {
    if (node_idx == null_node) return null;
    const node = ast.getNode(node_idx);
    if (node.tag != .expr_literal) return null;
    return filter_mod.parseLiteralValue(tokens, source, node.data.token) catch null;
}

/// Compute the successor key (smallest key strictly greater than input).
/// Used to convert inclusive upper bounds to exclusive for B+ tree range [lo, hi).
/// Returns the successor key slice, or null if overflow (all 0xFF).
pub fn successorKey(key: []const u8, buf: []u8) ?[]const u8 {
    @memcpy(buf[0..key.len], key);
    var i = key.len;
    while (i > 0) {
        i -= 1;
        if (buf[i] < 0xFF) {
            buf[i] += 1;
            return buf[0..key.len];
        }
        buf[i] = 0x00;
    }
    return null; // all 0xFF, no successor
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "equality on PK column → point lookup" {
    // Source: "id == 42"
    // Tokens: [0]=identifier("id"), [1]=equal_equal, [2]=integer_literal("42"), [3]=end_of_input
    const source = "id == 42";
    const tokens = tokenizer_mod.tokenize(source);

    var ast = Ast{};
    // node 0: column_ref for "id" at token 0
    const col_node = try ast.addNode(.expr_column_ref, .{ .token = 0 });
    // node 1: literal 42 at token 2
    const lit_node = try ast.addNode(.expr_literal, .{ .token = 2 });
    // node 2: binary == with extra=1 (token index of ==)
    const bin_node = try ast.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = col_node, .rhs = lit_node } },
        1, // operator token index
        null_node,
    );

    const plan = analyze(&ast, &tokens, source, bin_node, "id");
    try testing.expectEqual(ScanStrategy.pk_point_lookup, plan.strategy);
    try testing.expectEqual(@as(i64, 42), plan.eq_value.i64);
}

test "reversed equality: literal == PK column → point lookup" {
    // Source: "42 == id"
    const source = "42 == id";
    const tokens = tokenizer_mod.tokenize(source);

    var ast = Ast{};
    const lit_node = try ast.addNode(.expr_literal, .{ .token = 0 });
    const col_node = try ast.addNode(.expr_column_ref, .{ .token = 2 });
    const bin_node = try ast.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = lit_node, .rhs = col_node } },
        1,
        null_node,
    );

    const plan = analyze(&ast, &tokens, source, bin_node, "id");
    try testing.expectEqual(ScanStrategy.pk_point_lookup, plan.strategy);
    try testing.expectEqual(@as(i64, 42), plan.eq_value.i64);
}

test "range on PK column → range scan with both bounds" {
    // Represents: id > 3 && id < 10
    // We build this as a top-level AND with two binary sub-expressions.
    //
    // Tokens for "id > 3 && id < 10":
    //   [0]=id  [1]=>  [2]=3  [3]=&&  [4]=id  [5]=<  [6]=10  [7]=eoi
    const source = "id > 3 && id < 10";
    const tokens = tokenizer_mod.tokenize(source);

    var ast = Ast{};
    // Left sub-expr: id > 3
    const col_left = try ast.addNode(.expr_column_ref, .{ .token = 0 });
    const lit_left = try ast.addNode(.expr_literal, .{ .token = 2 });
    const gt_node = try ast.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = col_left, .rhs = lit_left } },
        1, // token index of >
        null_node,
    );

    // Right sub-expr: id < 10
    const col_right = try ast.addNode(.expr_column_ref, .{ .token = 4 });
    const lit_right = try ast.addNode(.expr_literal, .{ .token = 6 });
    const lt_node = try ast.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = col_right, .rhs = lit_right } },
        5, // token index of <
        null_node,
    );

    // Top-level AND
    const and_node = try ast.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = gt_node, .rhs = lt_node } },
        3, // token index of &&
        null_node,
    );

    const plan = analyze(&ast, &tokens, source, and_node, "id");
    try testing.expectEqual(ScanStrategy.pk_range_scan, plan.strategy);
    // Both bounds should be set.
    try testing.expect(plan.lo_key_len > 0);
    try testing.expect(plan.hi_key_len > 0);
}

test "non-PK column → table scan" {
    // Source: "name == 42"
    const source = "name == 42";
    const tokens = tokenizer_mod.tokenize(source);

    var ast = Ast{};
    const col_node = try ast.addNode(.expr_column_ref, .{ .token = 0 });
    const lit_node = try ast.addNode(.expr_literal, .{ .token = 2 });
    const bin_node = try ast.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = col_node, .rhs = lit_node } },
        1,
        null_node,
    );

    const plan = analyze(&ast, &tokens, source, bin_node, "id");
    try testing.expectEqual(ScanStrategy.table_scan, plan.strategy);
}

test "greater-equal sets inclusive lower bound" {
    // Source: "id >= 5"
    const source = "id >= 5";
    const tokens = tokenizer_mod.tokenize(source);

    var ast = Ast{};
    const col_node = try ast.addNode(.expr_column_ref, .{ .token = 0 });
    const lit_node = try ast.addNode(.expr_literal, .{ .token = 2 });
    const bin_node = try ast.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = col_node, .rhs = lit_node } },
        1,
        null_node,
    );

    const plan = analyze(&ast, &tokens, source, bin_node, "id");
    try testing.expectEqual(ScanStrategy.pk_range_scan, plan.strategy);
    try testing.expect(plan.lo_key_len > 0);
    try testing.expectEqual(@as(u16, 0), plan.hi_key_len);

    // The lower bound should be the encoded value of 5 (inclusive).
    var expected_buf: [max_key_buf]u8 = undefined;
    const expected = index_key_mod.encodeValue(.{ .i64 = 5 }, &expected_buf);
    try testing.expectEqualSlices(u8, expected, plan.loKey().?);
}

test "less-equal sets exclusive upper bound via successor" {
    // Source: "id <= 5"
    const source = "id <= 5";
    const tokens = tokenizer_mod.tokenize(source);

    var ast = Ast{};
    const col_node = try ast.addNode(.expr_column_ref, .{ .token = 0 });
    const lit_node = try ast.addNode(.expr_literal, .{ .token = 2 });
    const bin_node = try ast.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = col_node, .rhs = lit_node } },
        1,
        null_node,
    );

    const plan = analyze(&ast, &tokens, source, bin_node, "id");
    try testing.expectEqual(ScanStrategy.pk_range_scan, plan.strategy);
    try testing.expectEqual(@as(u16, 0), plan.lo_key_len);
    try testing.expect(plan.hi_key_len > 0);

    // The upper bound should be the successor of encoded value of 5.
    var enc_buf: [max_key_buf]u8 = undefined;
    const encoded = index_key_mod.encodeValue(.{ .i64 = 5 }, &enc_buf);
    var succ_buf: [max_key_buf]u8 = undefined;
    const expected = successorKey(encoded, &succ_buf).?;
    try testing.expectEqualSlices(u8, expected, plan.hiKey().?);
}

test "flipped comparison: 5 > id → upper bound (equivalent to id < 5)" {
    // Source: "5 > id"
    const source = "5 > id";
    const tokens = tokenizer_mod.tokenize(source);

    var ast = Ast{};
    const lit_node = try ast.addNode(.expr_literal, .{ .token = 0 });
    const col_node = try ast.addNode(.expr_column_ref, .{ .token = 2 });
    const bin_node = try ast.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = lit_node, .rhs = col_node } },
        1, // token index of >
        null_node,
    );

    const plan = analyze(&ast, &tokens, source, bin_node, "id");
    try testing.expectEqual(ScanStrategy.pk_range_scan, plan.strategy);
    // 5 > id ⟹ id < 5 ⟹ upper bound (exclusive) = encoded(5)
    try testing.expectEqual(@as(u16, 0), plan.lo_key_len);
    try testing.expect(plan.hi_key_len > 0);

    var enc_buf: [max_key_buf]u8 = undefined;
    const expected = index_key_mod.encodeValue(.{ .i64 = 5 }, &enc_buf);
    try testing.expectEqualSlices(u8, expected, plan.hiKey().?);
}

test "successor key" {
    var buf: [8]u8 = undefined;
    const key = [_]u8{ 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05 };
    const succ = successorKey(&key, &buf).?;
    try testing.expectEqual(@as(u8, 0x06), succ[7]);
    try testing.expectEqual(@as(u8, 0x80), succ[0]);
}

test "successor key overflow returns null" {
    var buf: [3]u8 = undefined;
    const key = [_]u8{ 0xFF, 0xFF, 0xFF };
    const result = successorKey(&key, &buf);
    try testing.expect(result == null);
}

test "null predicate node → table scan" {
    var ast = Ast{};
    const tokens = tokenizer_mod.tokenize("");

    const plan = analyze(&ast, &tokens, "", null_node, "id");
    try testing.expectEqual(ScanStrategy.table_scan, plan.strategy);
}

test "AND with equality on one side → point lookup wins" {
    // Represents: id == 7 && name > 3
    // Even though the right side references a non-PK column, the left
    // equality on PK should dominate.
    //
    // Tokens for "id == 7 && name > 3":
    //   [0]=id  [1]==  [2]=7  [3]=&&  [4]=name  [5]=>  [6]=3  [7]=eoi
    const source = "id == 7 && name > 3";
    const tokens = tokenizer_mod.tokenize(source);

    var ast = Ast{};
    // Left sub-expr: id == 7
    const col_left = try ast.addNode(.expr_column_ref, .{ .token = 0 });
    const lit_left = try ast.addNode(.expr_literal, .{ .token = 2 });
    const eq_node = try ast.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = col_left, .rhs = lit_left } },
        1,
        null_node,
    );

    // Right sub-expr: name > 3 (non-PK, will yield table_scan)
    const col_right = try ast.addNode(.expr_column_ref, .{ .token = 4 });
    const lit_right = try ast.addNode(.expr_literal, .{ .token = 6 });
    const gt_node = try ast.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = col_right, .rhs = lit_right } },
        5,
        null_node,
    );

    const and_node = try ast.addNodeFull(
        .expr_binary,
        .{ .binary = .{ .lhs = eq_node, .rhs = gt_node } },
        3,
        null_node,
    );

    const plan = analyze(&ast, &tokens, source, and_node, "id");
    try testing.expectEqual(ScanStrategy.pk_point_lookup, plan.strategy);
    try testing.expectEqual(@as(i64, 7), plan.eq_value.i64);
}

test "loKey and hiKey return null when length is zero" {
    const plan = PkScanPlan{};
    try testing.expect(plan.loKey() == null);
    try testing.expect(plan.hiKey() == null);
}

