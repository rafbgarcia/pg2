//! Fixed-capacity abstract syntax tree (AST) definitions.
//!
//! Responsibilities in this file:
//! - Declares all parser node tags and compact node payload layouts.
//! - Owns AST node allocation/indexing/list-link helpers.
//! - Provides bounded AST storage contracts used across parser/executor.
//! - Serves as the canonical syntax-tree data model for query/schema input.
const std = @import("std");

/// Maximum number of AST nodes in a single parse.
pub const max_ast_nodes = 8192;

/// Index into the AST node array.
pub const NodeIndex = u16;

/// Sentinel value for "no node".
pub const null_node: NodeIndex = std.math.maxInt(NodeIndex);

/// Tags for AST node types.
pub const NodeTag = enum(u8) {
    // Top-level
    root, // program root; data.unary = first statement, linked by next
    schema_def, // model schema definition
    query, // query starting from a model/source

    // Pipeline
    pipeline, // data.binary = source, rhs = first operator (linked by next)
    pipe_source, // model reference; data.token = model name token

    // Operators (pipeline stages)
    op_where, // data.unary = expression node
    op_sort, // data.unary = first sort_key node (linked by next)
    op_limit, // data.unary = expression node
    op_offset, // data.unary = expression node
    op_group, // data.unary = first group field (linked by next)
    op_unique,
    op_delete,
    op_insert, // data.unary = first assignment OR first insert_row_group (linked by next)
    op_update, // data.unary = first assignment (linked by next)
    op_inspect,
    op_scope_ref, // reference to a named scope; data.token = scope name

    // Expressions
    expr_binary, // data.binary = lhs, rhs; extra = operator token
    expr_unary, // data.unary = operand; extra = operator token
    expr_literal, // data.token = literal token
    expr_column_ref, // data.token = column name
    expr_function_call, // data.token = fn name, data.unary = first arg (linked by next)
    expr_aggregate, // data.token = agg name, data.unary = arg (or null_node for count(*))
    expr_list, // [a, b, c]; data.unary = first element (linked by next)
    expr_in, // data.binary = lhs (value), rhs (list)
    expr_not_in, // data.binary = lhs (value), rhs (list)
    expr_parameter, // $param; data.token = parameter token

    // Selection set
    selection_set, // data.unary = first field (linked by next)
    select_field, // data.token = field name (+ optional alias via extra)
    select_computed, // alias: expr; data.token = alias, data.unary = expression
    select_nested, // nested relation; data.token = relation name, data.unary = pipeline/selection

    // Sort key
    // data.token = field token (column key), or data.unary = expression node.
    // extra bit 0 encodes direction asc(0)/desc(1), bit 15 marks expression key.
    sort_key,

    // Assignment (for insert/update)
    insert_row_group, // data.unary = first assignment in this row (linked by next)
    assignment, // data.token = field name, data.unary = expression

    // Schema constructs
    schema_field, // data.token = field name; linked by next
    schema_has_many, // data.token = relation name
    schema_has_one, // data.token = relation name
    schema_belongs_to, // data.token = relation name
    schema_index, // data.unary = first column ref (linked by next)
    schema_unique_index,
    schema_scope, // data.token = scope name, data.unary = pipeline
    schema_reference, // data.unary = linked token payload list (alias/local/target/model/ri/actions)

    // Control flow
    let_binding, // data.token = name, data.unary = expression/pipeline
    fn_def, // data.token = name, data.unary = body pipeline
    pipe_def, // data.token = name, data.unary = body pipeline

    // Introspection
    stats_call, // stats(Model); data.token = model name

    // Group-by having
    op_having, // data.unary = expression node (post-group filter)
};

/// Data payload for an AST node. Chosen to fit in 8 bytes.
pub const NodeData = union {
    /// For nodes with a single child (unary expressions, operator args, etc.)
    unary: NodeIndex,
    /// For nodes with two children (binary expressions, pipelines, etc.)
    binary: struct {
        lhs: NodeIndex,
        rhs: NodeIndex,
    },
    /// For nodes that reference a token directly (literals, identifiers).
    token: u16,
    /// Raw 4-byte payload for special cases.
    raw: u32,
};

/// A single AST node.
pub const AstNode = struct {
    tag: NodeTag,
    data: NodeData,
    /// Link to next sibling (for lists: operators, fields, args).
    next: NodeIndex = null_node,
    /// Extra data: operator token index for binary/unary expr,
    /// sort-key direction/encoding bits, type token for schema_field, etc.
    extra: u16 = 0,
};

/// The AST: a fixed-capacity array of nodes.
pub const Ast = struct {
    nodes: [max_ast_nodes]AstNode = undefined,
    node_count: u16 = 0,
    root: NodeIndex = null_node,

    /// Allocate a new node. Returns its index.
    pub fn addNode(self: *Ast, tag: NodeTag, data: NodeData) error{AstFull}!NodeIndex {
        if (self.node_count >= max_ast_nodes) return error.AstFull;
        const idx = self.node_count;
        self.nodes[idx] = .{
            .tag = tag,
            .data = data,
        };
        self.node_count += 1;
        return idx;
    }

    /// Allocate a new node with extra data and next pointer.
    pub fn addNodeFull(
        self: *Ast,
        tag: NodeTag,
        data: NodeData,
        extra: u16,
        next: NodeIndex,
    ) error{AstFull}!NodeIndex {
        if (self.node_count >= max_ast_nodes) return error.AstFull;
        const idx = self.node_count;
        self.nodes[idx] = .{
            .tag = tag,
            .data = data,
            .next = next,
            .extra = extra,
        };
        self.node_count += 1;
        return idx;
    }

    /// Get a node by index.
    pub fn getNode(self: *const Ast, idx: NodeIndex) *const AstNode {
        std.debug.assert(idx < self.node_count);
        return &self.nodes[idx];
    }

    /// Get a mutable node by index.
    pub fn getNodeMut(self: *Ast, idx: NodeIndex) *AstNode {
        std.debug.assert(idx < self.node_count);
        return &self.nodes[idx];
    }

    /// Set the next sibling of a node.
    pub fn setNext(self: *Ast, idx: NodeIndex, next: NodeIndex) void {
        self.nodes[idx].next = next;
    }

    /// Count nodes in a linked list starting at `head`.
    pub fn listLen(self: *const Ast, head: NodeIndex) u16 {
        var count: u16 = 0;
        var current = head;
        while (current != null_node) {
            count += 1;
            current = self.nodes[current].next;
        }
        return count;
    }
};

// --- Tests ---

const testing = std.testing;

test "addNode allocates sequential indices" {
    var ast = Ast{};

    const n0 = try ast.addNode(.root, .{ .unary = null_node });
    const n1 = try ast.addNode(.expr_literal, .{ .token = 42 });

    try testing.expectEqual(@as(NodeIndex, 0), n0);
    try testing.expectEqual(@as(NodeIndex, 1), n1);
    try testing.expectEqual(@as(u16, 2), ast.node_count);
}

test "getNode retrieves correct data" {
    var ast = Ast{};

    _ = try ast.addNode(.expr_literal, .{ .token = 10 });
    const node = ast.getNode(0);

    try testing.expectEqual(NodeTag.expr_literal, node.tag);
    try testing.expectEqual(@as(u16, 10), node.data.token);
    try testing.expectEqual(null_node, node.next);
}

test "linked list traversal" {
    var ast = Ast{};

    const n0 = try ast.addNode(.select_field, .{ .token = 0 });
    const n1 = try ast.addNode(.select_field, .{ .token = 1 });
    const n2 = try ast.addNode(.select_field, .{ .token = 2 });

    ast.setNext(n0, n1);
    ast.setNext(n1, n2);

    try testing.expectEqual(@as(u16, 3), ast.listLen(n0));

    // Traverse and verify.
    var current = n0;
    var count: u16 = 0;
    while (current != null_node) {
        const node = ast.getNode(current);
        try testing.expectEqual(count, node.data.token);
        current = node.next;
        count += 1;
    }
    try testing.expectEqual(@as(u16, 3), count);
}

test "addNodeFull sets extra and next" {
    var ast = Ast{};

    const n0 = try ast.addNodeFull(.sort_key, .{ .token = 5 }, 1, null_node);
    const node = ast.getNode(n0);

    try testing.expectEqual(NodeTag.sort_key, node.tag);
    try testing.expectEqual(@as(u16, 5), node.data.token);
    try testing.expectEqual(@as(u16, 1), node.extra); // desc
    try testing.expectEqual(null_node, node.next);
}

test "capacity overflow returns error" {
    var ast = Ast{};
    ast.node_count = max_ast_nodes;

    const result = ast.addNode(.root, .{ .unary = null_node });
    try testing.expectError(error.AstFull, result);
}

test "getNodeMut allows modification" {
    var ast = Ast{};

    const n0 = try ast.addNode(.expr_literal, .{ .token = 0 });
    const node = ast.getNodeMut(n0);
    node.extra = 42;

    try testing.expectEqual(@as(u16, 42), ast.getNode(n0).extra);
}

test "binary node stores two children" {
    var ast = Ast{};

    const lhs = try ast.addNode(.expr_literal, .{ .token = 1 });
    const rhs = try ast.addNode(.expr_literal, .{ .token = 2 });
    const bin = try ast.addNode(.expr_binary, .{ .binary = .{ .lhs = lhs, .rhs = rhs } });

    const node = ast.getNode(bin);
    try testing.expectEqual(lhs, node.data.binary.lhs);
    try testing.expectEqual(rhs, node.data.binary.rhs);
}

test "empty list has length zero" {
    const ast = Ast{};
    try testing.expectEqual(@as(u16, 0), ast.listLen(null_node));
}
