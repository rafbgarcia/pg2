//! Shared parser result/error types used across parser submodules.
const ast_mod = @import("ast.zig");

const NodeIndex = ast_mod.NodeIndex;

pub const ParseError = error{
    AstFull,
    UnexpectedToken,
    NestingTooDeep,
    StackOverflow,
    StackUnderflow,
    MismatchedParentheses,
};

/// Returned by internal parse functions: node index + next token position.
pub const NodeResult = struct {
    node: NodeIndex,
    pos: u16,
};

/// Maximum nesting depth for selection sets.
pub const max_nesting_depth: u16 = 16;
