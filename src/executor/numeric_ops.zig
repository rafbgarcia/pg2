//! Numeric arithmetic, overflow-safe operations, type coercion, and comparison.
//!
//! Extracted from filter.zig — provides the numeric evaluation primitives
//! used by the stack-based expression evaluator.
const std = @import("std");
const row_mod = @import("../storage/row.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const filter = @import("filter.zig");

const EvalError = filter.EvalError;
const Value = row_mod.Value;
const TokenType = tokenizer_mod.TokenType;

pub const ArithOp = enum { add, sub, mul, div };

pub fn applyArithmetic(lhs: Value, rhs: Value, op: ArithOp) EvalError!Value {
    // Integer arithmetic.
    if (lhs == .i8 and rhs == .i8) {
        const a = lhs.i8;
        const b = rhs.i8;
        return Value{ .i8 = try applySignedArithmetic(i8, a, b, op) };
    }
    if (lhs == .i16 and rhs == .i16) {
        const a = lhs.i16;
        const b = rhs.i16;
        return Value{ .i16 = try applySignedArithmetic(i16, a, b, op) };
    }
    if (lhs == .i32 and rhs == .i32) {
        const a = lhs.i32;
        const b = rhs.i32;
        return Value{ .i32 = try applySignedArithmetic(i32, a, b, op) };
    }
    if (lhs == .i64 and rhs == .i64) {
        const a = lhs.i64;
        const b = rhs.i64;
        return Value{ .i64 = try applySignedArithmetic(i64, a, b, op) };
    }
    if (lhs == .u8 and rhs == .u8) {
        const a = lhs.u8;
        const b = rhs.u8;
        return Value{ .u8 = try applyUnsignedArithmetic(u8, a, b, op) };
    }
    if (lhs == .u16 and rhs == .u16) {
        const a = lhs.u16;
        const b = rhs.u16;
        return Value{ .u16 = try applyUnsignedArithmetic(u16, a, b, op) };
    }
    if (lhs == .u32 and rhs == .u32) {
        const a = lhs.u32;
        const b = rhs.u32;
        return Value{ .u32 = try applyUnsignedArithmetic(u32, a, b, op) };
    }
    if (lhs == .u64 and rhs == .u64) {
        const a = lhs.u64;
        const b = rhs.u64;
        return Value{ .u64 = try applyUnsignedArithmetic(u64, a, b, op) };
    }
    if (isIntegerValue(lhs) and isIntegerValue(rhs)) {
        if (isSignedIntegerValue(lhs) or isSignedIntegerValue(rhs)) {
            const a = try toI64Integer(lhs);
            const b = try toI64Integer(rhs);
            return Value{ .i64 = try applySignedArithmetic(i64, a, b, op) };
        }
        const a = try toU64Integer(lhs);
        const b = try toU64Integer(rhs);
        return Value{ .u64 = try applyUnsignedArithmetic(u64, a, b, op) };
    }

    // Float arithmetic (promote if mixed).
    const a = toFloat(lhs) orelse return error.TypeMismatch;
    const b = toFloat(rhs) orelse return error.TypeMismatch;
    return Value{ .f64 = switch (op) {
        .add => a + b,
        .sub => a - b,
        .mul => a * b,
        .div => blk: {
            if (b == 0.0) return error.DivisionByZero;
            break :blk a / b;
        },
    } };
}

fn applySignedArithmetic(comptime T: type, a: T, b: T, op: ArithOp) EvalError!T {
    return switch (op) {
        .add => std.math.add(T, a, b) catch return error.NumericOverflow,
        .sub => std.math.sub(T, a, b) catch return error.NumericOverflow,
        .mul => std.math.mul(T, a, b) catch return error.NumericOverflow,
        .div => blk: {
            if (b == 0) return error.DivisionByZero;
            if (a == std.math.minInt(T) and b == -1) return error.NumericOverflow;
            break :blk @divTrunc(a, b);
        },
    };
}

fn applyUnsignedArithmetic(comptime T: type, a: T, b: T, op: ArithOp) EvalError!T {
    return switch (op) {
        .add => std.math.add(T, a, b) catch return error.NumericOverflow,
        .sub => std.math.sub(T, a, b) catch return error.NumericOverflow,
        .mul => std.math.mul(T, a, b) catch return error.NumericOverflow,
        .div => blk: {
            if (b == 0) return error.DivisionByZero;
            break :blk @divTrunc(a, b);
        },
    };
}

pub fn isIntegerValue(v: Value) bool {
    return switch (v) {
        .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64 => true,
        else => false,
    };
}

pub fn isSignedIntegerValue(v: Value) bool {
    return switch (v) {
        .i8, .i16, .i32, .i64 => true,
        else => false,
    };
}

pub fn toI64Integer(v: Value) EvalError!i64 {
    return switch (v) {
        .i8 => |x| x,
        .i16 => |x| x,
        .i32 => |x| x,
        .i64 => |x| x,
        .u8 => |x| x,
        .u16 => |x| x,
        .u32 => |x| x,
        .u64 => |x| std.math.cast(i64, x) orelse return error.NumericOverflow,
        else => error.TypeMismatch,
    };
}

pub fn toU64Integer(v: Value) EvalError!u64 {
    return switch (v) {
        .i8 => |x| std.math.cast(u64, x) orelse return error.NumericOverflow,
        .i16 => |x| std.math.cast(u64, x) orelse return error.NumericOverflow,
        .i32 => |x| std.math.cast(u64, x) orelse return error.NumericOverflow,
        .i64 => |x| std.math.cast(u64, x) orelse return error.NumericOverflow,
        .u8 => |x| x,
        .u16 => |x| x,
        .u32 => |x| x,
        .u64 => |x| x,
        else => error.TypeMismatch,
    };
}

pub fn toFloat(v: Value) ?f64 {
    return switch (v) {
        .f64 => |f| f,
        .i8 => |i| @as(f64, @floatFromInt(i)),
        .i16 => |i| @as(f64, @floatFromInt(i)),
        .i64 => |i| @as(f64, @floatFromInt(i)),
        .i32 => |i| @as(f64, @floatFromInt(i)),
        .u8 => |i| @as(f64, @floatFromInt(i)),
        .u16 => |i| @as(f64, @floatFromInt(i)),
        .u32 => |i| @as(f64, @floatFromInt(i)),
        .u64 => |i| @as(f64, @floatFromInt(i)),
        else => null,
    };
}

fn toSigned(v: Value) ?i128 {
    return switch (v) {
        .i8 => |x| x,
        .i16 => |x| x,
        .i32 => |x| x,
        .i64 => |x| x,
        else => null,
    };
}

fn toUnsigned(v: Value) ?u128 {
    return switch (v) {
        .u8 => |x| x,
        .u16 => |x| x,
        .u32 => |x| x,
        .u64 => |x| x,
        else => null,
    };
}

pub fn numericOrder(lhs: Value, rhs: Value) ?std.math.Order {
    if (lhs == .f64 or rhs == .f64) {
        const lf = toFloat(lhs) orelse return null;
        const rf = toFloat(rhs) orelse return null;
        return std.math.order(lf, rf);
    }

    if (toSigned(lhs)) |ls| {
        if (toSigned(rhs)) |rs| return std.math.order(ls, rs);
        if (toUnsigned(rhs)) |ru| {
            if (ls < 0) return .lt;
            return std.math.order(@as(u128, @intCast(ls)), ru);
        }
        return null;
    }
    if (toUnsigned(lhs)) |lu| {
        if (toUnsigned(rhs)) |ru| return std.math.order(lu, ru);
        if (toSigned(rhs)) |rs| {
            if (rs < 0) return .gt;
            return std.math.order(lu, @as(u128, @intCast(rs)));
        }
        return null;
    }
    return null;
}

pub const CmpOp = enum { eq, neq, lt, lte, gt, gte };

pub fn applyComparison(lhs: Value, rhs: Value, op: CmpOp) EvalError!Value {
    if (lhs == .null_value or rhs == .null_value) {
        return switch (op) {
            .eq => Value{ .bool = lhs == .null_value and rhs == .null_value },
            .neq => Value{
                .bool = (lhs == .null_value) != (rhs == .null_value),
            },
            else => Value{ .null_value = {} },
        };
    }

    const ord = if (isNumericValue(lhs) and isNumericValue(rhs))
        (numericOrder(lhs, rhs) orelse return error.TypeMismatch)
    else blk: {
        const lhs_type = lhs.columnType() orelse return error.TypeMismatch;
        const rhs_type = rhs.columnType() orelse return error.TypeMismatch;
        if (lhs_type != rhs_type) return error.TypeMismatch;
        break :blk row_mod.compareValues(lhs, rhs);
    };
    return Value{ .bool = switch (op) {
        .eq => ord == .eq,
        .neq => ord != .eq,
        .lt => ord == .lt,
        .lte => ord == .lt or ord == .eq,
        .gt => ord == .gt,
        .gte => ord == .gt or ord == .eq,
    } };
}

pub fn isNumericValue(v: Value) bool {
    return switch (v) {
        .i8, .i16, .i32, .i64, .u8, .u16, .u32, .u64, .f64 => true,
        else => false,
    };
}

pub fn membershipElementEquals(needle: Value, element: Value) EvalError!bool {
    if (isNumericValue(needle) and isNumericValue(element)) {
        const ord = numericOrder(needle, element) orelse return error.TypeMismatch;
        return ord == .eq;
    }

    if (needle.columnType()) |needle_type| {
        const element_type = element.columnType() orelse return error.TypeMismatch;
        if (needle_type != element_type) return error.TypeMismatch;
        return row_mod.compareValues(needle, element) == .eq;
    }

    return error.TypeMismatch;
}

/// Apply a unary operator.
pub fn applyUnaryOp(operand: Value, op: TokenType) EvalError!Value {
    if (operand == .null_value) return Value{ .null_value = {} };

    return switch (op) {
        .bang => blk: {
            if (operand != .bool) return error.TypeMismatch;
            break :blk Value{ .bool = !operand.bool };
        },
        .minus => blk: {
            if (operand == .i8) {
                if (operand.i8 == std.math.minInt(i8)) return error.NumericOverflow;
                break :blk Value{ .i8 = -operand.i8 };
            }
            if (operand == .i16) {
                if (operand.i16 == std.math.minInt(i16)) return error.NumericOverflow;
                break :blk Value{ .i16 = -operand.i16 };
            }
            if (operand == .i64) {
                if (operand.i64 == std.math.minInt(i64)) return error.NumericOverflow;
                break :blk Value{ .i64 = -operand.i64 };
            }
            if (operand == .i32) {
                if (operand.i32 == std.math.minInt(i32)) return error.NumericOverflow;
                break :blk Value{ .i32 = -operand.i32 };
            }
            if (operand == .u8) break :blk Value{ .i64 = -@as(i64, operand.u8) };
            if (operand == .u16) break :blk Value{ .i64 = -@as(i64, operand.u16) };
            if (operand == .u32) break :blk Value{ .i64 = -@as(i64, operand.u32) };
            if (operand == .u64) break :blk Value{ .i64 = try negateUnsignedIntoI64(operand.u64) };
            if (operand == .f64) break :blk Value{ .f64 = -operand.f64 };
            return error.TypeMismatch;
        },
        else => error.TypeMismatch,
    };
}

fn negateUnsignedIntoI64(magnitude: u64) EvalError!i64 {
    const signed_min_magnitude: u64 = @as(u64, std.math.maxInt(i64)) + 1;
    if (magnitude > signed_min_magnitude) return error.NumericOverflow;
    if (magnitude == signed_min_magnitude) return std.math.minInt(i64);
    const narrowed = std.math.cast(i64, magnitude) orelse return error.NumericOverflow;
    return -narrowed;
}
