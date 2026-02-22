//! Scalar built-in function dispatch and string helpers.
//!
//! Extracted from filter.zig — provides the built-in function implementations
//! (abs, sqrt, round, length, coalesce, lower, upper, trim) used by the
//! stack-based expression evaluator.
const std = @import("std");
const row_mod = @import("../storage/row.zig");
const scan_mod = @import("scan.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const filter = @import("filter.zig");
const numeric_ops = @import("numeric_ops.zig");

const EvalError = filter.EvalError;
const EvalContext = filter.EvalContext;
const Value = row_mod.Value;
const TokenType = tokenizer_mod.TokenType;
const max_string_result_bytes = filter.max_string_result_bytes;
const toFloat = numeric_ops.toFloat;

/// Apply a built-in scalar function.
pub fn applyBuiltinFunction(
    fn_type: TokenType,
    args: []const Value,
    eval_ctx: *const EvalContext,
) EvalError!Value {
    return switch (fn_type) {
        .fn_abs => blk: {
            if (args.len != 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            if (v == .i8) {
                if (v.i8 == std.math.minInt(i8)) return error.NumericOverflow;
                break :blk Value{ .i8 = if (v.i8 < 0) -v.i8 else v.i8 };
            }
            if (v == .i16) {
                if (v.i16 == std.math.minInt(i16)) return error.NumericOverflow;
                break :blk Value{ .i16 = if (v.i16 < 0) -v.i16 else v.i16 };
            }
            if (v == .i64) {
                if (v.i64 == std.math.minInt(i64)) return error.NumericOverflow;
                break :blk Value{
                    .i64 = if (v.i64 < 0) -v.i64 else v.i64,
                };
            }
            if (v == .i32) {
                if (v.i32 == std.math.minInt(i32)) return error.NumericOverflow;
                break :blk Value{
                    .i32 = if (v.i32 < 0) -v.i32 else v.i32,
                };
            }
            if (v == .u8 or v == .u16 or v == .u32 or v == .u64) break :blk v;
            if (v == .f64) break :blk Value{ .f64 = @abs(v.f64) };
            return error.TypeMismatch;
        },
        .fn_sqrt => blk: {
            if (args.len != 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            const f = toFloat(v) orelse return error.TypeMismatch;
            if (f < 0) return error.NumericDomain;
            break :blk Value{ .f64 = @sqrt(f) };
        },
        .fn_round => blk: {
            if (args.len != 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            const f = toFloat(v) orelse return error.TypeMismatch;
            break :blk Value{ .f64 = roundToNearestEven(f) };
        },
        .fn_length => blk: {
            if (args.len != 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            if (v != .string) return error.TypeMismatch;
            break :blk Value{ .i64 = @intCast(v.string.len) };
        },
        .fn_coalesce => blk: {
            if (args.len == 0) return error.TypeMismatch;
            for (args) |arg| {
                if (arg != .null_value) break :blk arg;
            }
            break :blk Value{ .null_value = {} };
        },
        .fn_lower => blk: {
            if (args.len != 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            if (v != .string) return error.TypeMismatch;
            if (!std.unicode.utf8ValidateSlice(v.string)) return error.TypeMismatch;
            break :blk Value{
                .string = try asciiLowerString(v.string, eval_ctx.string_arena),
            };
        },
        .fn_upper => blk: {
            if (args.len != 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            if (v != .string) return error.TypeMismatch;
            if (!std.unicode.utf8ValidateSlice(v.string)) return error.TypeMismatch;
            break :blk Value{
                .string = try asciiUpperString(v.string, eval_ctx.string_arena),
            };
        },
        .fn_trim => blk: {
            if (args.len != 1) return error.TypeMismatch;
            const v = args[0];
            if (v == .null_value) break :blk Value{ .null_value = {} };
            if (v != .string) return error.TypeMismatch;
            if (!std.unicode.utf8ValidateSlice(v.string)) return error.TypeMismatch;
            break :blk Value{
                .string = try trimAsciiSpaces(v.string, eval_ctx.string_arena),
            };
        },
        else => error.UnknownFunction,
    };
}

fn copyToArena(
    input: []const u8,
    string_arena: ?*scan_mod.StringArena,
) EvalError![]const u8 {
    if (input.len == 0) return "";
    if (input.len > max_string_result_bytes) return error.NumericOverflow;
    const arena = string_arena orelse return error.TypeMismatch;
    const start = arena.startString();
    arena.appendChunk(input) catch return error.NumericOverflow;
    return arena.finishString(start);
}

fn asciiLowerString(
    input: []const u8,
    string_arena: ?*scan_mod.StringArena,
) EvalError![]const u8 {
    var changed = false;
    for (input) |byte| {
        if (byte >= 'A' and byte <= 'Z') {
            changed = true;
            break;
        }
    }
    if (!changed) return input;

    if (input.len > max_string_result_bytes) return error.NumericOverflow;
    var lowered: [max_string_result_bytes]u8 = undefined;
    for (input, 0..) |byte, i| lowered[i] = std.ascii.toLower(byte);
    return copyToArena(lowered[0..input.len], string_arena);
}

fn asciiUpperString(
    input: []const u8,
    string_arena: ?*scan_mod.StringArena,
) EvalError![]const u8 {
    var changed = false;
    for (input) |byte| {
        if (byte >= 'a' and byte <= 'z') {
            changed = true;
            break;
        }
    }
    if (!changed) return input;

    if (input.len > max_string_result_bytes) return error.NumericOverflow;
    var uppered: [max_string_result_bytes]u8 = undefined;
    for (input, 0..) |byte, i| uppered[i] = std.ascii.toUpper(byte);
    return copyToArena(uppered[0..input.len], string_arena);
}

fn trimAsciiSpaces(
    input: []const u8,
    string_arena: ?*scan_mod.StringArena,
) EvalError![]const u8 {
    var start: usize = 0;
    var end: usize = input.len;
    while (start < end and input[start] == ' ') : (start += 1) {}
    while (end > start and input[end - 1] == ' ') : (end -= 1) {}

    if (start == 0 and end == input.len) return input;
    return copyToArena(input[start..end], string_arena);
}

fn roundToNearestEven(v: f64) f64 {
    if (!std.math.isFinite(v)) return v;
    const lower = @floor(v);
    const delta = v - lower;
    if (delta < 0.5) return lower;
    if (delta > 0.5) return lower + 1.0;

    const lower_mod_two = @rem(lower, 2.0);
    if (lower_mod_two == 0.0) return lower;
    return lower + 1.0;
}
