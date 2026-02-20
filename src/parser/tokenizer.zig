//! Query/schema tokenizer for pg2 source text.
//!
//! Responsibilities in this file:
//! - Converts source bytes into bounded token streams with line metadata.
//! - Classifies literals, identifiers, keywords, operators, and punctuation.
//! - Provides token text slicing helpers used by parser and catalog loader.
//! - Fails closed with deterministic error reporting on malformed lexemes.
const std = @import("std");

/// Token types produced by the tokenizer.
pub const TokenType = enum(u8) {
    // Literals
    integer_literal,
    float_literal,
    string_literal,
    true_literal,
    false_literal,
    null_literal,

    // Identifiers
    identifier, // snake_case
    model_name, // PascalCase (starts with uppercase)
    parameter, // $name

    // Keywords — query operators
    kw_where,
    kw_sort,
    kw_limit,
    kw_offset,
    kw_group,
    kw_unique,
    kw_delete,
    kw_insert,
    kw_update,
    kw_inspect,
    kw_asc,
    kw_desc,

    // Keywords — schema
    kw_field,
    kw_has_many,
    kw_has_one,
    kw_belongs_to,
    kw_primary_key,
    kw_not_null,
    kw_default,
    kw_index,
    kw_unique_index,
    kw_scope,
    kw_reference,
    kw_with_referential_integrity,
    kw_without_referential_integrity,
    kw_on_delete_restrict,
    kw_on_delete_cascade,
    kw_on_delete_set_null,
    kw_on_delete_set_default,
    kw_on_update_restrict,
    kw_on_update_cascade,
    kw_on_update_set_null,
    kw_on_update_set_default,

    // Keywords — control
    kw_let,
    kw_fn,
    kw_pipe,
    kw_end,

    // Keywords — logical
    kw_and,
    kw_or,
    kw_not,
    kw_in,

    // Keywords — types
    kw_bigint,
    kw_int,
    kw_float,
    kw_boolean,
    kw_string,
    kw_timestamp,

    // Aggregate names
    agg_count,
    agg_sum,
    agg_avg,
    agg_min,
    agg_max,

    // Built-in scalar functions
    fn_now,
    fn_lower,
    fn_upper,
    fn_trim,
    fn_length,
    fn_abs,
    fn_sqrt,
    fn_round,
    fn_coalesce,

    // Operators
    pipe_arrow, // |>
    equal, // =
    not_equal, // !=
    less_than, // <
    less_equal, // <=
    greater_than, // >
    greater_equal, // >=
    plus, // +
    minus, // -
    star, // *
    slash, // /
    dot, // .

    // Punctuation
    left_paren, // (
    right_paren, // )
    left_brace, // {
    right_brace, // }
    left_bracket, // [
    right_bracket, // ]
    comma, // ,
    colon, // :

    // Duration suffixes
    dur_days,
    dur_hours,
    dur_minutes,
    dur_seconds,

    // Special
    end_of_input,
    err,
};

/// A single token with position info.
pub const Token = struct {
    token_type: TokenType,
    start: u32,
    len: u16,
    line: u16,
};

/// Maximum number of tokens the tokenizer can produce.
pub const max_tokens = 4096;

/// Result of tokenization.
pub const TokenizeResult = struct {
    tokens: [max_tokens]Token,
    count: u16,
    has_error: bool,
    error_line: u16,
    error_message: ErrorMessage,

    pub const ErrorMessage = [64]u8;

    /// Get the text of a token from the original source.
    pub fn getText(self: *const TokenizeResult, idx: u16, source: []const u8) []const u8 {
        const tok = self.tokens[idx];
        return source[tok.start..][0..tok.len];
    }
};

/// Tokenize source into a fixed-capacity token array.
pub fn tokenize(source: []const u8) TokenizeResult {
    var result = TokenizeResult{
        .tokens = undefined,
        .count = 0,
        .has_error = false,
        .error_line = 0,
        .error_message = std.mem.zeroes(TokenizeResult.ErrorMessage),
    };

    var pos: u32 = 0;
    var line: u16 = 1;

    while (pos < source.len) {
        // Skip whitespace.
        if (source[pos] == ' ' or source[pos] == '\t' or source[pos] == '\r') {
            pos += 1;
            continue;
        }
        if (source[pos] == '\n') {
            pos += 1;
            line += 1;
            continue;
        }

        // Skip comments (-- to end of line).
        if (pos + 1 < source.len and source[pos] == '-' and source[pos + 1] == '-') {
            pos += 2;
            while (pos < source.len and source[pos] != '\n') : (pos += 1) {}
            continue;
        }

        // Check capacity.
        if (result.count >= max_tokens) {
            setError(&result, line, "too many tokens");
            break;
        }

        const start = pos;

        // String literal.
        if (source[pos] == '"') {
            pos += 1;
            while (pos < source.len and source[pos] != '"' and source[pos] != '\n') : (pos += 1) {}
            if (pos >= source.len or source[pos] != '"') {
                setError(&result, line, "unterminated string");
                break;
            }
            pos += 1; // consume closing quote
            addToken(&result, .string_literal, start, pos - start, line);
            continue;
        }

        // Parameter ($name).
        if (source[pos] == '$') {
            pos += 1;
            while (pos < source.len and (isIdentChar(source[pos]))) : (pos += 1) {}
            addToken(&result, .parameter, start, pos - start, line);
            continue;
        }

        // Number literal.
        if (isDigit(source[pos])) {
            var is_float = false;
            while (pos < source.len and isDigit(source[pos])) : (pos += 1) {}
            if (pos < source.len and source[pos] == '.') {
                // Check next char is digit (not a method call like 123.foo).
                if (pos + 1 < source.len and isDigit(source[pos + 1])) {
                    is_float = true;
                    pos += 1; // skip dot
                    while (pos < source.len and isDigit(source[pos])) : (pos += 1) {}
                }
            }
            const tok_type: TokenType = if (is_float) .float_literal else .integer_literal;
            addToken(&result, tok_type, start, pos - start, line);
            continue;
        }

        // Identifier or keyword.
        if (isIdentStart(source[pos])) {
            while (pos < source.len and isIdentChar(source[pos])) : (pos += 1) {}
            const text = source[start..pos];
            const tok_type = classifyWord(text, source[start] >= 'A' and source[start] <= 'Z');
            addToken(&result, tok_type, start, pos - start, line);
            continue;
        }

        // Two-character operators.
        if (pos + 1 < source.len) {
            const two = source[pos..][0..2];
            if (std.mem.eql(u8, two, "|>")) {
                addToken(&result, .pipe_arrow, start, 2, line);
                pos += 2;
                continue;
            }
            if (std.mem.eql(u8, two, "!=")) {
                addToken(&result, .not_equal, start, 2, line);
                pos += 2;
                continue;
            }
            if (std.mem.eql(u8, two, "<=")) {
                addToken(&result, .less_equal, start, 2, line);
                pos += 2;
                continue;
            }
            if (std.mem.eql(u8, two, ">=")) {
                addToken(&result, .greater_equal, start, 2, line);
                pos += 2;
                continue;
            }
        }

        // Single-character operators and punctuation.
        const tok_type: ?TokenType = switch (source[pos]) {
            '=' => .equal,
            '<' => .less_than,
            '>' => .greater_than,
            '+' => .plus,
            '-' => .minus,
            '*' => .star,
            '/' => .slash,
            '.' => .dot,
            '(' => .left_paren,
            ')' => .right_paren,
            '{' => .left_brace,
            '}' => .right_brace,
            '[' => .left_bracket,
            ']' => .right_bracket,
            ',' => .comma,
            ':' => .colon,
            else => null,
        };

        if (tok_type) |tt| {
            addToken(&result, tt, start, 1, line);
            pos += 1;
            continue;
        }

        // Unknown character.
        setError(&result, line, "unexpected character");
        break;
    }

    // Add end-of-input sentinel.
    if (result.count < max_tokens) {
        addToken(&result, .end_of_input, pos, 0, line);
    }

    return result;
}

fn addToken(result: *TokenizeResult, tok_type: TokenType, start: u32, length: anytype, line: u16) void {
    if (result.count >= max_tokens) return;
    result.tokens[result.count] = .{
        .token_type = tok_type,
        .start = start,
        .len = @intCast(length),
        .line = line,
    };
    result.count += 1;
}

fn setError(result: *TokenizeResult, line: u16, msg: []const u8) void {
    result.has_error = true;
    result.error_line = line;
    const copy_len = @min(msg.len, result.error_message.len);
    @memcpy(result.error_message[0..copy_len], msg[0..copy_len]);
}

fn isDigit(c: u8) bool {
    return c >= '0' and c <= '9';
}

fn isIdentStart(c: u8) bool {
    return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_';
}

fn isIdentChar(c: u8) bool {
    return isIdentStart(c) or isDigit(c);
}

/// Classify an identifier word as a keyword, aggregate, function, or plain identifier.
fn classifyWord(text: []const u8, starts_upper: bool) TokenType {
    // Check keywords first.
    const kw_table = [_]struct { word: []const u8, tok: TokenType }{
        .{ .word = "where", .tok = .kw_where },
        .{ .word = "sort", .tok = .kw_sort },
        .{ .word = "limit", .tok = .kw_limit },
        .{ .word = "offset", .tok = .kw_offset },
        .{ .word = "group", .tok = .kw_group },
        .{ .word = "unique", .tok = .kw_unique },
        .{ .word = "delete", .tok = .kw_delete },
        .{ .word = "insert", .tok = .kw_insert },
        .{ .word = "update", .tok = .kw_update },
        .{ .word = "inspect", .tok = .kw_inspect },
        .{ .word = "asc", .tok = .kw_asc },
        .{ .word = "desc", .tok = .kw_desc },
        .{ .word = "field", .tok = .kw_field },
        .{ .word = "hasMany", .tok = .kw_has_many },
        .{ .word = "hasOne", .tok = .kw_has_one },
        .{ .word = "belongsTo", .tok = .kw_belongs_to },
        .{ .word = "primaryKey", .tok = .kw_primary_key },
        .{ .word = "notNull", .tok = .kw_not_null },
        .{ .word = "default", .tok = .kw_default },
        .{ .word = "index", .tok = .kw_index },
        .{ .word = "uniqueIndex", .tok = .kw_unique_index },
        .{ .word = "scope", .tok = .kw_scope },
        .{ .word = "reference", .tok = .kw_reference },
        .{ .word = "withReferentialIntegrity", .tok = .kw_with_referential_integrity },
        .{ .word = "withoutReferentialIntegrity", .tok = .kw_without_referential_integrity },
        .{ .word = "onDeleteRestrict", .tok = .kw_on_delete_restrict },
        .{ .word = "onDeleteCascade", .tok = .kw_on_delete_cascade },
        .{ .word = "onDeleteSetNull", .tok = .kw_on_delete_set_null },
        .{ .word = "onDeleteSetDefault", .tok = .kw_on_delete_set_default },
        .{ .word = "onUpdateRestrict", .tok = .kw_on_update_restrict },
        .{ .word = "onUpdateCascade", .tok = .kw_on_update_cascade },
        .{ .word = "onUpdateSetNull", .tok = .kw_on_update_set_null },
        .{ .word = "onUpdateSetDefault", .tok = .kw_on_update_set_default },
        .{ .word = "let", .tok = .kw_let },
        .{ .word = "fn", .tok = .kw_fn },
        .{ .word = "pipe", .tok = .kw_pipe },
        .{ .word = "end", .tok = .kw_end },
        .{ .word = "and", .tok = .kw_and },
        .{ .word = "or", .tok = .kw_or },
        .{ .word = "not", .tok = .kw_not },
        .{ .word = "in", .tok = .kw_in },
        .{ .word = "true", .tok = .true_literal },
        .{ .word = "false", .tok = .false_literal },
        .{ .word = "null", .tok = .null_literal },
        .{ .word = "bigint", .tok = .kw_bigint },
        .{ .word = "int", .tok = .kw_int },
        .{ .word = "float", .tok = .kw_float },
        .{ .word = "boolean", .tok = .kw_boolean },
        .{ .word = "string", .tok = .kw_string },
        .{ .word = "timestamp", .tok = .kw_timestamp },
        // Aggregates
        .{ .word = "count", .tok = .agg_count },
        .{ .word = "sum", .tok = .agg_sum },
        .{ .word = "avg", .tok = .agg_avg },
        .{ .word = "min", .tok = .agg_min },
        .{ .word = "max", .tok = .agg_max },
        // Built-in functions
        .{ .word = "now", .tok = .fn_now },
        .{ .word = "lower", .tok = .fn_lower },
        .{ .word = "upper", .tok = .fn_upper },
        .{ .word = "trim", .tok = .fn_trim },
        .{ .word = "length", .tok = .fn_length },
        .{ .word = "abs", .tok = .fn_abs },
        .{ .word = "sqrt", .tok = .fn_sqrt },
        .{ .word = "round", .tok = .fn_round },
        .{ .word = "coalesce", .tok = .fn_coalesce },
        // Duration suffixes
        .{ .word = "days", .tok = .dur_days },
        .{ .word = "hours", .tok = .dur_hours },
        .{ .word = "minutes", .tok = .dur_minutes },
        .{ .word = "seconds", .tok = .dur_seconds },
    };

    for (kw_table) |entry| {
        if (std.mem.eql(u8, text, entry.word)) return entry.tok;
    }

    // If starts with uppercase, it's a model name.
    if (starts_upper) return .model_name;

    return .identifier;
}

// --- Tests ---

const testing = std.testing;

test "empty source produces end_of_input" {
    const result = tokenize("");
    try testing.expectEqual(@as(u16, 1), result.count);
    try testing.expectEqual(TokenType.end_of_input, result.tokens[0].token_type);
    try testing.expect(!result.has_error);
}

test "integer literal" {
    const result = tokenize("42");
    try testing.expectEqual(@as(u16, 2), result.count);
    try testing.expectEqual(TokenType.integer_literal, result.tokens[0].token_type);
    try testing.expectEqualSlices(u8, "42", result.getText(0, "42"));
}

test "float literal" {
    const source = "3.14";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.float_literal, result.tokens[0].token_type);
    try testing.expectEqualSlices(u8, "3.14", result.getText(0, source));
}

test "string literal" {
    const source =
        \\"hello world"
    ;
    const result = tokenize(source);
    try testing.expectEqual(TokenType.string_literal, result.tokens[0].token_type);
}

test "unterminated string" {
    const source =
        \\"hello
    ;
    const result = tokenize(source);
    try testing.expect(result.has_error);
}

test "keywords recognized" {
    const source = "where sort limit offset group let fn";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.kw_where, result.tokens[0].token_type);
    try testing.expectEqual(TokenType.kw_sort, result.tokens[1].token_type);
    try testing.expectEqual(TokenType.kw_limit, result.tokens[2].token_type);
    try testing.expectEqual(TokenType.kw_offset, result.tokens[3].token_type);
    try testing.expectEqual(TokenType.kw_group, result.tokens[4].token_type);
    try testing.expectEqual(TokenType.kw_let, result.tokens[5].token_type);
    try testing.expectEqual(TokenType.kw_fn, result.tokens[6].token_type);
}

test "model name starts uppercase" {
    const source = "User";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.model_name, result.tokens[0].token_type);
}

test "parameter token" {
    const source = "$user_id";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.parameter, result.tokens[0].token_type);
}

test "operators" {
    const source = "|> = != <= >= < > + - * /";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.pipe_arrow, result.tokens[0].token_type);
    try testing.expectEqual(TokenType.equal, result.tokens[1].token_type);
    try testing.expectEqual(TokenType.not_equal, result.tokens[2].token_type);
    try testing.expectEqual(TokenType.less_equal, result.tokens[3].token_type);
    try testing.expectEqual(TokenType.greater_equal, result.tokens[4].token_type);
    try testing.expectEqual(TokenType.less_than, result.tokens[5].token_type);
    try testing.expectEqual(TokenType.greater_than, result.tokens[6].token_type);
    try testing.expectEqual(TokenType.plus, result.tokens[7].token_type);
    try testing.expectEqual(TokenType.minus, result.tokens[8].token_type);
    try testing.expectEqual(TokenType.star, result.tokens[9].token_type);
    try testing.expectEqual(TokenType.slash, result.tokens[10].token_type);
}

test "punctuation" {
    const source = "{}()[],:";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.left_brace, result.tokens[0].token_type);
    try testing.expectEqual(TokenType.right_brace, result.tokens[1].token_type);
    try testing.expectEqual(TokenType.left_paren, result.tokens[2].token_type);
    try testing.expectEqual(TokenType.right_paren, result.tokens[3].token_type);
    try testing.expectEqual(TokenType.left_bracket, result.tokens[4].token_type);
    try testing.expectEqual(TokenType.right_bracket, result.tokens[5].token_type);
    try testing.expectEqual(TokenType.comma, result.tokens[6].token_type);
    try testing.expectEqual(TokenType.colon, result.tokens[7].token_type);
}

test "comments are skipped" {
    const source = "a -- this is a comment\nb";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.identifier, result.tokens[0].token_type);
    try testing.expectEqual(TokenType.identifier, result.tokens[1].token_type);
    try testing.expectEqualSlices(u8, "b", result.getText(1, source));
}

test "multiline tokenization" {
    const source = "User\n  |> where(active = true)";
    const result = tokenize(source);
    try testing.expectEqual(@as(u16, 1), result.tokens[0].line);
    // |> is on line 2
    try testing.expectEqual(@as(u16, 2), result.tokens[1].line);
}

test "boolean and null literals" {
    const source = "true false null";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.true_literal, result.tokens[0].token_type);
    try testing.expectEqual(TokenType.false_literal, result.tokens[1].token_type);
    try testing.expectEqual(TokenType.null_literal, result.tokens[2].token_type);
}

test "aggregate names" {
    const source = "count sum avg min max";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.agg_count, result.tokens[0].token_type);
    try testing.expectEqual(TokenType.agg_sum, result.tokens[1].token_type);
    try testing.expectEqual(TokenType.agg_avg, result.tokens[2].token_type);
    try testing.expectEqual(TokenType.agg_min, result.tokens[3].token_type);
    try testing.expectEqual(TokenType.agg_max, result.tokens[4].token_type);
}

test "built-in function names" {
    const source = "lower upper trim length coalesce";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.fn_lower, result.tokens[0].token_type);
    try testing.expectEqual(TokenType.fn_upper, result.tokens[1].token_type);
    try testing.expectEqual(TokenType.fn_trim, result.tokens[2].token_type);
    try testing.expectEqual(TokenType.fn_length, result.tokens[3].token_type);
    try testing.expectEqual(TokenType.fn_coalesce, result.tokens[4].token_type);
}

test "duration suffixes" {
    const source = "days hours minutes seconds";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.dur_days, result.tokens[0].token_type);
    try testing.expectEqual(TokenType.dur_hours, result.tokens[1].token_type);
    try testing.expectEqual(TokenType.dur_minutes, result.tokens[2].token_type);
    try testing.expectEqual(TokenType.dur_seconds, result.tokens[3].token_type);
}

test "dot operator" {
    const source = "User.email_index";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.model_name, result.tokens[0].token_type);
    try testing.expectEqual(TokenType.dot, result.tokens[1].token_type);
    try testing.expectEqual(TokenType.identifier, result.tokens[2].token_type);
}

test "full pipeline tokenizes correctly" {
    const source = "User |> where(active = true) |> sort(name asc) |> limit(10) { id email }";
    const result = tokenize(source);
    try testing.expect(!result.has_error);
    // User, |>, where, (, active, =, true, ), |>, sort, (, name, asc, ), |>, limit, (, 10, ), {, id, email, }, EOF = 24
    try testing.expectEqual(@as(u16, 24), result.count);
}

test "schema definition tokenizes" {
    const source =
        \\User {
        \\  field id bigint primaryKey
        \\  field email string notNull
        \\  hasMany posts
        \\}
    ;
    const result = tokenize(source);
    try testing.expect(!result.has_error);
    try testing.expectEqual(TokenType.model_name, result.tokens[0].token_type);
    try testing.expectEqual(TokenType.left_brace, result.tokens[1].token_type);
    try testing.expectEqual(TokenType.kw_field, result.tokens[2].token_type);
}

test "logical operators" {
    const source = "and or not in";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.kw_and, result.tokens[0].token_type);
    try testing.expectEqual(TokenType.kw_or, result.tokens[1].token_type);
    try testing.expectEqual(TokenType.kw_not, result.tokens[2].token_type);
    try testing.expectEqual(TokenType.kw_in, result.tokens[3].token_type);
}

test "type keywords" {
    const source = "bigint int float boolean string timestamp";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.kw_bigint, result.tokens[0].token_type);
    try testing.expectEqual(TokenType.kw_int, result.tokens[1].token_type);
    try testing.expectEqual(TokenType.kw_float, result.tokens[2].token_type);
    try testing.expectEqual(TokenType.kw_boolean, result.tokens[3].token_type);
    try testing.expectEqual(TokenType.kw_string, result.tokens[4].token_type);
    try testing.expectEqual(TokenType.kw_timestamp, result.tokens[5].token_type);
}

test "reference and RI keywords" {
    const source =
        "reference withReferentialIntegrity withoutReferentialIntegrity onDeleteRestrict onUpdateCascade";
    const result = tokenize(source);
    try testing.expectEqual(TokenType.kw_reference, result.tokens[0].token_type);
    try testing.expectEqual(TokenType.kw_with_referential_integrity, result.tokens[1].token_type);
    try testing.expectEqual(TokenType.kw_without_referential_integrity, result.tokens[2].token_type);
    try testing.expectEqual(TokenType.kw_on_delete_restrict, result.tokens[3].token_type);
    try testing.expectEqual(TokenType.kw_on_update_cascade, result.tokens[4].token_type);
}
