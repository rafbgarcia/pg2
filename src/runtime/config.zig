const std = @import("std");

pub const default_memory_bytes: usize = 512 * 1024 * 1024;

pub const ConfigError = error{
    InvalidMemoryValue,
    Overflow,
};

/// Parse a memory budget string for --memory.
///
/// Accepted examples:
/// - "536870912"
/// - "512MiB"
/// - "512MB"
/// - "1GiB"
pub fn parseMemoryBytes(raw: []const u8) ConfigError!usize {
    const input = std.mem.trim(u8, raw, " \t\r\n");
    if (input.len == 0) return error.InvalidMemoryValue;

    var digits_len: usize = 0;
    while (digits_len < input.len and std.ascii.isDigit(input[digits_len])) {
        digits_len += 1;
    }
    if (digits_len == 0) return error.InvalidMemoryValue;

    const number = std.fmt.parseInt(usize, input[0..digits_len], 10) catch
        return error.InvalidMemoryValue;
    if (number == 0) return error.InvalidMemoryValue;

    const suffix_raw = std.mem.trim(u8, input[digits_len..], " \t");
    if (suffix_raw.len == 0) return number;

    var suffix_buf: [8]u8 = undefined;
    if (suffix_raw.len > suffix_buf.len) return error.InvalidMemoryValue;
    for (suffix_raw, 0..) |ch, i| {
        suffix_buf[i] = std.ascii.toLower(ch);
    }
    const suffix = suffix_buf[0..suffix_raw.len];

    const multiplier: usize = if (std.mem.eql(u8, suffix, "b"))
        1
    else if (std.mem.eql(u8, suffix, "k") or
        std.mem.eql(u8, suffix, "kb"))
        1000
    else if (std.mem.eql(u8, suffix, "kib"))
        1024
    else if (std.mem.eql(u8, suffix, "m") or
        std.mem.eql(u8, suffix, "mb"))
        1000 * 1000
    else if (std.mem.eql(u8, suffix, "mib"))
        1024 * 1024
    else if (std.mem.eql(u8, suffix, "g") or
        std.mem.eql(u8, suffix, "gb"))
        1000 * 1000 * 1000
    else if (std.mem.eql(u8, suffix, "gib"))
        1024 * 1024 * 1024
    else
        return error.InvalidMemoryValue;

    return std.math.mul(usize, number, multiplier) catch error.Overflow;
}

test "parse memory bytes plain integer" {
    try std.testing.expectEqual(
        @as(usize, 536_870_912),
        try parseMemoryBytes("536870912"),
    );
}

test "parse memory bytes with mib suffix" {
    try std.testing.expectEqual(
        @as(usize, 512 * 1024 * 1024),
        try parseMemoryBytes("512MiB"),
    );
}

test "parse memory bytes with gb suffix" {
    try std.testing.expectEqual(
        @as(usize, 2_000_000_000),
        try parseMemoryBytes("2GB"),
    );
}

test "parse memory bytes rejects invalid input" {
    try std.testing.expectError(
        error.InvalidMemoryValue,
        parseMemoryBytes(""),
    );
    try std.testing.expectError(
        error.InvalidMemoryValue,
        parseMemoryBytes("MiB"),
    );
    try std.testing.expectError(
        error.InvalidMemoryValue,
        parseMemoryBytes("0"),
    );
}
