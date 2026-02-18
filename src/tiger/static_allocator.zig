const std = @import("std");

const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;
const FixedBufferAllocator = std.heap.FixedBufferAllocator;

pub const AllocatorPhase = enum {
    init,
    sealed,
};

/// Startup-only bump allocator with an explicit seal boundary.
/// After `seal()`, any allocation or growth attempt is a hard panic.
pub const StaticAllocator = struct {
    fixed: FixedBufferAllocator,
    phase: AllocatorPhase = .init,

    const panic_msg =
        "StaticAllocator sealed: runtime allocation attempted";

    pub fn init(buffer: []u8) StaticAllocator {
        std.debug.assert(buffer.len > 0);
        return .{
            .fixed = FixedBufferAllocator.init(buffer),
        };
    }

    pub fn allocator(self: *StaticAllocator) Allocator {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn seal(self: *StaticAllocator) void {
        std.debug.assert(self.phase == .init);
        self.phase = .sealed;
    }

    pub fn isSealed(self: *const StaticAllocator) bool {
        return self.phase == .sealed;
    }

    pub fn bytesUsed(self: *const StaticAllocator) usize {
        return self.fixed.end_index;
    }

    pub fn bytesRemaining(self: *const StaticAllocator) usize {
        return self.fixed.buffer.len - self.fixed.end_index;
    }

    fn ensureInitPhase(self: *const StaticAllocator) void {
        if (self.phase == .sealed) @panic(panic_msg);
    }

    fn alloc(
        ctx: *anyopaque,
        len: usize,
        alignment: Alignment,
        ret_addr: usize,
    ) ?[*]u8 {
        const self: *StaticAllocator = @ptrCast(@alignCast(ctx));
        self.ensureInitPhase();
        return FixedBufferAllocator.alloc(
            &self.fixed,
            len,
            alignment,
            ret_addr,
        );
    }

    fn resize(
        ctx: *anyopaque,
        memory: []u8,
        alignment: Alignment,
        new_len: usize,
        ret_addr: usize,
    ) bool {
        const self: *StaticAllocator = @ptrCast(@alignCast(ctx));
        if (new_len > memory.len) self.ensureInitPhase();
        return FixedBufferAllocator.resize(
            &self.fixed,
            memory,
            alignment,
            new_len,
            ret_addr,
        );
    }

    fn remap(
        ctx: *anyopaque,
        memory: []u8,
        alignment: Alignment,
        new_len: usize,
        ret_addr: usize,
    ) ?[*]u8 {
        const self: *StaticAllocator = @ptrCast(@alignCast(ctx));
        if (new_len > memory.len) self.ensureInitPhase();
        return FixedBufferAllocator.remap(
            &self.fixed,
            memory,
            alignment,
            new_len,
            ret_addr,
        );
    }

    fn free(
        ctx: *anyopaque,
        memory: []u8,
        alignment: Alignment,
        ret_addr: usize,
    ) void {
        const self: *StaticAllocator = @ptrCast(@alignCast(ctx));
        FixedBufferAllocator.free(
            &self.fixed,
            memory,
            alignment,
            ret_addr,
        );
    }

    const vtable: Allocator.VTable = .{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };
};

test "static allocator allocates in init phase and tracks usage" {
    var backing: [128]u8 = undefined;
    var static_allocator = StaticAllocator.init(backing[0..]);
    const allocator = static_allocator.allocator();

    const bytes_before = static_allocator.bytesUsed();
    const slice = try allocator.alloc(u8, 32);
    defer allocator.free(slice);

    try std.testing.expect(static_allocator.bytesUsed() >= bytes_before + 32);
    try std.testing.expect(static_allocator.bytesRemaining() <= backing.len);
}

test "static allocator seal flips allocator phase" {
    var backing: [64]u8 = undefined;
    var static_allocator = StaticAllocator.init(backing[0..]);

    try std.testing.expect(!static_allocator.isSealed());
    static_allocator.seal();
    try std.testing.expect(static_allocator.isSealed());
}

test "static allocator permits shrink after seal but blocks growth" {
    var backing: [64]u8 = undefined;
    var static_allocator = StaticAllocator.init(backing[0..]);
    const allocator = static_allocator.allocator();

    const slice = try allocator.alloc(u8, 24);
    static_allocator.seal();

    // Shrink is allowed because it does not require new allocation.
    const shrunk = try allocator.realloc(slice, 16);
    allocator.free(shrunk);
}
