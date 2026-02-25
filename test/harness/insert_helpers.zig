const std = @import("std");

pub fn buildBulkUserInsertRequest(buf: []u8, start_id: usize, row_count: usize) ![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();
    try writer.writeAll("User |> insert(");
    var row_index: usize = 0;
    while (row_index < row_count) : (row_index += 1) {
        if (row_index > 0) try writer.writeAll(", ");
        const id = start_id + row_index;
        try writer.print(
            "(id = {d}, name = \"user-{d}\", active = true)",
            .{ id, id },
        );
    }
    try writer.writeAll(") {}");
    return stream.getWritten();
}
