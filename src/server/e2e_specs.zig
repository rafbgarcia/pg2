const std = @import("std");
const bootstrap_mod = @import("../runtime/bootstrap.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const schema_loader_mod = @import("../catalog/schema_loader.zig");
const parser_mod = @import("../parser/parser.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const heap_mod = @import("../storage/heap.zig");
const disk_mod = @import("../simulator/disk.zig");
const session_mod = @import("session.zig");
const pool_mod = @import("pool.zig");

const BootstrappedRuntime = bootstrap_mod.BootstrappedRuntime;
const Catalog = catalog_mod.Catalog;
const Session = session_mod.Session;
const ConnectionPool = pool_mod.ConnectionPool;

const ScenarioStep = struct {
    request: []const u8,
    expect_exact: []const u8,
};

fn runScenario(
    schema_source: []const u8,
    steps: []const ScenarioStep,
) !void {
    var disk = disk_mod.SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    const backing_memory = try std.testing.allocator.alloc(
        u8,
        256 * 1024 * 1024,
    );
    defer std.testing.allocator.free(backing_memory);

    var runtime = try BootstrappedRuntime.init(
        backing_memory,
        disk.storage(),
        .{ .max_query_slots = 1 },
    );

    var catalog = Catalog{};
    try loadSchemaAndInitializeHeapPages(&catalog, &runtime, schema_source);

    var session = Session.init(&runtime, &catalog);
    var pool = ConnectionPool.init(&runtime);
    var response_buf: [1024]u8 = undefined;

    for (steps) |step| {
        var pool_conn = try pool.checkout();
        defer pool.checkin(&pool_conn) catch {
            @panic("pool checkin failed in E2E scenario");
        };

        const result = try session.handleRequest(
            &pool,
            &pool_conn,
            step.request,
            response_buf[0..],
        );
        try std.testing.expectEqualStrings(
            step.expect_exact,
            response_buf[0..result.bytes_written],
        );
    }
}

fn loadSchemaAndInitializeHeapPages(
    catalog: *Catalog,
    runtime: *BootstrappedRuntime,
    schema_source: []const u8,
) !void {
    const tokens = tokenizer_mod.tokenize(schema_source);
    if (tokens.has_error) return error.InvalidSchema;

    const parsed = parser_mod.parse(&tokens, schema_source);
    if (parsed.has_error) return error.InvalidSchema;

    try schema_loader_mod.loadSchema(catalog, &parsed.ast, &tokens, schema_source);

    var model_id: u16 = 0;
    while (model_id < catalog.model_count) : (model_id += 1) {
        const page_id: u32 = @as(u32, 100) + model_id;
        catalog.models[model_id].heap_first_page_id = page_id;
        catalog.models[model_id].total_pages = 1;

        const page = try runtime.pool.pin(page_id);
        heap_mod.HeapPage.init(page);
        runtime.pool.unpin(page_id, true);
    }
}

test "e2e spec 03 filter sort limit offset via server session path" {
    // Mirrors e2e/specs/03_filter_sort_limit_offset.spec
    const schema_source =
        \\User {
        \\  field(id, bigint, notNull, primaryKey)
        \\  field(name, string, notNull)
        \\  field(active, boolean, notNull)
        \\}
    ;

    const steps = [_]ScenarioStep{
        .{
            .request = "User |> insert(id = 1, name = \"Charlie\", active = true)",
            .expect_exact = "OK rows=0\n",
        },
        .{
            .request = "User |> insert(id = 2, name = \"Alice\", active = true)",
            .expect_exact = "OK rows=0\n",
        },
        .{
            .request = "User |> insert(id = 3, name = \"Bob\", active = false)",
            .expect_exact = "OK rows=0\n",
        },
        .{
            .request = "User |> where(active = true) |> sort(name asc)",
            .expect_exact = "OK rows=2\n2,Alice,true\n1,Charlie,true\n",
        },
        .{
            .request = "User |> sort(name asc) |> offset(1) |> limit(1)",
            .expect_exact = "OK rows=1\n3,Bob,false\n",
        },
    };

    try runScenario(schema_source, steps[0..]);
}
