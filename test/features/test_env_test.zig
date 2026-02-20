//! Shared feature-test harness for server session-path tests.
//!
//! Responsibilities in this file:
//! - Builds deterministic in-memory runtime/disk/catalog test fixtures.
//! - Applies schema definitions and initializes heap pages for models.
//! - Provides a thin request executor over `Session` + `ConnectionPool`.
const std = @import("std");
const pg2 = @import("pg2");

const bootstrap_mod = pg2.runtime.bootstrap;
const catalog_mod = pg2.catalog.meta;
const schema_loader_mod = pg2.catalog.schema_loader;
const parser_mod = pg2.parser.parse;
const tokenizer_mod = pg2.parser.tokenizer;
const heap_mod = pg2.storage.heap;
const disk_mod = pg2.simulator.disk;
const session_mod = pg2.server.session;
const pool_mod = pg2.server.pool;
const mutation_mod = pg2.executor.mutation;

const BootstrappedRuntime = bootstrap_mod.BootstrappedRuntime;
const Catalog = catalog_mod.Catalog;
const Session = session_mod.Session;
const ConnectionPool = pool_mod.ConnectionPool;
const testing_allocator = std.testing.allocator;

pub const TestExecutor = struct {
    runtime: *BootstrappedRuntime,
    catalog: *Catalog,
    session: Session,
    pool: ConnectionPool,
    response_buf: [16 * 1024]u8 = undefined,

    pub fn init(
        self: *TestExecutor,
        runtime: *BootstrappedRuntime,
        catalog: *Catalog,
    ) void {
        self.* = .{
            .runtime = runtime,
            .catalog = catalog,
            .session = Session.init(runtime, catalog),
            .pool = ConnectionPool.init(runtime),
        };
    }

    pub fn applyDefinitions(self: *TestExecutor, source: []const u8) !void {
        const tokens = tokenizer_mod.tokenize(source);
        if (tokens.has_error) return error.InvalidSchema;

        const parsed = parser_mod.parse(&tokens, source);
        if (parsed.has_error) return error.InvalidSchema;

        try schema_loader_mod.loadSchema(self.catalog, &parsed.ast, &tokens, source);

        var model_id: u16 = 0;
        while (model_id < self.catalog.*.model_count) : (model_id += 1) {
            const page_id: u32 = @as(u32, 100) + model_id;
            self.catalog.models[model_id].heap_first_page_id = page_id;
            self.catalog.models[model_id].total_pages = 1;

            const page = try self.runtime.pool.pin(page_id);
            heap_mod.HeapPage.init(page);
            self.runtime.pool.unpin(page_id, true);
        }
    }

    pub fn run(self: *TestExecutor, request: []const u8) ![]const u8 {
        var pool_conn = try self.pool.checkout();
        const result = try self.session.handleRequest(
            &self.pool,
            &pool_conn,
            request,
            self.response_buf[0..],
        );

        if (result.is_query_error) {
            mutation_mod.rollbackOverflowReclaimEntriesForTx(
                self.catalog,
                pool_conn.tx_id,
            );
            try self.pool.abortCheckin(&pool_conn);
            return self.response_buf[0..result.bytes_written];
        }

        const tx_id = pool_conn.tx_id;
        if (result.had_mutation) {
            mutation_mod.commitOverflowReclaimEntriesForTx(
                self.catalog,
                &self.runtime.pool,
                &self.runtime.wal,
                tx_id,
                1,
            ) catch |err| {
                mutation_mod.rollbackOverflowReclaimEntriesForTx(
                    self.catalog,
                    tx_id,
                );
                try self.pool.abortCheckin(&pool_conn);
                return err;
            };
        }
        try self.pool.checkin(&pool_conn);
        return self.response_buf[0..result.bytes_written];
    }
};

pub const FeatureEnv = struct {
    disk: disk_mod.SimulatedDisk,
    backing_memory: []u8,
    runtime: BootstrappedRuntime,
    catalog: Catalog,
    executor: TestExecutor,

    pub fn init(self: *FeatureEnv) !void {
        self.disk = disk_mod.SimulatedDisk.init(testing_allocator);
        errdefer self.disk.deinit();

        self.backing_memory = try testing_allocator.alloc(u8, 256 * 1024 * 1024);
        errdefer testing_allocator.free(self.backing_memory);

        self.runtime = try BootstrappedRuntime.init(
            self.backing_memory,
            self.disk.storage(),
            .{ .max_query_slots = 1 },
        );
        self.catalog = .{};
        self.executor.init(&self.runtime, &self.catalog);
    }

    pub fn deinit(self: *FeatureEnv) void {
        // Keep teardown aligned with existing server tests which do not call
        // runtime.deinit in this path.
        testing_allocator.free(self.backing_memory);
        self.disk.deinit();
    }
};
