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
const btree_mod = pg2.storage.btree;
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

        // Reserve disjoint heap page regions per model in tests so one model's
        // growth cannot overwrite another model's base page.
        const heap_region_start: u32 = 100;
        const heap_region_stride_pages: u32 = 512;
        var model_id: u16 = 0;
        while (model_id < self.catalog.*.model_count) : (model_id += 1) {
            const page_id: u32 = heap_region_start +
                @as(u32, model_id) * heap_region_stride_pages;
            self.catalog.models[model_id].heap_first_page_id = page_id;
            self.catalog.models[model_id].total_pages = 1;

            const page = try self.runtime.pool.pin(page_id);
            heap_mod.HeapPage.init(page);
            self.runtime.pool.unpin(page_id, true);
        }

        // Auto-create PK IndexInfo entries for models with primaryKey columns
        // that don't already have a PK index in the catalog.
        {
            var mid: u16 = 0;
            while (mid < self.catalog.*.model_count) : (mid += 1) {
                const pk_col = catalog_mod.findPrimaryKeyColumnId(self.catalog, mid) orelse continue;
                // Check if a unique index already covers this PK column.
                const model = &self.catalog.models[mid];
                var has_pk_index = false;
                var ii: u16 = 0;
                while (ii < model.index_count) : (ii += 1) {
                    if (model.indexes[ii].is_unique and
                        model.indexes[ii].column_count == 1 and
                        model.indexes[ii].column_ids[0] == pk_col)
                    {
                        has_pk_index = true;
                        break;
                    }
                }
                if (!has_pk_index) {
                    _ = self.catalog.addIndex(
                        mid,
                        "pk",
                        &[_]catalog_mod.ColumnId{pk_col},
                        true,
                    ) catch continue;
                }
            }
        }

        // Initialize B+ tree indexes for all unique indexes (PK and non-PK).
        var next_index_page_id: u32 = 10_000;
        try self.catalog.initializeIndexTrees(
            &self.runtime.pool,
            &self.runtime.wal,
            &next_index_page_id,
        );
    }

    pub fn run(self: *TestExecutor, request: []const u8) ![]const u8 {
        var pool_conn = try self.pool.checkout();
        const result = try self.session.handleRequest(
            &self.pool,
            &pool_conn,
            request,
            null,
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
        try self.runtime.wal.forceFlush();
        return self.response_buf[0..result.bytes_written];
    }

    /// Seed `{id, active=true}` rows using chunked multi-row insert statements.
    /// This keeps feature tests deterministic while avoiding per-row request overhead.
    pub fn seedActiveRows(
        self: *TestExecutor,
        model_name: []const u8,
        first_id: u32,
        last_id_inclusive: u32,
        chunk_size: u16,
    ) !void {
        if (chunk_size == 0) return error.InvalidBatchSize;
        if (first_id > last_id_inclusive) return;

        var next_id = first_id;
        while (next_id <= last_id_inclusive) {
            const remaining = (last_id_inclusive - next_id) + 1;
            const this_chunk: u32 = @min(@as(u32, chunk_size), remaining);
            const chunk_end = next_id + this_chunk - 1;

            var req_buf: [32 * 1024]u8 = undefined;
            var stream = std.io.fixedBufferStream(req_buf[0..]);
            const writer = stream.writer();
            try writer.print("{s} |> insert(", .{model_name});

            var id = next_id;
            var first = true;
            while (id <= chunk_end) : (id += 1) {
                if (!first) try writer.writeAll(", ");
                first = false;
                try writer.print("(id = {d}, active = true)", .{id});
            }
            try writer.writeAll(") {}");

            _ = try self.run(stream.getWritten());
            next_id = chunk_end + 1;
        }
    }
};

pub const FeatureEnv = struct {
    disk: disk_mod.SimulatedDisk,
    backing_memory: []u8,
    runtime: BootstrappedRuntime,
    catalog: Catalog,
    executor: TestExecutor,

    pub fn init(self: *FeatureEnv) !void {
        return self.initWithConfig(.{ .max_query_slots = 1 });
    }

    pub fn initWithConfig(
        self: *FeatureEnv,
        config: bootstrap_mod.BootstrapConfig,
    ) !void {
        return self.initWithConfigAndMemory(
            config,
            64 * 1024 * 1024,
        );
    }

    pub fn initWithConfigAndMemory(
        self: *FeatureEnv,
        config: bootstrap_mod.BootstrapConfig,
        memory_bytes: usize,
    ) !void {
        self.disk = disk_mod.SimulatedDisk.init(testing_allocator);
        errdefer self.disk.deinit();

        self.backing_memory = try testing_allocator.alloc(u8, memory_bytes);
        errdefer testing_allocator.free(self.backing_memory);

        self.runtime = try BootstrappedRuntime.init(
            self.backing_memory,
            self.disk.storage(),
            config,
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
