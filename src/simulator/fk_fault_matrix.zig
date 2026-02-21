//! Deterministic fault-matrix tests for referential-integrity behavior.
//!
//! Responsibilities in this file:
//! - Builds parent/child schemas with explicit referential actions.
//! - Exercises delete/update behavior under crash/restart conditions.
//! - Verifies RI invariants remain stable across seeded fault scenarios.
const std = @import("std");
const disk_mod = @import("disk.zig");
const buffer_pool_mod = @import("../storage/buffer_pool.zig");
const wal_mod = @import("../storage/wal.zig");
const heap_mod = @import("../storage/heap.zig");
const catalog_mod = @import("../catalog/catalog.zig");
const mutation_mod = @import("../executor/mutation.zig");
const scan_mod = @import("../executor/scan.zig");
const tx_mod = @import("../mvcc/transaction.zig");
const undo_mod = @import("../mvcc/undo.zig");
const tokenizer_mod = @import("../parser/tokenizer.zig");
const parser_mod = @import("../parser/parser.zig");

const SimulatedDisk = disk_mod.SimulatedDisk;
const BufferPool = buffer_pool_mod.BufferPool;
const Wal = wal_mod.Wal;
const HeapPage = heap_mod.HeapPage;
const Catalog = catalog_mod.Catalog;
const ModelId = catalog_mod.ModelId;
const ReferentialAction = catalog_mod.ReferentialAction;
const TxManager = tx_mod.TxManager;
const UndoLog = undo_mod.UndoLog;

const ScenarioOutcome = struct {
    signature: u64,
};

const ModelIds = struct {
    user: ModelId,
    post: ModelId,
};

fn setupReferenceCatalog(
    catalog: *Catalog,
    user_page_id: u32,
    post_page_id: u32,
    post_user_nullable: bool,
    on_delete: ReferentialAction,
    on_update: ReferentialAction,
) !ModelIds {
    catalog.* = Catalog{};

    const user_model = try catalog.addModel("User");
    _ = try catalog.addColumn(user_model, "id", .i64, false);
    catalog.setColumnPrimaryKey(user_model, 0);
    catalog.models[user_model].heap_first_page_id = user_page_id;
    catalog.models[user_model].total_pages = 1;

    const post_model = try catalog.addModel("Post");
    _ = try catalog.addColumn(post_model, "id", .i64, false);
    _ = try catalog.addColumn(post_model, "user_id", .i64, post_user_nullable);
    catalog.setColumnPrimaryKey(post_model, 0);
    catalog.models[post_model].heap_first_page_id = post_page_id;
    catalog.models[post_model].total_pages = 1;

    const assoc_id = try catalog.addAssociation(
        post_model,
        "author",
        .belongs_to,
        "User",
    );
    try catalog.setAssociationKeys(post_model, assoc_id, "user_id", "id");
    try catalog.setAssociationReferentialIntegrity(
        post_model,
        assoc_id,
        .with_referential_integrity,
        on_delete,
        on_update,
    );
    try catalog.resolveAssociations();

    return .{ .user = user_model, .post = post_model };
}

fn initHeapPages(pool: *BufferPool, catalog: *const Catalog, models: ModelIds) !void {
    const user_page = try pool.pin(catalog.models[models.user].heap_first_page_id);
    HeapPage.init(user_page);
    pool.unpin(catalog.models[models.user].heap_first_page_id, true);

    const post_page = try pool.pin(catalog.models[models.post].heap_first_page_id);
    HeapPage.init(post_page);
    pool.unpin(catalog.models[models.post].heap_first_page_id, true);
}

fn executeInsertSource(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    tx_id: tx_mod.TxId,
    model_id: ModelId,
    source: []const u8,
) !void {
    const tokens = tokenizer_mod.tokenize(source);
    try std.testing.expect(!tokens.has_error);
    const parsed = parser_mod.parse(&tokens, source);
    try std.testing.expect(!parsed.has_error);

    const root = parsed.ast.getNode(parsed.ast.root);
    const pipeline = parsed.ast.getNode(root.data.unary);
    const insert_op = parsed.ast.getNode(pipeline.data.binary.rhs);
    _ = try mutation_mod.executeInsert(
        catalog,
        pool,
        wal,
        tx_id,
        model_id,
        &parsed.ast,
        &tokens,
        source,
        insert_op.data.unary,
    );
}

fn executeDeleteSource(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    undo_log: *UndoLog,
    tx_id: tx_mod.TxId,
    snapshot: *const tx_mod.Snapshot,
    tm: *const TxManager,
    model_id: ModelId,
    source: []const u8,
) !u32 {
    const tokens = tokenizer_mod.tokenize(source);
    try std.testing.expect(!tokens.has_error);
    const parsed = parser_mod.parse(&tokens, source);
    try std.testing.expect(!parsed.has_error);

    const root = parsed.ast.getNode(parsed.ast.root);
    const pipeline = parsed.ast.getNode(root.data.unary);
    const where_op = parsed.ast.getNode(pipeline.data.binary.rhs);
    return mutation_mod.executeDelete(
        catalog,
        pool,
        wal,
        undo_log,
        tx_id,
        snapshot,
        tm,
        model_id,
        &parsed.ast,
        &tokens,
        source,
        where_op.data.unary,
        std.testing.allocator,
    );
}

fn executeUpdateSource(
    catalog: *Catalog,
    pool: *BufferPool,
    wal: *Wal,
    undo_log: *UndoLog,
    tx_id: tx_mod.TxId,
    snapshot: *const tx_mod.Snapshot,
    tm: *const TxManager,
    model_id: ModelId,
    source: []const u8,
) !u32 {
    const tokens = tokenizer_mod.tokenize(source);
    try std.testing.expect(!tokens.has_error);
    const parsed = parser_mod.parse(&tokens, source);
    try std.testing.expect(!parsed.has_error);

    const root = parsed.ast.getNode(parsed.ast.root);
    const pipeline = parsed.ast.getNode(root.data.unary);
    const where_op = parsed.ast.getNode(pipeline.data.binary.rhs);
    const update_op = parsed.ast.getNode(where_op.next);
    return mutation_mod.executeUpdate(
        catalog,
        pool,
        wal,
        undo_log,
        tx_id,
        snapshot,
        tm,
        model_id,
        &parsed.ast,
        &tokens,
        source,
        where_op.data.unary,
        update_op.data.unary,
        std.testing.allocator,
    );
}

fn runFkRestrictCrashRestart(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();
    const user_page_id: u32 = 200 + rand.uintLessThan(u32, 50);
    const post_page_id: u32 = 400 + rand.uintLessThan(u32, 50);

    var before_crash_violation: u8 = 0;
    var after_crash_violation: u8 = 0;
    var user_rows_after: u16 = 0;
    var post_rows_after: u16 = 0;
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    {
        var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 16);
        defer pool.deinit();
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();
        pool.wal = &wal;
        var tm = TxManager.init(std.testing.allocator);
        defer tm.deinit();
        var undo_log = try UndoLog.init(std.testing.allocator, 1024, 64 * 1024);
        defer undo_log.deinit();
        var catalog: Catalog = undefined;
        const models = try setupReferenceCatalog(
            &catalog,
            user_page_id,
            post_page_id,
            true,
            .restrict,
            .restrict,
        );
        try initHeapPages(&pool, &catalog, models);

        const tx_insert = try tm.begin();
        try executeInsertSource(&catalog, &pool, &wal, tx_insert, models.user, "User |> insert(id = 1)");
        try executeInsertSource(
            &catalog,
            &pool,
            &wal,
            tx_insert,
            models.post,
            "Post |> insert(id = 10, user_id = 1)",
        );
        try tm.commit(tx_insert);
        _ = try wal.commitTx(tx_insert);
        try pool.flushAll();

        const tx_delete = try tm.begin();
        var snap = try tm.snapshot(tx_delete);
        defer snap.deinit();
        const delete_result = executeDeleteSource(
            &catalog,
            &pool,
            &wal,
            &undo_log,
            tx_delete,
            &snap,
            &tm,
            models.user,
            "User |> where(id == 1) |> delete",
        );
        try std.testing.expectError(error.ReferentialIntegrityViolation, delete_result);
        before_crash_violation = 1;
        try tm.abort(tx_delete);
    }

    disk.crash();

    {
        var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 16);
        defer pool.deinit();
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();
        try wal.recover();
        pool.wal = &wal;
        var tm = TxManager.init(std.testing.allocator);
        defer tm.deinit();
        var undo_log = try UndoLog.init(std.testing.allocator, 1024, 64 * 1024);
        defer undo_log.deinit();
        var catalog: Catalog = undefined;
        const models = try setupReferenceCatalog(
            &catalog,
            user_page_id,
            post_page_id,
            true,
            .restrict,
            .restrict,
        );

        const tx_delete = try tm.begin();
        var snap_delete = try tm.snapshot(tx_delete);
        defer snap_delete.deinit();
        const delete_result = executeDeleteSource(
            &catalog,
            &pool,
            &wal,
            &undo_log,
            tx_delete,
            &snap_delete,
            &tm,
            models.user,
            "User |> where(id == 1) |> delete",
        );
        try std.testing.expectError(error.ReferentialIntegrityViolation, delete_result);
        after_crash_violation = 1;
        try tm.abort(tx_delete);

        const tx_read = try tm.begin();
        defer tm.abort(tx_read) catch {};
        var snap_read = try tm.snapshot(tx_read);
        defer snap_read.deinit();
        var user_scan = try scan_mod.tableScan(
            &catalog,
            &pool,
            &undo_log,
            &snap_read,
            &tm,
            models.user,
            std.testing.allocator,
        );
        defer user_scan.deinit();
        user_rows_after = user_scan.row_count;

        var post_scan = try scan_mod.tableScan(
            &catalog,
            &pool,
            &undo_log,
            &snap_read,
            &tm,
            models.post,
            std.testing.allocator,
        );
        defer post_scan.deinit();
        post_rows_after = post_scan.row_count;
    }

    var h = std.hash.Wyhash.init(seed ^ 0xF00D0001);
    h.update(&[_]u8{ before_crash_violation, after_crash_violation });
    h.update(std.mem.asBytes(&user_rows_after));
    h.update(std.mem.asBytes(&post_rows_after));
    h.update(std.mem.asBytes(&disk.writes));
    h.update(std.mem.asBytes(&disk.fsyncs));
    return .{ .signature = h.final() };
}

fn runFkCascadeCrashRestart(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();
    const user_page_id: u32 = 260 + rand.uintLessThan(u32, 50);
    const post_page_id: u32 = 460 + rand.uintLessThan(u32, 50);

    var deleted_before_crash: u32 = 0;
    var user_rows_after: u16 = 0;
    var post_rows_after: u16 = 0;
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    {
        var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 16);
        defer pool.deinit();
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();
        pool.wal = &wal;
        var tm = TxManager.init(std.testing.allocator);
        defer tm.deinit();
        var undo_log = try UndoLog.init(std.testing.allocator, 1024, 64 * 1024);
        defer undo_log.deinit();
        var catalog: Catalog = undefined;
        const models = try setupReferenceCatalog(
            &catalog,
            user_page_id,
            post_page_id,
            true,
            .cascade,
            .restrict,
        );
        try initHeapPages(&pool, &catalog, models);

        const tx_insert = try tm.begin();
        try executeInsertSource(&catalog, &pool, &wal, tx_insert, models.user, "User |> insert(id = 1)");
        try executeInsertSource(
            &catalog,
            &pool,
            &wal,
            tx_insert,
            models.post,
            "Post |> insert(id = 10, user_id = 1)",
        );
        try tm.commit(tx_insert);
        _ = try wal.commitTx(tx_insert);

        const tx_delete = try tm.begin();
        var snap_delete = try tm.snapshot(tx_delete);
        defer snap_delete.deinit();
        deleted_before_crash = try executeDeleteSource(
            &catalog,
            &pool,
            &wal,
            &undo_log,
            tx_delete,
            &snap_delete,
            &tm,
            models.user,
            "User |> where(id == 1) |> delete",
        );
        try std.testing.expectEqual(@as(u32, 1), deleted_before_crash);
        try tm.commit(tx_delete);
        _ = try wal.commitTx(tx_delete);
        try pool.flushAll();
    }

    disk.crash();

    {
        var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 16);
        defer pool.deinit();
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();
        try wal.recover();
        pool.wal = &wal;
        var tm = TxManager.init(std.testing.allocator);
        defer tm.deinit();
        var undo_log = try UndoLog.init(std.testing.allocator, 1024, 64 * 1024);
        defer undo_log.deinit();
        var catalog: Catalog = undefined;
        const models = try setupReferenceCatalog(
            &catalog,
            user_page_id,
            post_page_id,
            true,
            .cascade,
            .restrict,
        );

        const tx_read = try tm.begin();
        defer tm.abort(tx_read) catch {};
        var snap_read = try tm.snapshot(tx_read);
        defer snap_read.deinit();
        var user_scan = try scan_mod.tableScan(
            &catalog,
            &pool,
            &undo_log,
            &snap_read,
            &tm,
            models.user,
            std.testing.allocator,
        );
        defer user_scan.deinit();
        user_rows_after = user_scan.row_count;

        var post_scan = try scan_mod.tableScan(
            &catalog,
            &pool,
            &undo_log,
            &snap_read,
            &tm,
            models.post,
            std.testing.allocator,
        );
        defer post_scan.deinit();
        post_rows_after = post_scan.row_count;
    }

    try std.testing.expectEqual(@as(u16, 0), user_rows_after);
    try std.testing.expectEqual(@as(u16, 0), post_rows_after);

    var h = std.hash.Wyhash.init(seed ^ 0xF00D0002);
    h.update(std.mem.asBytes(&deleted_before_crash));
    h.update(std.mem.asBytes(&user_rows_after));
    h.update(std.mem.asBytes(&post_rows_after));
    h.update(std.mem.asBytes(&disk.writes));
    h.update(std.mem.asBytes(&disk.fsyncs));
    return .{ .signature = h.final() };
}

fn runFkUpdateSetNullCrashRestart(seed: u64) !ScenarioOutcome {
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();
    const user_page_id: u32 = 320 + rand.uintLessThan(u32, 50);
    const post_page_id: u32 = 520 + rand.uintLessThan(u32, 50);

    var updated_before_crash: u32 = 0;
    var post_user_id_is_null: u8 = 0;
    var user_id_after: i64 = -1;
    var disk = SimulatedDisk.init(std.testing.allocator);
    defer disk.deinit();

    {
        var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 16);
        defer pool.deinit();
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();
        pool.wal = &wal;
        var tm = TxManager.init(std.testing.allocator);
        defer tm.deinit();
        var undo_log = try UndoLog.init(std.testing.allocator, 1024, 64 * 1024);
        defer undo_log.deinit();
        var catalog: Catalog = undefined;
        const models = try setupReferenceCatalog(
            &catalog,
            user_page_id,
            post_page_id,
            true,
            .restrict,
            .set_null,
        );
        try initHeapPages(&pool, &catalog, models);

        const tx_insert = try tm.begin();
        try executeInsertSource(&catalog, &pool, &wal, tx_insert, models.user, "User |> insert(id = 1)");
        try executeInsertSource(
            &catalog,
            &pool,
            &wal,
            tx_insert,
            models.post,
            "Post |> insert(id = 10, user_id = 1)",
        );
        try tm.commit(tx_insert);
        _ = try wal.commitTx(tx_insert);

        const tx_update = try tm.begin();
        var snap_update = try tm.snapshot(tx_update);
        defer snap_update.deinit();
        updated_before_crash = try executeUpdateSource(
            &catalog,
            &pool,
            &wal,
            &undo_log,
            tx_update,
            &snap_update,
            &tm,
            models.user,
            "User |> where(id == 1) |> update(id = 2)",
        );
        try std.testing.expectEqual(@as(u32, 1), updated_before_crash);
        try tm.commit(tx_update);
        _ = try wal.commitTx(tx_update);
        try pool.flushAll();
    }

    disk.crash();

    {
        var pool = try BufferPool.init(std.testing.allocator, disk.storage(), 16);
        defer pool.deinit();
        var wal = Wal.init(std.testing.allocator, disk.storage());
        defer wal.deinit();
        try wal.recover();
        pool.wal = &wal;
        var tm = TxManager.init(std.testing.allocator);
        defer tm.deinit();
        var undo_log = try UndoLog.init(std.testing.allocator, 1024, 64 * 1024);
        defer undo_log.deinit();
        var catalog: Catalog = undefined;
        const models = try setupReferenceCatalog(
            &catalog,
            user_page_id,
            post_page_id,
            true,
            .restrict,
            .set_null,
        );

        const tx_read = try tm.begin();
        defer tm.abort(tx_read) catch {};
        var snap_read = try tm.snapshot(tx_read);
        defer snap_read.deinit();
        var post_scan = try scan_mod.tableScan(
            &catalog,
            &pool,
            &undo_log,
            &snap_read,
            &tm,
            models.post,
            std.testing.allocator,
        );
        defer post_scan.deinit();
        try std.testing.expectEqual(@as(u16, 1), post_scan.row_count);
        post_user_id_is_null = if (post_scan.rows[0].values[1] == .null_value) 1 else 0;

        var user_scan = try scan_mod.tableScan(
            &catalog,
            &pool,
            &undo_log,
            &snap_read,
            &tm,
            models.user,
            std.testing.allocator,
        );
        defer user_scan.deinit();
        try std.testing.expectEqual(@as(u16, 1), user_scan.row_count);
        user_id_after = user_scan.rows[0].values[0].i64;
    }

    try std.testing.expectEqual(@as(u8, 1), post_user_id_is_null);
    try std.testing.expectEqual(@as(i64, 2), user_id_after);

    var h = std.hash.Wyhash.init(seed ^ 0xF00D0003);
    h.update(std.mem.asBytes(&updated_before_crash));
    h.update(&[_]u8{post_user_id_is_null});
    h.update(std.mem.asBytes(&user_id_after));
    h.update(std.mem.asBytes(&disk.writes));
    h.update(std.mem.asBytes(&disk.fsyncs));
    return .{ .signature = h.final() };
}

const seed_set = [_]u64{
    0xC0FFEE01,
    0xC0FFEEA5,
    0xD15EA5E1,
    0xFEED1234,
    0x1234ABCD,
    0x0BADF00D,
    0xABAD1DEA,
    0x51515151,
};

test "seeded schedule: FK restrict delete remains rejected across crash and restart deterministically" {
    for (seed_set) |seed| {
        const first = try runFkRestrictCrashRestart(seed);
        const second = try runFkRestrictCrashRestart(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: FK cascade delete remains deterministic across crash and restart" {
    for (seed_set) |seed| {
        const first = try runFkCascadeCrashRestart(seed);
        const second = try runFkCascadeCrashRestart(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}

test "seeded schedule: FK set_null update remains deterministic across crash and restart" {
    for (seed_set) |seed| {
        const first = try runFkUpdateSetNullCrashRestart(seed);
        const second = try runFkUpdateSetNullCrashRestart(seed);
        try std.testing.expectEqual(first.signature, second.signature);
    }
}
