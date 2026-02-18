const std = @import("std");
const row_mod = @import("../storage/row.zig");

const ColumnType = row_mod.ColumnType;
const RowSchema = row_mod.RowSchema;
const Value = row_mod.Value;

/// Capacity limits.
pub const max_models = 256;
pub const max_columns_per_model = 128;
pub const max_indexes_per_model = 32;
pub const max_associations_per_model = 32;
pub const max_scopes_per_model = 32;
pub const max_name_bytes = 64 * 1024;

pub const ModelId = u16;
pub const ColumnId = u16;
pub const IndexId = u16;
pub const AssociationId = u16;
pub const ScopeId = u16;

pub const null_model: ModelId = std.math.maxInt(ModelId);
pub const null_column: ColumnId = std.math.maxInt(ColumnId);

pub const AssociationKind = enum(u8) {
    has_one,
    has_many,
    belongs_to,
};

pub const IndexInfo = struct {
    name_offset: u32,
    name_len: u16,
    column_ids: [16]ColumnId = [_]ColumnId{null_column} ** 16,
    column_count: u8 = 0,
    is_unique: bool = false,
    btree_root_page_id: u32 = 0,
    // O(1) stats.
    entry_count: u64 = 0,
    distinct_count: u64 = 0,
    min_value: Value = .{ .null_value = {} },
    max_value: Value = .{ .null_value = {} },
};

pub const AssociationInfo = struct {
    name_offset: u32,
    name_len: u16,
    kind: AssociationKind,
    target_model_id: ModelId = null_model,
    target_model_name_offset: u32 = 0,
    target_model_name_len: u16 = 0,
    foreign_key_column_id: ColumnId = null_column,
    local_column_id: ColumnId = null_column,
};

pub const ScopeInfo = struct {
    name_offset: u32,
    name_len: u16,
    /// AST node index of the first pipeline operator for this scope.
    ast_first_op: u16 = 0,
};

pub const ColumnInfo = struct {
    name_offset: u32,
    name_len: u16,
    column_type: ColumnType,
    nullable: bool,
    is_primary_key: bool = false,
    has_default: bool = false,
};

pub const ModelInfo = struct {
    name_offset: u32,
    name_len: u16,

    columns: [max_columns_per_model]ColumnInfo = undefined,
    column_count: u16 = 0,

    indexes: [max_indexes_per_model]IndexInfo = undefined,
    index_count: u16 = 0,

    associations: [max_associations_per_model]AssociationInfo = undefined,
    association_count: u16 = 0,

    scopes: [max_scopes_per_model]ScopeInfo = undefined,
    scope_count: u16 = 0,

    heap_first_page_id: u32 = 0,
    row_schema: RowSchema = RowSchema{},

    // O(1) stats.
    row_count: u64 = 0,
    avg_row_size_bytes: u32 = 0,
    total_pages: u32 = 0,
};

pub const CatalogError = error{
    CatalogSealed,
    TooManyModels,
    TooManyColumns,
    TooManyIndexes,
    TooManyAssociations,
    TooManyScopes,
    NameBufferFull,
    DuplicateName,
    ModelNotFound,
    ColumnNotFound,
};

pub const Catalog = struct {
    models: [max_models]ModelInfo = undefined,
    model_count: u16 = 0,
    sealed: bool = false,

    name_buffer: [max_name_bytes]u8 = undefined,
    name_buffer_len: u32 = 0,

    /// Store a name in the shared name buffer. Returns offset and length.
    fn storeName(self: *Catalog, name: []const u8) CatalogError!struct { offset: u32, len: u16 } {
        if (self.sealed) return error.CatalogSealed;
        const name_u16: u16 = @intCast(name.len);
        if (self.name_buffer_len + name.len > max_name_bytes) return error.NameBufferFull;
        const offset = self.name_buffer_len;
        @memcpy(self.name_buffer[offset..][0..name.len], name);
        self.name_buffer_len += @intCast(name.len);
        return .{ .offset = offset, .len = name_u16 };
    }

    /// Retrieve a name from the buffer.
    pub fn getName(self: *const Catalog, offset: u32, len: u16) []const u8 {
        return self.name_buffer[offset..][0..len];
    }

    pub fn getModelName(self: *const Catalog, model_id: ModelId) []const u8 {
        const m = &self.models[model_id];
        return self.getName(m.name_offset, m.name_len);
    }

    pub fn addModel(self: *Catalog, name: []const u8) CatalogError!ModelId {
        if (self.sealed) return error.CatalogSealed;
        if (self.model_count >= max_models) return error.TooManyModels;
        if (self.findModel(name) != null) return error.DuplicateName;

        const stored = try self.storeName(name);
        const id = self.model_count;
        self.models[id] = ModelInfo{
            .name_offset = stored.offset,
            .name_len = stored.len,
        };
        self.model_count += 1;
        return id;
    }

    pub fn addColumn(
        self: *Catalog,
        model_id: ModelId,
        name: []const u8,
        col_type: ColumnType,
        nullable: bool,
    ) CatalogError!ColumnId {
        if (self.sealed) return error.CatalogSealed;
        std.debug.assert(model_id < self.model_count);
        var model = &self.models[model_id];
        if (model.column_count >= max_columns_per_model) return error.TooManyColumns;

        // Check duplicate column name within model.
        if (self.findColumn(model_id, name) != null) return error.DuplicateName;

        const stored = try self.storeName(name);
        const col_id = model.column_count;
        model.columns[col_id] = ColumnInfo{
            .name_offset = stored.offset,
            .name_len = stored.len,
            .column_type = col_type,
            .nullable = nullable,
        };
        model.column_count += 1;

        // Mirror into row_schema.
        _ = model.row_schema.addColumn(name, col_type, nullable) catch |e| {
            return switch (e) {
                error.TooManyColumns => error.TooManyColumns,
                error.NameBufferFull => error.NameBufferFull,
            };
        };

        return col_id;
    }

    pub fn addIndex(
        self: *Catalog,
        model_id: ModelId,
        name: []const u8,
        column_ids: []const ColumnId,
        is_unique: bool,
    ) CatalogError!IndexId {
        if (self.sealed) return error.CatalogSealed;
        std.debug.assert(model_id < self.model_count);
        var model = &self.models[model_id];
        if (model.index_count >= max_indexes_per_model) return error.TooManyIndexes;

        const stored = try self.storeName(name);
        const idx_id = model.index_count;
        var info = IndexInfo{
            .name_offset = stored.offset,
            .name_len = stored.len,
            .is_unique = is_unique,
        };
        const copy_count = @min(column_ids.len, 16);
        for (0..copy_count) |i| {
            info.column_ids[i] = column_ids[i];
        }
        info.column_count = @intCast(copy_count);
        model.indexes[idx_id] = info;
        model.index_count += 1;
        return idx_id;
    }

    pub fn addAssociation(
        self: *Catalog,
        model_id: ModelId,
        name: []const u8,
        kind: AssociationKind,
        target_name: []const u8,
    ) CatalogError!AssociationId {
        if (self.sealed) return error.CatalogSealed;
        std.debug.assert(model_id < self.model_count);
        var model = &self.models[model_id];
        if (model.association_count >= max_associations_per_model) {
            return error.TooManyAssociations;
        }

        const stored = try self.storeName(name);
        const target_stored = try self.storeName(target_name);
        const assoc_id = model.association_count;
        model.associations[assoc_id] = AssociationInfo{
            .name_offset = stored.offset,
            .name_len = stored.len,
            .kind = kind,
            .target_model_name_offset = target_stored.offset,
            .target_model_name_len = target_stored.len,
        };
        model.association_count += 1;
        return assoc_id;
    }

    pub fn setAssociationKeys(
        self: *Catalog,
        model_id: ModelId,
        assoc_id: AssociationId,
        local_column_name: []const u8,
        foreign_column_name: []const u8,
    ) CatalogError!void {
        if (self.sealed) return error.CatalogSealed;
        std.debug.assert(model_id < self.model_count);
        var model = &self.models[model_id];
        std.debug.assert(assoc_id < model.association_count);
        const assoc = &model.associations[assoc_id];
        const target_model_id = if (assoc.target_model_id != null_model)
            assoc.target_model_id
        else blk: {
            const target_name = self.getName(
                assoc.target_model_name_offset,
                assoc.target_model_name_len,
            );
            break :blk self.findModel(target_name) orelse return error.ModelNotFound;
        };
        assoc.local_column_id = self.findColumn(model_id, local_column_name) orelse
            return error.ColumnNotFound;
        assoc.foreign_key_column_id = self.findColumn(
            target_model_id,
            foreign_column_name,
        ) orelse return error.ColumnNotFound;
    }

    pub fn addScope(
        self: *Catalog,
        model_id: ModelId,
        name: []const u8,
        ast_first_op: u16,
    ) CatalogError!ScopeId {
        if (self.sealed) return error.CatalogSealed;
        std.debug.assert(model_id < self.model_count);
        var model = &self.models[model_id];
        if (model.scope_count >= max_scopes_per_model) return error.TooManyScopes;

        const stored = try self.storeName(name);
        const scope_id = model.scope_count;
        model.scopes[scope_id] = ScopeInfo{
            .name_offset = stored.offset,
            .name_len = stored.len,
            .ast_first_op = ast_first_op,
        };
        model.scope_count += 1;
        return scope_id;
    }

    // --- Lookup functions ---

    pub fn findModel(self: *const Catalog, name: []const u8) ?ModelId {
        for (0..self.model_count) |i| {
            const m = &self.models[i];
            if (std.mem.eql(u8, self.getName(m.name_offset, m.name_len), name)) {
                return @intCast(i);
            }
        }
        return null;
    }

    pub fn findColumn(self: *const Catalog, model_id: ModelId, name: []const u8) ?ColumnId {
        std.debug.assert(model_id < self.model_count);
        const model = &self.models[model_id];
        for (0..model.column_count) |i| {
            const col = &model.columns[i];
            if (std.mem.eql(u8, self.getName(col.name_offset, col.name_len), name)) {
                return @intCast(i);
            }
        }
        return null;
    }

    pub fn findIndex(self: *const Catalog, model_id: ModelId, name: []const u8) ?IndexId {
        std.debug.assert(model_id < self.model_count);
        const model = &self.models[model_id];
        for (0..model.index_count) |i| {
            const idx = &model.indexes[i];
            if (std.mem.eql(u8, self.getName(idx.name_offset, idx.name_len), name)) {
                return @intCast(i);
            }
        }
        return null;
    }

    pub fn findAssociation(
        self: *const Catalog,
        model_id: ModelId,
        name: []const u8,
    ) ?AssociationId {
        std.debug.assert(model_id < self.model_count);
        const model = &self.models[model_id];
        for (0..model.association_count) |i| {
            const assoc = &model.associations[i];
            if (std.mem.eql(u8, self.getName(assoc.name_offset, assoc.name_len), name)) {
                return @intCast(i);
            }
        }
        return null;
    }

    pub fn findScope(self: *const Catalog, model_id: ModelId, name: []const u8) ?ScopeId {
        std.debug.assert(model_id < self.model_count);
        const model = &self.models[model_id];
        for (0..model.scope_count) |i| {
            const sc = &model.scopes[i];
            if (std.mem.eql(u8, self.getName(sc.name_offset, sc.name_len), name)) {
                return @intCast(i);
            }
        }
        return null;
    }

    // --- Stats update functions ---

    pub fn incrementRowCount(self: *Catalog, model_id: ModelId, count: u64) void {
        std.debug.assert(model_id < self.model_count);
        self.models[model_id].row_count += count;
    }

    pub fn decrementRowCount(self: *Catalog, model_id: ModelId, count: u64) void {
        std.debug.assert(model_id < self.model_count);
        const model = &self.models[model_id];
        std.debug.assert(model.row_count >= count);
        model.row_count -= count;
    }

    pub fn updateAvgRowSize(self: *Catalog, model_id: ModelId, avg_bytes: u32) void {
        std.debug.assert(model_id < self.model_count);
        self.models[model_id].avg_row_size_bytes = avg_bytes;
    }

    pub fn updateIndexStats(
        self: *Catalog,
        model_id: ModelId,
        index_id: IndexId,
        entry_count: u64,
        distinct_count: u64,
    ) void {
        std.debug.assert(model_id < self.model_count);
        const model = &self.models[model_id];
        std.debug.assert(index_id < model.index_count);
        model.indexes[index_id].entry_count = entry_count;
        model.indexes[index_id].distinct_count = distinct_count;
    }

    /// Seal the catalog. No further additions allowed.
    pub fn seal(self: *Catalog) void {
        std.debug.assert(!self.sealed);
        self.sealed = true;
    }

    /// Resolve association target model IDs by name.
    /// Call after all models are added, before sealing.
    pub fn resolveAssociations(self: *Catalog) CatalogError!void {
        for (0..self.model_count) |mi| {
            const model = &self.models[mi];
            for (0..model.association_count) |ai| {
                const assoc = &model.associations[ai];
                const target_name = self.getName(
                    assoc.target_model_name_offset,
                    assoc.target_model_name_len,
                );
                const target_id = self.findModel(target_name) orelse
                    return error.ModelNotFound;
                assoc.target_model_id = target_id;

                if (assoc.local_column_id == null_column) {
                    assoc.local_column_id = inferAssociationLocalKey(
                        self,
                        @intCast(mi),
                        assoc,
                    ) orelse return error.ColumnNotFound;
                }
                if (assoc.foreign_key_column_id == null_column) {
                    assoc.foreign_key_column_id = inferAssociationForeignKey(
                        self,
                        @intCast(mi),
                        assoc,
                    ) orelse return error.ColumnNotFound;
                }
            }
        }
    }

    /// Mark a column as primary key.
    pub fn setColumnPrimaryKey(self: *Catalog, model_id: ModelId, col_id: ColumnId) void {
        std.debug.assert(model_id < self.model_count);
        const model = &self.models[model_id];
        std.debug.assert(col_id < model.column_count);
        model.columns[col_id].is_primary_key = true;
    }
};

fn inferAssociationLocalKey(
    catalog: *const Catalog,
    source_model_id: ModelId,
    assoc: *const AssociationInfo,
) ?ColumnId {
    return switch (assoc.kind) {
        .has_many, .has_one => findPrimaryKeyOrId(catalog, source_model_id),
        .belongs_to => findModelForeignKey(
            catalog,
            source_model_id,
            catalog.getModelName(assoc.target_model_id),
        ),
    };
}

fn inferAssociationForeignKey(
    catalog: *const Catalog,
    source_model_id: ModelId,
    assoc: *const AssociationInfo,
) ?ColumnId {
    return switch (assoc.kind) {
        .has_many, .has_one => findModelForeignKey(
            catalog,
            assoc.target_model_id,
            catalog.getModelName(source_model_id),
        ),
        .belongs_to => findPrimaryKeyOrId(catalog, assoc.target_model_id),
    };
}

fn findPrimaryKeyOrId(
    catalog: *const Catalog,
    model_id: ModelId,
) ?ColumnId {
    const model = &catalog.models[model_id];
    var i: ColumnId = 0;
    while (i < model.column_count) : (i += 1) {
        if (model.columns[i].is_primary_key) return i;
    }
    return catalog.findColumn(model_id, "id");
}

fn findModelForeignKey(
    catalog: *const Catalog,
    model_id: ModelId,
    base_model_name: []const u8,
) ?ColumnId {
    var buf: [96]u8 = undefined;
    const key_name = modelForeignKeyName(base_model_name, &buf) orelse
        return null;
    return catalog.findColumn(model_id, key_name);
}

fn modelForeignKeyName(
    model_name: []const u8,
    out: []u8,
) ?[]const u8 {
    var write_idx: usize = 0;
    var prev_was_lower = false;
    for (model_name, 0..) |ch, i| {
        const is_upper = std.ascii.isUpper(ch);
        if (is_upper and i > 0 and prev_was_lower) {
            if (write_idx >= out.len) return null;
            out[write_idx] = '_';
            write_idx += 1;
        }
        if (write_idx >= out.len) return null;
        out[write_idx] = std.ascii.toLower(ch);
        write_idx += 1;
        prev_was_lower = std.ascii.isLower(ch);
    }
    if (write_idx + 3 > out.len) return null;
    out[write_idx] = '_';
    out[write_idx + 1] = 'i';
    out[write_idx + 2] = 'd';
    write_idx += 3;
    return out[0..write_idx];
}

// --- Tests ---

const testing = std.testing;

test "add and find model" {
    var cat = Catalog{};
    const user_id = try cat.addModel("User");
    const post_id = try cat.addModel("Post");

    try testing.expectEqual(@as(ModelId, 0), user_id);
    try testing.expectEqual(@as(ModelId, 1), post_id);
    try testing.expectEqual(@as(?ModelId, 0), cat.findModel("User"));
    try testing.expectEqual(@as(?ModelId, 1), cat.findModel("Post"));
    try testing.expect(cat.findModel("Comment") == null);
}

test "duplicate model name rejected" {
    var cat = Catalog{};
    _ = try cat.addModel("User");
    try testing.expectError(error.DuplicateName, cat.addModel("User"));
}

test "add and find columns" {
    var cat = Catalog{};
    const uid = try cat.addModel("User");
    const id_col = try cat.addColumn(uid, "id", .bigint, false);
    const email_col = try cat.addColumn(uid, "email", .string, false);
    const name_col = try cat.addColumn(uid, "name", .string, true);

    try testing.expectEqual(@as(ColumnId, 0), id_col);
    try testing.expectEqual(@as(ColumnId, 1), email_col);
    try testing.expectEqual(@as(ColumnId, 2), name_col);

    try testing.expectEqual(@as(?ColumnId, 0), cat.findColumn(uid, "id"));
    try testing.expectEqual(@as(?ColumnId, 1), cat.findColumn(uid, "email"));
    try testing.expect(cat.findColumn(uid, "missing") == null);

    try testing.expectEqual(@as(u16, 3), cat.models[uid].column_count);
    try testing.expectEqual(@as(u16, 3), cat.models[uid].row_schema.column_count);
}

test "add and find index" {
    var cat = Catalog{};
    const uid = try cat.addModel("User");
    const id_col = try cat.addColumn(uid, "id", .bigint, false);
    const email_col = try cat.addColumn(uid, "email", .string, false);

    const col_ids = [_]ColumnId{ id_col, email_col };
    const idx = try cat.addIndex(uid, "user_email_idx", &col_ids, true);

    try testing.expectEqual(@as(IndexId, 0), idx);
    try testing.expectEqual(@as(?IndexId, 0), cat.findIndex(uid, "user_email_idx"));
    try testing.expect(cat.findIndex(uid, "missing") == null);
    try testing.expect(cat.models[uid].indexes[0].is_unique);
}

test "add and find association" {
    var cat = Catalog{};
    const uid = try cat.addModel("User");
    _ = try cat.addColumn(uid, "id", .bigint, false);
    const post_id = try cat.addModel("Post");
    _ = try cat.addColumn(post_id, "user_id", .bigint, false);

    const assoc = try cat.addAssociation(uid, "posts", .has_many, "Post");
    try testing.expectEqual(@as(AssociationId, 0), assoc);
    try testing.expectEqual(@as(?AssociationId, 0), cat.findAssociation(uid, "posts"));

    try cat.resolveAssociations();
    try testing.expectEqual(@as(ModelId, 1), cat.models[uid].associations[0].target_model_id);
}

test "add and find scope" {
    var cat = Catalog{};
    const uid = try cat.addModel("User");

    const scope = try cat.addScope(uid, "active", 42);
    try testing.expectEqual(@as(ScopeId, 0), scope);
    try testing.expectEqual(@as(?ScopeId, 0), cat.findScope(uid, "active"));
    try testing.expectEqual(@as(u16, 42), cat.models[uid].scopes[0].ast_first_op);
}

test "seal prevents additions" {
    var cat = Catalog{};
    _ = try cat.addModel("User");
    cat.seal();
    try testing.expectError(error.CatalogSealed, cat.addModel("Post"));
}

test "stats update functions" {
    var cat = Catalog{};
    const uid = try cat.addModel("User");
    _ = try cat.addColumn(uid, "id", .bigint, false);
    const col_ids = [_]ColumnId{0};
    _ = try cat.addIndex(uid, "pk", &col_ids, true);

    cat.incrementRowCount(uid, 100);
    try testing.expectEqual(@as(u64, 100), cat.models[uid].row_count);

    cat.decrementRowCount(uid, 30);
    try testing.expectEqual(@as(u64, 70), cat.models[uid].row_count);

    cat.updateAvgRowSize(uid, 64);
    try testing.expectEqual(@as(u32, 64), cat.models[uid].avg_row_size_bytes);

    cat.updateIndexStats(uid, 0, 70, 70);
    try testing.expectEqual(@as(u64, 70), cat.models[uid].indexes[0].entry_count);
    try testing.expectEqual(@as(u64, 70), cat.models[uid].indexes[0].distinct_count);
}

test "model name retrieval" {
    var cat = Catalog{};
    _ = try cat.addModel("User");
    _ = try cat.addModel("Post");

    try testing.expectEqualSlices(u8, "User", cat.getModelName(0));
    try testing.expectEqualSlices(u8, "Post", cat.getModelName(1));
}

test "primary key flag" {
    var cat = Catalog{};
    const uid = try cat.addModel("User");
    const id_col = try cat.addColumn(uid, "id", .bigint, false);
    cat.setColumnPrimaryKey(uid, id_col);
    try testing.expect(cat.models[uid].columns[0].is_primary_key);
}

test "resolve missing association target fails" {
    var cat = Catalog{};
    const uid = try cat.addModel("User");
    _ = try cat.addAssociation(uid, "posts", .has_many, "Post");
    try testing.expectError(error.ModelNotFound, cat.resolveAssociations());
}

test "resolve associations infers default key columns" {
    var cat = Catalog{};
    const user_id = try cat.addModel("User");
    const user_pk = try cat.addColumn(user_id, "id", .bigint, false);
    cat.setColumnPrimaryKey(user_id, user_pk);

    const post_id = try cat.addModel("Post");
    _ = try cat.addColumn(post_id, "id", .bigint, false);
    const post_fk = try cat.addColumn(post_id, "user_id", .bigint, false);

    _ = try cat.addAssociation(user_id, "posts", .has_many, "Post");
    try cat.resolveAssociations();

    const assoc = cat.models[user_id].associations[0];
    try testing.expectEqual(user_pk, assoc.local_column_id);
    try testing.expectEqual(post_fk, assoc.foreign_key_column_id);
}

test "resolve associations keeps explicit key metadata" {
    var cat = Catalog{};
    const user_id = try cat.addModel("User");
    _ = try cat.addColumn(user_id, "id", .bigint, false);

    const post_id = try cat.addModel("Post");
    _ = try cat.addColumn(post_id, "id", .bigint, false);
    const owner_fk = try cat.addColumn(post_id, "owner_id", .bigint, false);

    const assoc_id = try cat.addAssociation(user_id, "posts", .has_many, "Post");
    try cat.setAssociationKeys(user_id, assoc_id, "id", "owner_id");
    try cat.resolveAssociations();

    const assoc = cat.models[user_id].associations[assoc_id];
    try testing.expectEqual(@as(ColumnId, 0), assoc.local_column_id);
    try testing.expectEqual(owner_fk, assoc.foreign_key_column_id);
}

test "resolve associations fails when inferred key columns are missing" {
    var cat = Catalog{};
    const user_id = try cat.addModel("User");
    _ = try cat.addColumn(user_id, "pk", .bigint, false);

    _ = try cat.addModel("Post");
    _ = try cat.addAssociation(user_id, "posts", .has_many, "Post");
    try testing.expectError(error.ColumnNotFound, cat.resolveAssociations());
}
