# Workfront 11: Write Performance — Primary Key Indexing and WAL Batching

## Objective
Eliminate O(n²) insert degradation by wiring B+ tree indexes into constraint enforcement, query execution, and bulk mutation paths. Reduce per-insert WAL overhead through group commit.

## Current State
- **Phases 1-4 complete.** PK B+ tree indexes are auto-created and used for uniqueness checks (O(log n) vs O(n²) heap scan), PK index-assisted point/range scans work, WAL group commit reduces fsyncs from N to ~1 for N sequential inserts, and all unique indexes (PK and non-PK) now have B+ trees for O(log n) constraint enforcement including FK checks.
- **Phase 4 follow-up fix complete.** Non-PK unique checks are now MVCC-aware when using B+ trees (dead entries from deleted/invisible rows no longer cause false duplicates), and dedicated Phase 4 feature coverage is in place.
- **Phase 5 next.** Multi-row INSERT syntax and bulk insert path.

## Why (original motivation, for context)
- INSERT used to perform a full heap scan per row to enforce PK uniqueness. Inserting N rows cost O(N²). 4200 inserts triggered ~8.8M row comparisons. **Fixed by Phase 1.**
- WAL committed (fsync) on every transaction. 4200 inserts = 4200 fsyncs. **Fixed by Phase 3.**
- FK and non-PK unique checks used to do full heap scans per row. **Fixed by Phase 4.**
- No multi-row INSERT syntax exists. **Phase 5 target.**

## Dependencies
- None. The B+ tree storage layer and catalog structures are already in place.

## Non-Goals
- Secondary index creation syntax or user-declared indexes (future workfront).
- Composite primary keys (current `primaryKey` is single-column only).
- Composite unique index key encoding (deferred — single-column unique indexes only for Phase 4).
- Index-only scans or covering indexes.
- Concurrent index builds or online reindexing.
- Free Space Map (running page hint is sufficient for bulk insert).

## Phase 1: Automatic PK Index Creation and Maintenance ✅

### Design Decisions
- **PK gets an implicit `IndexInfo` entry.** When `applyDefinitions` processes a model with a `primaryKey` column, it auto-creates an `IndexInfo` with `is_unique = true` and allocates a B+ tree root page. This mirrors PostgreSQL's behavior where a PRIMARY KEY constraint implicitly creates a unique index.
- **Key encoding.** The B+ tree operates on `[]const u8` keys. PK values (`Value` union) must be serialized to a byte representation that preserves sort order. For `i64`: big-endian with sign bit flipped (so lexicographic order matches numeric order). For `string`: raw bytes. For other types: type-specific encoding added as needed.
- **Index maintenance on mutation (PostgreSQL-style).** INSERT adds to the B+ tree after heap write. DELETE does NOT remove from the B+ tree — dead entries stay and MVCC on the heap determines visibility. UPDATE on a PK column inserts the new key; the old entry remains as a dead pointer. Dead entries are cleaned up opportunistically during INSERT uniqueness checks (when a key maps to an invisible row, the dead entry is removed and the key is reused).
- **B+ tree page allocation.** Each model's PK index gets a dedicated page region, tracked via `IndexInfo.btree_root_page_id`. Page allocation uses the existing `BTree.allocPage()` mechanism. The catalog must track the next available page ID for index pages so new inserts can allocate split pages.

### Scope
- Extend `applyDefinitions` (or the schema loader path) to allocate a B+ tree root page and create an `IndexInfo` entry for columns marked `primaryKey`.
- Implement key encoding functions (`Value` → `[]const u8`) with sort-preserving properties for `i64` and `string` types.
- Wire INSERT in `mutation.zig` to insert into the PK B+ tree after successful heap write. MVCC-aware uniqueness check (`primaryKeyVisibleInIndex`) follows B+ tree entries to the heap and checks snapshot visibility — dead entries from committed deletes are cleaned up and the key is reused.
- DELETE leaves B+ tree entries intact (PostgreSQL-style). MVCC visibility on the heap handles aborted/committed deletes correctly.
- Wire UPDATE (when PK column changes) to insert new key. Old entry stays as dead pointer.
- Replace `enforceInsertUniqueness` heap scan with a B+ tree `find()` call — O(log n) instead of O(n).

### Gate
- Unit tests: key encoding round-trip and sort order for i64 (positive, negative, zero, MIN, MAX) and string types.
- Unit tests: PK index auto-creation on schema apply — `IndexInfo` populated, `btree_root_page_id` non-zero.
- Integration: inserting N rows with a PK constraint uses B+ tree lookup, not heap scan. Duplicate key still rejected.
- Integration: DELETE leaves B+ tree entry intact; re-inserting the same key succeeds (MVCC-aware uniqueness check detects the invisible row and cleans up the dead entry).
- Regression: all existing feature and stress tests pass.
- Performance: 4200 sequential inserts complete in under 2 seconds (down from 20+).
- Determinism: PK index operations produce identical results under simulation replay.

## Phase 2: PK Index-Assisted Scan ✅

### Design Decisions
- **Point lookup.** `WHERE id == <value>` on a PK column uses `BTree.find()` to get the `RowId`, then fetches the single heap row directly — O(log n) instead of a full table scan.
- **Range scan.** `WHERE id > X && id < Y` on a PK column uses `BTree.rangeScan()` to iterate matching `RowId`s and fetch heap rows on demand. Handles `>`, `>=`, `<`, `<=` with correct successor-key encoding for inclusive/exclusive bounds.
- **Planner integration.** `index_scan_planner.zig` analyzes the WHERE predicate AST for PK-indexable patterns (equality, range, AND combinations, flipped operands like `5 > id`). The executor's `tryIndexScan` dispatches to `executePointLookup` or `executeRangeScan`, skipping the chunked table scan loop entirely. Non-PK filters fall back to the existing heap scan.
- **MVCC correctness.** Index scans follow B+ tree entries to the heap and apply `resolveVisibleVersion` — dead entries from committed deletes or aborted inserts are correctly filtered. This is critical because DELETE no longer removes B+ tree entries (Phase 1 design).
- **Scan cursor interaction.** Index scans bypass the chunked heap scan loop entirely — the B+ tree iterator is the driving cursor. Results feed into the same `SpillingResultCollector` pipeline.

### Scope
- `index_scan_planner.zig`: WHERE clause analysis for PK scan strategy (table_scan, pk_point_lookup, pk_range_scan).
- `scan.zig`: `indexFind`, `indexRange`, `indexRangeScanInto` — B+ tree scan paths with MVCC visibility.
- `executor.zig`: `tryIndexScan` wired into read pipeline, `scan_strategy` added to `PlanStats`.
- Ensure the index scan path works correctly with the spill/degrade pipeline (Phase 2 of workfront 03).

### Bugs fixed during implementation
- **`syncBTreeState` missing `root_page_id`.** After a B+ tree root split, only `next_page_id` was synced to the catalog. The root page ID was lost, causing lookups to fail on tables with enough inserts to trigger root splits. Pre-existing Phase 1 bug, surfaced by Phase 2 reads.

### Gate
- Integration: `WHERE id = X` on a PK column returns the correct row via index lookup (INSPECT shows index scan).
- Integration: `WHERE id > X AND id < Y` returns correct range via index range scan.
- Integration: `WHERE id = X AND other_col = Y` uses index for PK, then filters remaining predicate on the fetched row.
- Integration: query on a non-PK column still uses full table scan.
- Performance: point lookup on a 10,000-row table completes in microseconds, not milliseconds.
- Regression: all existing tests pass.

## Phase 3: WAL Group Commit ✅

### Design Decisions
- **Deferred fsync.** Instead of fsyncing on every `commitTx`, the WAL accumulates commit records and flushes on a configurable trigger: either a byte threshold (e.g., 64 KB of WAL data) or when explicitly requested (e.g., at session boundary or explicit FLUSH).
- **Single-statement auto-commit batching.** For single-statement auto-commit transactions (the common INSERT case), the WAL batches multiple commits and flushes once at the end of a request batch or when the buffer fills. This turns N inserts from N fsyncs into ~1 fsync.
- **Durability contract.** A committed transaction is durable only after the WAL flush that includes its commit record. For single-row auto-commit inserts, the response is sent after the flush — the batching is transparent to the client. For explicit transactions (BEGIN/COMMIT), COMMIT always triggers an immediate flush (matches PostgreSQL's `synchronous_commit = on` default).
- **Configuration.** `BootstrapConfig` gains `wal_flush_threshold_bytes` (default 64 KB). This is a tuning knob, not an architecture change.

### Scope
- Refactor `Wal.commitTx` to append without flushing. Add `Wal.flushIfNeeded()` that checks the byte threshold.
- Add `Wal.forceFlush()` for explicit transaction commit and session teardown.
- Wire the session/executor layer to call `flushIfNeeded()` after each auto-commit and `forceFlush()` after explicit COMMIT.
- Add `wal_flush_threshold_bytes` to `BootstrapConfig`.

### Implementation Notes
- `commitTx()` appends the commit record without flushing. `flushIfNeeded()` gates on `buffer_len >= flush_threshold_bytes` (threshold=0 always flushes for backward compat). `forceFlush()` flushes unconditionally.
- `pool.checkin()` calls `flushIfNeeded()` — the group commit gate for auto-commit transactions.
- `session.serveConnection()` calls `forceFlush()` on connection close to ensure all deferred commits are durable.
- `BootstrapConfig.wal_buffer_capacity_bytes` default raised from 8 KB to 128 KB to accommodate deferred flush accumulation.
- Test harnesses (`TestExecutor.run()`) call `forceFlush()` after checkin to preserve per-operation durability semantics expected by feature/stress tests.
- Fault matrix and FK fault matrix tests updated with explicit `flush()`/`forceFlush()` calls since `commitTx` no longer flushes.
- Durability semantics for auto-commit: `synchronous_commit = off` — response may be sent before WAL is durable, but flush arrives within the next threshold cycle or at session close.

### Gate
- Unit tests: WAL accumulates multiple commit records without fsyncing until threshold.
- Unit tests: explicit COMMIT always triggers immediate flush.
- Integration: batch of 1000 single-row inserts produces far fewer than 1000 fsyncs.
- Durability: crash after flush recovers all committed transactions. Crash before flush loses only uncommitted transactions (correct behavior).
- Regression: all existing tests pass (tests that check WAL behavior may need adjustment for deferred flush).
- Performance: 4200 sequential inserts see measurable improvement from reduced fsync count.
- Determinism: WAL batching behavior is deterministic under simulation.

## Phase 4: Index-Backed Constraint Enforcement ✅

### Why
Constraint enforcement is the dominant per-row cost in the INSERT pipeline. Two paths use O(n) heap scans where O(log n) index lookups are possible:

1. **FK checks** (`referential_integrity.zig:enforceOutgoingReferentialIntegrity`, line 35): calls `rowExistsForValue()` — full heap scan of the parent table per FK per row. But `belongs_to` FKs always reference the target's PK column (resolved via `inferAssociationForeignKey` → `findPrimaryKeyOrId` at `catalog.zig:791`), and the target table already has a PK B+ tree index from Phase 1. This is a missing wiring problem: the B+ tree exists, the code just doesn't use it.

2. **Non-PK unique index checks** (`constraints.zig:enforceNonPkUniqueness`, line 50): calls `rowExistsForUniqueIndex()` — full heap scan per unique index per row. The catalog creates `IndexInfo` entries for unique indexes via `schema_loader.zig:loadIndex()` (line 349), but never allocates B+ tree pages — `btree_root_page_id` stays 0 (default in `IndexInfo` at `catalog.zig:61`). The B+ tree infrastructure exists but is only wired for PK. These indexes need real B+ trees.

Without fixing these, Phase 5's bulk insert would need batch deduplication hacks to work around O(n)-per-check architecture. With index-backed constraints, each check is O(log n), making per-row enforcement fast enough that no deduplication is needed.

### Dependencies
- Phase 1 (PK B+ tree infrastructure must exist).

### Architecture: Schema Load vs Index Initialization

`schema_loader.zig:loadSchema()` takes `(catalog, ast, tokens, source)` — no runtime access (no pool, no wal). It populates catalog metadata only. B+ tree allocation requires runtime (pool + wal to pin pages, write leaf headers).

Currently, PK B+ tree allocation lives **only in the test harness** (`test_env_test.zig:applyDefinitions()`, lines 68-90). It runs after `loadSchema()` using `self.runtime.pool` and `self.runtime.wal`. There is no production equivalent.

**Solution: new `catalog.initializeIndexTrees(pool, wal)` function.** This iterates all models, finds all unique indexes (PK and non-PK) with `btree_root_page_id == 0`, and allocates B+ trees for them. Called:
- In the test harness: after `loadSchema()` (replaces the existing PK-only manual loop).
- In production: after schema load during bootstrap (in `main.zig` or wherever the server starts accepting queries).

This maintains the clean separation: `loadSchema()` = AST → catalog metadata (pure, no I/O). `initializeIndexTrees()` = catalog → B+ tree pages (needs runtime).

**Page allocation strategy**: Follow the existing test harness convention extended to multiple indexes. Each model gets a page region for all its indexes. The catalog's `IndexInfo.btree_root_page_id` and `btree_next_page_id` track each index's page range independently. `BTree.init()` takes a `start_page_id` and sets `next_page_id = start_page_id + 1`. `BTree.allocPage()` increments `next_page_id` for splits.

### Design Decisions

- **FK checks use the target table's PK index.** In `enforceOutgoingReferentialIntegrity`: check if `foreign_key_column_id` equals the target model's PK column (`findPrimaryKeyColumnId`). If yes and target has PK B+ tree (`btree_root_page_id != 0`), call `primaryKeyExists()` — O(log n). If no PK index, fall back to `rowExistsForValue()` heap scan for correctness.

- **Non-PK unique indexes get real B+ trees.** `initializeIndexTrees()` allocates a B+ tree for every unique index that lacks one. Uses same `BTree.init(pool, wal, start_page_id)` pattern as PK indexes from Phase 1.

- **Generalize `index_maintenance.zig` beyond PK-only.**
  - New `openIndex(catalog, model_id, index_id) → ?BTree` — generic; returns null if `btree_root_page_id == 0`.
  - New `insertIndexKey(catalog, btree, model_id, index_id, key_value, row_id)` — encode key, insert, sync state.
  - Existing `openPrimaryKeyIndex()` / `insertPrimaryKey()` become thin wrappers.
  - INSERT maintains ALL unique indexes after heap write (not just PK).
  - DELETE leaves entries intact (same PostgreSQL-style tombstone approach as PK).
  - UPDATE on indexed columns inserts new key; old entry stays as dead pointer.

- **`enforceNonPkUniqueness` uses MVCC-aware B+ tree visibility checks when available.** When `btree_root_page_id != 0` and `column_count == 1`, use index lookup plus heap visibility (`indexKeyVisibleInIndex`) instead of `rowExistsForUniqueIndex()` heap scan. Invisible/deleted rows trigger dead-index-entry cleanup. Fall back to heap scan when no B+ tree is present (safety net for indexes created before Phase 4).

- **Single-column unique indexes only (this phase).** Composite unique indexes (`column_count > 1`) require multi-column key encoding (concatenated sort-preserving encoding with length prefixes). This is deferred — composite unique indexes continue using the heap scan path. PK is already single-column only (workfront non-goal).

### Scope

**New file or section in `src/catalog/catalog.zig`**:
- New `initializeIndexTrees(catalog, pool, wal)`: iterate all models and all unique indexes. For each with `btree_root_page_id == 0`: allocate a B+ tree via `BTree.init()`, set `btree_root_page_id` and `btree_next_page_id` on the `IndexInfo`.

**`src/executor/referential_integrity.zig`**:
- In `enforceOutgoingReferentialIntegrity` (line 60): check if `foreign_key_column_id` is the target's PK and target has PK index. If yes, call `primaryKeyExists()`. If no, existing `rowExistsForValue()`.

**`src/executor/index_maintenance.zig`**:
- New `openIndex(catalog, model_id, index_id) → ?BTree`.
- New `insertIndexKey(catalog, btree, model_id, index_id, key_value, row_id)`.
- Refactor `openPrimaryKeyIndex` / `insertPrimaryKey` as wrappers.

**`src/executor/mutation.zig`**:
- In `executeInsertWithDiagnosticAndParameters` (after line 406): loop over all unique indexes for the model, call `insertIndexKey()` for each with a B+ tree.

**`src/executor/constraints.zig`**:
- In `enforceNonPkUniqueness` (line 60): when index has `btree_root_page_id != 0` and `column_count == 1`, open B+ tree and use `find()`. Otherwise, existing heap scan.

**`test/features/test_env_test.zig`**:
- Replace the manual PK-only B+ tree loop (lines 68-90) with a call to `initializeIndexTrees()`.

### Gate
- Unit tests: `initializeIndexTrees` allocates B+ trees for all unique indexes — `btree_root_page_id != 0` after call.
- Unit tests: `initializeIndexTrees` is idempotent — calling twice doesn't re-allocate.
- Integration: FK check on `belongs_to` uses PK index lookup (not heap scan). Insert with valid FK passes, invalid FK fails.
- Integration: non-PK unique constraint enforced via B+ tree — duplicate rejected, unique values accepted.
- Integration: INSERT maintains all unique indexes — post-insert, B+ tree `find()` on the unique column returns the correct RowId.
- Integration: DELETE leaves unique index entries intact; re-inserting the same unique value succeeds after delete (MVCC-aware check detects invisible row).
- Regression: all existing feature and stress tests pass (critical: existing PK-only behavior unchanged).
- Determinism: index-backed constraint checks produce identical results under simulation replay.

### Follow-up (post-Phase 4)
- Commit `3ba7439` fixed MVCC visibility handling for non-PK unique index checks by threading snapshot/undo/tx-manager context through insert uniqueness enforcement and using `indexKeyVisibleInIndex` for B+ tree-backed checks.
- Added dedicated Phase 4 integration suite at `test/features/constraints/index_backed_constraints_test.zig` and wired it into `test/features/features_specs_test.zig`.

## Phase 5: Bulk Insert Path

### Dependencies
- Phase 4 (index-backed constraints make per-row enforcement O(log n), removing the need for batch deduplication hacks).

### Design Decisions

- **Multi-row insert syntax.** Repeated named assignments within the existing pipe grammar:
  ```
  User |> insert(
    (id = 1, name = "Alice"),
    (id = 2, name = "Bob")
  ) { id name }
  ```
  Single-row INSERT remains unchanged: `User |> insert(id = 1, name = "Alice") {}`. Parser disambiguation: after `insert(`, if the next token is `(` followed by identifier followed by `=`, enter multi-row path. Otherwise, single-row.

- **AST capacity.** Multi-row INSERT needs ~(2×cols+1) AST nodes per row (1 `insert_row_group` + N `assignment` + N expression nodes). Current `max_ast_nodes = 1024` limits batches to ~144 rows for a 3-column table. Raise to 8192 (64 KB stack cost per parse, u16 NodeIndex supports up to 65535). This allows ~1170 rows for 3 columns.

- **Per-row constraint enforcement.** With Phase 4's index-backed constraints, each check is O(log n). No batch deduplication needed — just call the same constraint functions per row. PK uniqueness: check in-batch duplicates (linear scan of collected PK values), then B+ tree via `primaryKeyVisibleInIndex()`. FK and non-PK unique: same per-row functions, now backed by indexes.

- **Heap page allocation hint.** `findPageWithSpace()` scans backward from the last page on every call. For a 1000-row batch this re-scans the same pages repeatedly. The batch loop tracks the last successfully used page_id and starts subsequent scans from there.

- **Two-phase batch execution.**
  1. *Per-row heap insertion*: build values, enforce all constraints (PK + FK + unique — all O(log n) via indexes), overflow handling, heap write, WAL append. Running page hint avoids repeated scans. Store `(pk_value, row_id)` pair in batch array.
  2. *Sorted B+ tree index insertion*: sort collected PK values by key, open each unique index's B+ tree once, insert all keys in sorted order, sync state once per index. Sorted insertion targets the rightmost leaf for sequential keys — fewer page splits and better buffer pool locality.

- **Single WAL flush per batch.** A multi-row insert is a single transaction. Phase 3's deferred fsync means: N WAL append records (one per row) + one commit record + one conditional flush at `pool.checkin()`. No additional work needed — this is free from Phase 3.

- **Error semantics.** Any row failure (duplicate key, FK violation, constraint error) aborts the entire batch. Partial heap writes are invisible because the WAL commit never happens. B+ tree index insertion (the second phase of batch execution) hasn't run yet, so no index cleanup is needed. This matches PostgreSQL's all-or-nothing batch INSERT semantics.

### Scope

**`src/parser/ast.zig`**:
- Add `insert_row_group` to `NodeTag` enum: `data.unary` = first assignment (linked by `.next`); row groups linked to each other via `.next`.
- Raise `max_ast_nodes` from 1024 to 8192.

**`src/parser/parser_ops.zig`**:
- Extract `parseAssignmentList()` helper from existing `parseMutationOp()` code (lines 260-294) to share between single-row and multi-row paths.
- Add multi-row detection in `parseMutationOp()`: after consuming `insert(`, 2-token lookahead (`(` + identifier + `=`) disambiguates multi-row from single-row.
- New `parseMultiRowInsert()`: loop over `(assignment_list)` groups separated by commas, each producing an `insert_row_group` node. Final `op_insert` node's `data.unary` points to first row group.

**`src/executor/executor.zig`**:
- In `.insert_op` branch: check if `node.data.unary` points to `insert_row_group` tag → dispatch to `mutation_mod.executeBulkInsert()`. Collect row count and RowId array. RETURNING: `materializeRowsById()` with all collected RowIds.

**`src/executor/mutation.zig`**:
- New `executeBulkInsert()` implementing the two-phase batch execution described above.
- `findPageWithSpace` hint: batch loop tracks last page_id, passes as starting point for subsequent calls.

**`src/executor/index_maintenance.zig`**:
- New `insertPrimaryKeyBatch()`: takes pre-sorted PK values + RowIds, calls `btree.insert()` for each, calls `syncBTreeState()` once at the end (not per-insert).
- Extend to insert into all unique indexes in sorted order (not just PK).

### Gate
- Unit tests: parser produces `op_insert` → `insert_row_group` chain for multi-row syntax; single-row syntax unchanged; parenthesized expressions `insert(id = (1+2))` stay single-row.
- Integration: multi-row insert of 3 rows returns correct `inserted_rows=3`.
- Integration: multi-row insert with RETURNING (`{ id name }`) returns all inserted rows.
- Integration: in-batch PK duplicate `(id=1), (id=1)` rejected with `DuplicateKey`.
- Integration: PK duplicate against existing row rejected.
- Integration: FK enforcement in batch — valid FK values pass, invalid FK values fail.
- Integration: omitted columns receive defaults.
- Integration: PK index correctness — bulk insert 100 rows, each findable via `WHERE id == X`.
- Integration: all unique indexes maintained — post-bulk-insert, unique column lookups via B+ tree return correct RowIds.
- Integration: multi-row insert of 1000 rows completes correctly with all rows in heap, PK index, and all unique indexes.
- Performance: multi-row insert of 1000 rows is significantly faster than 1000 individual inserts.
- Regression: all existing feature and stress tests pass.
- Determinism: bulk insert produces identical results under simulation replay.

## Phase 6: B+ Tree Bulk Insert Cursor

### Why
Phase 5 sorts PK keys before B+ tree insertion, which ensures sequential keys target the rightmost leaf and reduces page splits. However, `btree.insert()` still traverses root→leaf per key — rebuilding the full path for every insert. For sorted batch insertion, consecutive keys almost always land in the same leaf. A cursor that keeps the current leaf pinned and attempts a fast-path insert before falling back to full traversal eliminates redundant internal-node lookups.

### Dependencies
- Phase 5 (bulk insert path must exist to benefit from cursor).

### Design Decisions
- **Leaf cursor with fast-path.** After inserting key K into leaf L, the cursor retains L's page_id. For the next key K+1, it pins L directly and checks if K+1 belongs in L (key ≤ leaf's high key). If yes, insert without traversal. If no (key crossed a leaf boundary or triggered a split), fall back to full root→leaf traversal and update the cursor.
- **Scope limited to sorted batch insert.** The cursor API is used only by `insertPrimaryKeyBatch()`. Single-row `insertPrimaryKey()` continues using the existing root→leaf traversal — the cursor overhead is not worth it for one key.
- **No API change to `BTree.insert()`.** Add a new `BTree.insertWithHint(key, row_id, *LeafHint)` that accepts and updates an optional leaf hint. `insertPrimaryKeyBatch()` calls this in a loop, threading the hint through.
- **Split handling.** If the fast-path insert triggers a leaf split, the cursor is invalidated (set to null) and the next insert falls back to full traversal. This is rare for sorted insertion since splits create a new rightmost leaf which becomes the new cursor target.

### Scope
- `src/storage/btree.zig`: new `insertWithHint()` method and `LeafHint` struct.
- `src/executor/index_maintenance.zig`: `insertPrimaryKeyBatch()` uses `insertWithHint()` instead of `insert()`.

### Gate
- Unit tests: `insertWithHint` produces identical tree structure as `insert` for the same key sequence.
- Unit tests: cursor fast-path is taken for consecutive sorted keys (leaf re-pinned, no root traversal).
- Unit tests: cursor invalidation on split falls back to full traversal correctly.
- Integration: bulk insert of 1000 sorted rows with cursor produces identical results to Phase 5 without cursor.
- Performance: measurable reduction in buffer pool pin count for sorted batch inserts.
- Regression: all existing tests pass.
- Determinism: cursor behavior is deterministic under simulation replay.
