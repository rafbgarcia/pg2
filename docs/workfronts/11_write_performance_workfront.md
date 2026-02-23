# Workfront 11: Write Performance — Primary Key Indexing and WAL Batching

## Objective
Eliminate O(n²) insert degradation by wiring the existing B+ tree into primary key constraint enforcement and query execution, and reduce per-insert WAL overhead through group commit.

## Why
- INSERT currently performs a full heap scan per row to enforce primary key uniqueness (`constraints.zig:enforceInsertUniqueness` → `rowExistsForValue`). Inserting N rows costs O(N²) row decodings — 4200 sequential inserts trigger ~8.8 million row comparisons.
- The B+ tree implementation (`src/storage/btree.zig`) is complete: insert, find, delete, range scan, splits, WAL integration, and buffer pool eviction recovery are all tested. But no executor path uses it.
- `primaryKey` in the schema is stored as a boolean flag (`ColumnInfo.is_primary_key`) with no backing index structure. `IndexInfo` already has a `btree_root_page_id` field, but no index is created for primary keys.
- WAL commits flush (fsync) on every transaction commit (`wal.commitTx`). Sequential single-row inserts pay one fsync each — 4200 inserts = 4200 fsyncs.
- Both bottlenecks compound: stress tests that insert thousands of rows take 20+ seconds where sub-second is expected.

## Dependencies
- None. The B+ tree storage layer and catalog structures are already in place.

## Non-Goals
- Secondary index creation syntax or user-declared indexes (future workfront).
- Composite primary keys (current `primaryKey` is single-column only).
- Index-only scans or covering indexes.
- Concurrent index builds or online reindexing.

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

## Phase 3: WAL Group Commit

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

### Gate
- Unit tests: WAL accumulates multiple commit records without fsyncing until threshold.
- Unit tests: explicit COMMIT always triggers immediate flush.
- Integration: batch of 1000 single-row inserts produces far fewer than 1000 fsyncs.
- Durability: crash after flush recovers all committed transactions. Crash before flush loses only uncommitted transactions (correct behavior).
- Regression: all existing tests pass (tests that check WAL behavior may need adjustment for deferred flush).
- Performance: 4200 sequential inserts see measurable improvement from reduced fsync count.
- Determinism: WAL batching behavior is deterministic under simulation.

## Phase 4: Bulk Insert Path

### Design Decisions
- **Multi-row insert syntax.** Support inserting multiple rows in a single statement to amortize parse/plan overhead and enable batch index maintenance.
- **Batch B+ tree insert.** When inserting multiple rows, sort keys and insert into the B+ tree in sorted order. This minimizes page splits and improves locality (sequential inserts into the rightmost leaf instead of random access).
- **Single WAL flush per batch.** A multi-row insert is a single transaction with one commit + one fsync, regardless of row count.

### Scope
- Parser support for multi-row insert syntax.
- Executor batches heap writes + B+ tree inserts within a single transaction.
- WAL flushes once at transaction commit.

### Gate
- Integration: multi-row insert of 1000 rows completes correctly with all rows in heap and PK index.
- Performance: multi-row insert is significantly faster than 1000 individual inserts.
- Determinism: bulk insert produces identical results under simulation replay.
