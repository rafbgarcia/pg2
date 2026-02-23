# Workfront 03: Degrade-First Execution and Spill

## Objective
Queries should degrade performance under memory pressure before failing, using pg2 storage abstractions.

## Non-Negotiables
1. Core logic must use `Storage` abstraction for temp/spill I/O.
2. Fail only on hard-stop conditions (disk failures, corruption, impossible resource limits).
3. Spill behavior must be deterministic in simulation.

## Phase 1: Temp/Spill Storage Foundation ✅
### Scope
- `src/storage/temp.zig`: `TempPageAllocator` (per-query-slot monotonic page-id allocator with O(1) bulk reset), `TempPage` on-disk format (magic `0x5432`, 14-byte header, 8154-byte max payload), `TempStorageManager` (direct `Storage` I/O coordinator with stats tracking).
- `PageType.temp = 5` added to the on-disk page type enum.
- Per-slot disjoint temp page-id regions starting at `20_000_000`, sized by `BootstrapConfig.temp_pages_per_query_slot` (default 1024 pages = 8 MB).
- Temp pages bypass buffer pool and WAL — query-scoped, ephemeral, no durability.
- `ExecContext` carries `storage` and `query_slot_index` so operators can construct `TempStorageManager` on demand.
- Spill telemetry counters on `ExecStats` (`temp_pages_allocated`, `temp_pages_reclaimed`, `temp_bytes_written`, `temp_bytes_read`) with saturating accumulation across multi-statement execution. These raw counters are the source of truth consumed by Workfront 04's metrics contract.
- `INSPECT spill` line in session output exposes all four counters.

### Gate
- Unit tests for allocation, reclaim, fault handling, page format validation (`src/storage/temp.zig` inline tests).
- Integration tests: buffer pool bypass, per-slot isolation, checksum corruption detection, crash-drops-temp-data, stats accumulation, region exhaustion, INSPECT output (`test/internals/spill/temp_storage_surface_test.zig`).
- Simulation replay determinism for spill operations and fault injection (`test/internals/spill/temp_spill_determinism_test.zig`).

## Phase 2: Scan/Materialization Degrade Path

### Design Decisions Locked
- **Byte-budget spill, not row-count spill.** The current `max_result_rows = 4096` hard cap is a row-count proxy for memory. The real constraint is bytes. Spill triggers when the accumulated *serialized* data size of result rows exceeds the per-slot work memory budget, OR when the physical result buffer (4096 slots) is full — whichever comes first. Narrow rows hit the physical buffer limit first (common OLTP case, efficient). Wide rows hit the byte budget first (protects against memory blowout on wide schemas).
- **Chunk-and-filter-then-spill.** Scan fills the physical buffer in chunks of up to 4096 rows. Each chunk runs through WHERE/project in-place. Only surviving rows are serialized and spilled to temp pages. This avoids wasting the temp page budget on rows the filter will discard — critical since temp pages per slot are finite (default 1024 pages = 8 MB).
- **`work_memory_bytes_per_slot` config field.** Added to `BootstrapConfig` with a hardcoded default of 4 MB (matching PostgreSQL's `work_mem`). Workfront 02's planner will later derive this from `--memory` and `--concurrency`. The spill mechanism is agnostic to the source of the budget.
- **Arena safety valve instead of string spill path.** Rather than a dedicated string spill mechanism, the executor force-flushes the collector's hot batch and resets the string arena when remaining arena capacity drops below 10%. This keeps string materialization simple — strings are serialized inline during spill and deserialized into a caller-provided arena during iteration.

### Scope

#### 2a: Compact Row Serialization for Temp Pages ✅
- `src/storage/spill_row.zig`: compact serialization format for `ResultRow` data to temp pages (only actual column values, not 128-slot fixed structs). Multiple rows per 8 KB temp page. String data inline. Round-trip serialize/deserialize is deterministic for simulation replay.
- `spillRowSize()` tracks serialized byte size per row so the byte budget can be enforced accurately.

#### 2b: `SpillingResultCollector` ✅
- `src/executor/spill_collector.zig`.
- Wraps the existing pre-allocated `result_rows` buffer (as a "hot batch") + a `TempStorageManager` for overflow.
- Accepts rows via `appendRow()`. Tracks accumulated serialized bytes against `work_memory_bytes_per_slot`. When the hot batch is full or byte budget exceeded, serializes the batch to temp pages via 2a's format, resets the buffer, continues accepting.
- Provides `iterator()` to read back all rows: spilled batches (in spill order) then in-memory remainder. Iterator deserializes from temp pages on demand.
- `reset()` reclaims all temp pages via `TempPageAllocator.reset()`.
- `flushHotBatch()` is public so the executor can force-flush as an arena safety valve.

#### 2c: Chunked Scan Loop ✅
- Renamed `max_result_rows` to `scan_batch_size` to reflect its new role: it is the physical working buffer size for a single scan chunk, NOT a cap on total query results. Total result size is bounded by the byte budget + temp page budget.
- `tableScanInto()` in `scan.zig` accepts an optional `ScanCursor` parameter. When non-null, the scan resumes from the cursor position and sets `done = true` when all pages are exhausted.
- The executor's `executeReadPipeline()` drives the chunk loop: scan chunk (into `scratch_rows_a`) → apply WHERE in-place on chunk → feed survivors to `SpillingResultCollector` (whose hot batch is `result.rows`) → next chunk.
- For queries without sort/group/join, the collector holds the complete result after all chunks.
- For queries with sort/group/join, the scan still produces all input rows (no silent truncation), but operator capacity limits remain until Phase 3 adds operator-specific spill. When spill occurs with full-input operators, rows are reloaded from the collector (up to `scan_batch_size`) for post-scan processing.
- Three post-scan paths: (a) no spill — fast path with rows in `result.rows`; (b) spill + no GROUP/SORT — serialization iterates directly from collector; (c) spill + GROUP/SORT — reload from collector into `result.rows` then run post-scan operators.
- **Deferred**: `indexRange()` is not yet chunked; it still does a single-shot scan capped at `scan_batch_size`.

#### 2d: ExecContext and Pipeline Integration ✅
- `work_memory_bytes_per_slot` added to `BootstrapConfig` (default `4 * 1024 * 1024`).
- `BootstrappedRuntime` allocates per-slot `SpillingResultCollector` array. `QueryBuffers` carries collector pointer and `work_memory_bytes_per_slot`.
- Plumbed through `QueryBuffers` → `ExecContext` (gains `collector: *SpillingResultCollector` and `work_memory_bytes_per_slot: u64`).
- `QueryResult` gains `collector: ?*SpillingResultCollector = null`. When non-null, `serializeQueryResult()` iterates from the collector instead of the flat `result_rows` slice and uses `collector.totalRowCount()` for the OK header.

#### 2e: Telemetry ✅
- `ExecStats` gains `spill_triggered: bool` and `result_bytes_accumulated: u64`.
- `INSPECT spill` output includes whether the query degraded and total bytes accumulated.
- Existing temp page counters (`temp_pages_allocated`, `temp_bytes_written`, etc.) track spill volume.

### Gate
- ✅ Unit tests: row serialization round-trip (`src/storage/spill_row.zig` inline tests).
- ✅ Unit tests: collector flush/iterate, byte budget enforcement, multi-batch spill, reset/reuse, deterministic roundtrip (`src/executor/spill_collector.zig` inline tests).
- ✅ Unit tests: ScanCursor chunked scan, cross-page boundaries, empty table (`src/executor/scan.zig` inline tests).
- ✅ Regression: existing queries under 4096 rows / 4 MB show no behavior change (all existing unit, feature, and stress tests pass).
- ✅ Integration: `SELECT *` on table with >4096 rows returns complete correct results (`test/internals/spill/phase2_gate_test.zig`).
- ✅ Integration: query with large string columns exceeding 4 MB arena completes via arena safety valve (`test/internals/spill/phase2_gate_test.zig`).
- ✅ Integration: selective `WHERE` on large table does NOT spill (survivors fit in memory) (`test/internals/spill/phase2_gate_test.zig`).
- ✅ Determinism: spill replay from same seed produces identical results (`test/internals/spill/phase2_gate_test.zig`).
- ✅ Telemetry: `INSPECT spill` correctly reports degraded execution and byte counts (`test/internals/spill/temp_storage_surface_test.zig`).

## Phase 3: Sort/Group/Join Spill
### Scope
- Extend Phase 2's `SpillingResultCollector` pattern to operators that require full-input materialization:
  - **Sort**: external sort — generate sorted runs (each up to `work_memory_bytes_per_slot`), spill runs to temp pages, k-way merge.
  - **Group/Aggregate**: hash-based aggregation with spill partitions — when hash table exceeds byte budget, partition and spill, process partitions sequentially.
  - **Join**: build-side spill — when build side exceeds byte budget, partition both sides, spill, process partition pairs.
- Replace compile-time capacity constants in `capacity.zig` (`max_sort_rows`, `max_aggregate_groups`, `max_join_build_rows`) with byte-budget-derived runtime limits.
- All operator spill paths consume from Phase 2's chunked scan iterator (no separate scan mechanism needed).

### Gate
- Deterministic tests for large datasets under low memory profiles.
- Failure only on injected storage hard failures.
- Operators that previously failed at 4096 rows now complete via spill.

## Phase 4: Response Streaming
### Scope
- Move from fixed response buffer constraints to streaming writes where feasible.
- Response serialization reads from `SpillingResultCollector.iterator()` and writes to the wire incrementally, bounded by transport buffer size rather than total result size.

### Gate
- Large result sets do not fail with response-size errors under normal storage/network conditions.
- Memory usage during response serialization is O(transport buffer), not O(result size).
