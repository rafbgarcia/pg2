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

## Phase 3: Sort/Group/Join Operator Spill

### Design Decisions Locked

- **Shared monotonic temp page pool.** All operator spill paths allocate from the single per-slot `TempPageAllocator`. No intermediate frees between operators — the full pool is reclaimed at query end via `TempPageAllocator.reset()`. This keeps the allocator simple (no fragmentation) at the cost of peak temp page usage being the sum of all active operator spill. Default 1024 pages (8 MB) is sufficient for `work_memory_bytes_per_slot = 4 MB` with typical query shapes. Queries with multiple spilling operators may need a larger `temp_pages_per_query_slot` — this is a tuning knob, not an architecture limitation.

- **Byte-budget enforcement replaces row-count caps.** The compile-time constants `max_sort_rows`, `max_aggregate_groups`, `max_join_build_rows` become the in-memory batch size (how many rows fit in one working buffer), not total capacity limits. Total capacity is bounded only by `work_memory_bytes_per_slot` + temp page budget. `OperatorCapacities` derives runtime limits from the byte budget and average row width.

- **Deterministic hash function.** All hash-based operators (aggregate, join, partition assignment) use `std.hash.Wyhash` with a fixed seed of 0. Deterministic for simulation replay. The same function hashes group keys and join keys for both hash table probing and partition assignment.

- **Stable sort required.** ORDER BY must be stable (rows with equal sort keys preserve insertion order). Bottom-up merge sort satisfies this. External merge sort preserves stability within each run and during k-way merge (ties broken by run index, i.e. earlier runs win).

- **Operators consume from collector, not flat buffer.** The current "spill + full-input operators" path reloads rows from the collector into `result.rows` capped at `scan_batch_size` — this silently drops rows beyond 4096. Phase 3 removes this path. Instead, each spill-aware operator reads from the collector's iterator (or the in-memory buffer in the fast path) and handles its own spill internally. This means the operators must support two input modes: a flat `[]ResultRow` slice (fast path, no scan spill) and a `SpillingResultCollector.Iterator` (spill path, arbitrarily large input).

- **`TempStorageManager` shared across operators.** Currently created inside `executeReadPipeline` and owned by the collector. Phase 3 lifts it to `ExecContext` so all operators (collector, sort, aggregate, join) share the same manager and its underlying `TempPageAllocator`.

### Scope

#### 3a: In-Memory Sort Upgrade
- Replace O(n²) insertion sort in `src/executor/sorting.zig` with O(n log n) bottom-up merge sort.
- Use `scratch_rows_b` (currently unused, 4096 slots) as the merge auxiliary buffer. The merge alternates between `result.rows` and `scratch_rows_b`, copying back to `result.rows` at the end.
- Stable: equal-key rows preserve their original order.
- The existing `compareRowsBySortKeys()` and `SortKeyDescriptor` infrastructure is reused — only the sort loop changes.
- `plan.sort_strategy` gains a new variant: `.in_memory_merge`.
- No spill path yet; row count still bounded by `scan_batch_size` in this sub-phase.

#### 3b: External Merge Sort
- `src/executor/external_sort.zig`: external merge sort with sorted run generation, temp page spill, and k-way merge.
- **Run generation**: Read input in batches (up to `scan_batch_size` rows or `work_memory_bytes_per_slot` serialized bytes, whichever is smaller). Sort each batch in memory via 3a's merge sort. Serialize the sorted batch to temp pages as a contiguous "run" (sequence of pages). Track run metadata: first page ID, page count, row count.
- **K-way merge**: Maintain one `SpillPageReader` per run plus a min-heap of size K (one entry per active run). Each heap entry holds the current row and run index. Pop min, emit, advance that run's reader. Ties broken by run index (stability). Heap comparisons reuse `compareRowsBySortKeys()`.
- **Output**: If total sorted output fits in `result.rows` (≤ `scan_batch_size` rows), write directly to the buffer. Otherwise, write merged output to fresh temp pages and expose via a new iterator on the result. This feeds into subsequent operators (GROUP/HAVING/LIMIT) or directly to serialization.
- **Run count bound**: K = ceil(total_input_bytes / work_memory_bytes_per_slot). With defaults (4 MB work_mem, 8 MB temp budget), K ≤ 2 for typical workloads. Multi-pass merge (merge runs into larger runs, repeat) is deferred — not needed until temp budgets grow large enough to produce dozens of runs.
- **Capacity migration**: `max_sort_rows` no longer enforced as a hard error. Sort accepts any input size. Failure only on temp page exhaustion (`RegionExhausted`) or storage hard errors.
- `plan.sort_strategy` gains `.external_merge`.

#### 3c: Hash Aggregation with Partition Spill
- `src/executor/hash_aggregate.zig`: hash-based GROUP BY with grace-hash partition spill.
- **Hash table**: Open-addressing with linear probing. Array of `(hash: u64, group_index: u16)` entries, power-of-2 sized, load factor ≤ 0.7. Table size derived from `work_memory_bytes_per_slot` (budget / entry_size). Group representative rows stored in the working buffer (`result.rows` or `scratch_rows_b`).
- **Aggregate state**: Migrate from fixed `[max_group_aggregate_exprs][scan_batch_size]` to a flat array sized by the byte-budget-derived group capacity. The `AggregateState` struct and `updateAggregateState()` accumulation logic are reused unchanged.
- **In-memory fast path**: All input rows hash-aggregated into the table. If all groups fit (common OLTP case with few distinct keys), output the groups directly. O(n) total, O(1) per row amortized.
- **Spill path (grace hash aggregation)**: When a new group would exceed the table's capacity:
  1. Choose a partition count P (next power of 2 above current group count / target partition size). P partitions numbered 0..P-1.
  2. The hash table retains groups whose `hash % P == 0` (partition 0 stays resident). All other groups' rows are evicted from the table, their aggregate state is discarded.
  3. Remaining input rows: if `hash % P == 0`, aggregate inline. Otherwise, serialize to the spill partition's temp pages (raw input rows, not aggregated state).
  4. After all input consumed: emit partition 0 results. Then for each spilled partition (1..P-1): load from temp pages, hash-aggregate (fits in memory by construction since each partition is ~1/P of the data), emit results.
- **Subtlety — re-aggregation**: Spilled partitions contain raw input rows, not partial aggregates. This is necessary because aggregate functions like AVG need the original values (can't merge partial AVGs correctly without count+sum decomposition). For SUM/COUNT/MIN/MAX, partial merge would work, but the uniform re-aggregation approach is simpler and correct for all aggregate kinds.
- `max_aggregate_groups` becomes the in-memory hash table capacity, derived from byte budget. `plan.group_strategy` gains `.hash_spill`.

#### 3d: Hash Join with Partition Spill
- `src/executor/hash_join.zig`: hash join replacing nested-loop, with grace-hash partition spill.
- **Build phase**: Hash the build side (currently always the left/inner side; build-side selection is deferred to planner work) into a hash table. Hash table structure: same open-addressing design as 3c, storing `(hash: u64, row_index: u16)`. Build rows stored in a working buffer.
- **Probe phase**: For each probe row, look up matching build rows by hash + key equality. Emit concatenated output rows.
- **In-memory fast path**: If build side fits in `work_memory_bytes_per_slot`, standard in-memory hash join. O(n + m) where n = build, m = probe. Massive improvement over current O(n × m) nested-loop.
- **Spill path (grace hash join)**: When build side exceeds memory:
  1. Choose partition count P.
  2. Hash-partition both build and probe sides into P partitions, spill all to temp pages. Each partition pair is stored as a contiguous run of build pages followed by probe pages.
  3. For each partition: load build side into hash table (fits in memory), probe against it, emit matches.
- **LEFT JOIN handling**: During probe, track which build rows have been matched (bit vector per partition). After all probe rows processed, emit unmatched build rows with NULL-filled probe columns.
- `max_join_build_rows` and `max_join_output_rows` become byte-budget-derived. `plan.join_strategy` gains `.hash_spill` alongside the existing nested-loop (which remains available as a fallback for very small joins where hash overhead isn't worthwhile).
- **Single-key equality constraint remains.** Composite join keys and non-equality joins are out of scope for this phase.

#### 3e: Pipeline Integration and Capacity Migration
- **Lift `TempStorageManager` to `ExecContext`.** Created once in `executeReadPipeline`, shared by collector and all operators.
- **Remove the "reload up to scan_batch_size" spill path** in `executeReadPipeline`. Replace with:
  - If no spill: fast path unchanged (operators work on `result.rows` directly).
  - If spill: each operator receives the collector's iterator as input. Sort reads all rows, produces sorted output (in-memory or temp pages). Group reads sorted/unsorted rows, produces groups. Join reads both sides. Each operator handles its own memory management.
- **Operator chaining**: When an operator spills its output, the next operator reads from those temp pages. The executor orchestrates this by passing operator output descriptors (buffer pointer + count for in-memory, page ID list for spilled) between stages.
- **`OperatorCapacities` overhaul**: Remove `sort_rows`, `aggregate_groups`, `join_build_rows`, `join_output_rows` as hard caps. Replace with `work_memory_bytes` (from config). Keep `sort_keys`, `group_keys`, `group_aggregate_exprs` as compile-time structural limits (these bound the number of key columns and expressions, not data volume). Keep `sort_scratch_bytes`, `aggregate_state_bytes`, `join_state_bytes` as derived values (computed from `work_memory_bytes` at runtime).
- **Telemetry**: `plan.sort_strategy`, `plan.group_strategy`, `plan.join_strategy` reflect the actual strategy chosen (in-memory vs spill). `INSPECT spill` shows per-operator spill stats.

### Gate
- Unit tests: merge sort correctness, stability, edge cases (0, 1, 2 rows, all-equal keys, already-sorted, reverse-sorted) in `src/executor/sorting.zig` inline tests.
- Unit tests: external sort run generation, single-run passthrough, multi-run merge, deterministic output in `src/executor/external_sort.zig` inline tests.
- Unit tests: hash table insert/probe, collision handling, resize, group aggregation correctness in `src/executor/hash_aggregate.zig` inline tests.
- Unit tests: hash join build/probe, inner + left semantics, NULL key handling in `src/executor/hash_join.zig` inline tests.
- Regression: all existing unit, feature, and stress tests pass (in-memory fast paths unchanged).
- Integration: `SELECT * FROM large_table ORDER BY col` with >4096 rows returns correctly sorted results (`test/internals/spill/phase3_gate_test.zig`).
- Integration: `SELECT col, COUNT(*) FROM large_table GROUP BY col` with >4096 groups returns complete correct aggregates (`test/internals/spill/phase3_gate_test.zig`).
- Integration: `SELECT * FROM large_left JOIN large_right ON key` with >4096 build rows returns complete correct join output (`test/internals/spill/phase3_gate_test.zig`).
- Integration: combined query with scan spill + sort spill + group (e.g., `SELECT department, SUM(salary) FROM huge_employees GROUP BY department ORDER BY SUM(salary) DESC`) completes correctly under low memory profile (`test/internals/spill/phase3_gate_test.zig`).
- Determinism: all spill-path queries produce identical results from the same seed (`test/internals/spill/phase3_gate_test.zig`).
- Stress: sort/group/join on datasets 10x the memory budget complete without failure under default temp page budget (`test/stress/`).
- Failure: temp page exhaustion produces a clear error, not corruption or silent truncation.
- Telemetry: `INSPECT spill` correctly reports per-operator strategy and spill stats.

## Phase 4: Response Streaming
### Scope
- Move from fixed response buffer constraints to streaming writes where feasible.
- Response serialization reads from `SpillingResultCollector.iterator()` and writes to the wire incrementally, bounded by transport buffer size rather than total result size.

### Gate
- Large result sets do not fail with response-size errors under normal storage/network conditions.
- Memory usage during response serialization is O(transport buffer), not O(result size).
