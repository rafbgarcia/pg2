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
### Scope
- Replace fixed-result hard cap behavior in scan/materialization path with chunking/spill continuation.

### Gate
- Queries exceeding in-memory row buffer limits return complete results (or bounded paged protocol behavior), not resource errors.

## Phase 3: Sort/Group/Join Spill
### Scope
- Add external/partitioned algorithms:
  - sort run generation + merge
  - grouped aggregation with spill partitions
  - join build-side spill with partition strategy

### Gate
- Deterministic tests for large datasets under low memory profiles.
- Failure only on injected storage hard failures.

## Phase 4: Response Streaming
### Scope
- Move from fixed response buffer constraints to streaming writes where feasible.

### Gate
- Large result sets do not fail with response-size errors under normal storage/network conditions.
