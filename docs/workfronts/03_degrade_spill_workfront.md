# Workfront 03: Degrade-First Execution and Spill

## Objective
Queries should degrade performance under memory pressure before failing, using pg2 storage abstractions.

## Non-Negotiables
1. Core logic must use `Storage` abstraction for temp/spill I/O.
2. Fail only on hard-stop conditions (disk failures, corruption, impossible resource limits).
3. Spill behavior must be deterministic in simulation.

## Phase 1: Temp/Spill Storage Foundation
### Scope
- Add temp storage manager and page allocator.
- Add spill telemetry counters (bytes/pages read/write).

### Gate
- Unit tests for allocation, reclaim, and fault handling.
- Simulation replay determinism for spill operations.

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
