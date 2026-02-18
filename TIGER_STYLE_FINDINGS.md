# Tiger Style Findings Tracker

## Context

This project is mid-implementation and we are intentionally investing early in Tiger Style hardening to avoid building features on top of weak foundations.
The goal is to make correctness, determinism, and recovery guarantees first-class now, so later phases (server/replication/query features) inherit strong baseline behavior instead of requiring risky rewrites.

This tracker exists to keep continuity across sessions:

- Preserve why each hardening task matters.
- Show what is already completed vs what is still open.
- Guide next work toward highest-leverage production safety improvements.

## How To Use This Tracker

- Treat `done` items as landed baseline.
- Treat `partial` items as active migrations that should be finished before adding adjacent complexity.
- Prioritize `pending` items that affect durability/corruption/recovery boundaries first.
- When a task is completed, update status and add concrete file refs for traceability.

## Completed So Far

1. WAL crash recovery metadata/envelope implemented.
   Status: `done`
   Notes: Added durable recovery envelope with magic/version/checksum and `recover()` flow.
   Refs: `src/storage/wal.zig`

2. Row decode corruption hardening implemented.
   Status: `done`
   Notes: Added checked decode APIs (`decodeColumnChecked` / `decodeRowChecked`) with bounds validation and `error.Corruption`.
   Refs: `src/storage/row.zig`, `src/executor/scan.zig`

3. Global Tiger error taxonomy scaffold implemented.
   Status: `partial`
   Notes: Added `ErrorClass` and mappers for buffer pool / scan / mutation / tx manager / WAL.
   Refs: `src/tiger/error_taxonomy.zig`
   Remaining: enforce mapping coverage for all public subsystem errors and wire at boundaries.

4. `unreachable` cleanup in touched runtime code.
   Status: `partial`
   Notes: Replaced key runtime `unreachable` sites with explicit panic/error handling in touched files.
   Refs: `src/storage/btree.zig`, `src/storage/row.zig`, `src/simulator/main.zig`
   Remaining: full codebase sweep.

5. WAL partial-page read error swallowing fixed.
   Status: `done`
   Notes: Replaced silent catch with explicit WAL read error path.
   Refs: `src/storage/wal.zig`

## Remaining Findings

1. Schema name buffer overflow guard missing.
   Status: `done`
   Refs: `src/storage/row.zig` (`RowSchema.addColumn`)

2. B-tree structural decode/boundary validation incomplete.
   Status: `done`
   Refs: `src/storage/btree.zig` node read helpers
   Notes: Added structural validation checks and explicit `error.Corruption` return paths before B-tree traversal/scan reads; expanded malformed-page tests for bad leaf pointers and internal key-length overflows.

3. Unbounded B-tree traversal loops need explicit limits.
   Status: `done`
   Refs: `src/storage/btree.zig` traversal loops
   Notes: Added bounded root-to-leaf depth guards (`max_btree_depth`) and sibling-hop caps (`max_leaf_sibling_hops`) with corruption regressions for over-depth traversal and cyclic sibling chains.

4. Checked integer arithmetic in executor expression engine incomplete.
   Status: `done`
   Refs: `src/executor/filter.zig`
   Notes: Replaced unchecked integer arithmetic with checked ops and explicit `error.NumericOverflow` handling (binary arithmetic, unary negate, `abs`), with regression tests for overflow edges.

5. Persistent format versioning still incomplete outside WAL envelope.
   Status: `done`
   Refs: `src/storage/page.zig` and broader on-disk structs
   Notes: Added page-header on-disk format metadata (`format_version` + `format_magic`) with explicit deserialize validation and incompatibility tests; added row payload format header (`magic` + `version`) with checked decode validation tests; added heap slotted-header format metadata and B-tree leaf/internal payload format metadata with corruption-path validation tests.

6. Static allocation/allocator sealing policy not yet enforced.
   Status: `partial`
   Refs: runtime alloc paths across executor/storage/mvcc
   Notes: Removed hot-path dynamic allocator usage from B-tree split logic by using fixed-capacity split scratch buffers derived from page-size bounds; removed `BTree` allocator dependency from runtime API/state after split-path sealing (`src/storage/btree.zig`). Added bounded no-allocation WAL decode path `readFromInto` with caller-owned record/payload buffers and explicit capacity errors (`src/storage/wal.zig`), reducing recovery-path dependence on heap-owned record payload copies. Migrated seeded recovery scenario to bounded decode (`src/simulator/fault_matrix.zig`) to exercise call-site capacity contracts. Added bounded `tableScanInto` scan path (`src/executor/scan.zig`) and switched read execution pipeline to scan directly into preallocated query-result storage (`src/executor/executor.zig`), removing one per-query scan allocation/copy path. Reworked mutation update/delete paths to iterate visible rows in-place without allocator-backed `tableScan` materialization (`src/executor/mutation.zig`), preserving predicate and MVCC visibility checks. Migrated WAL recovery/LSN/filter regressions to bounded `readFromInto` buffers by default (retaining targeted `readFrom` compatibility coverage), so validation now primarily exercises sealed decode paths. Added explicit executor operator capacity-contract module (`src/executor/capacity.zig`) with compile-time invariants for upcoming sort/aggregate/join paths and wired read pipeline operator-count bound to shared contract constants. Implemented bounded in-place executor sort (multi-key `asc`/`desc`, expression keys, no heap allocation) with explicit key/row capacity enforcement and deterministic insertion-sort ordering (`src/executor/executor.zig`), and disambiguated parser sort-key encoding for expression vs column keys (`src/parser/parser.zig`, `src/parser/ast.zig`). Implemented bounded in-place group-key collapsing in read execution (explicit `group_keys` / `aggregate_groups` enforcement, no runtime heap allocation) with regression coverage for group behavior and key-capacity overflow (`src/executor/executor.zig`). Added bounded grouped aggregate state/evaluation for `count(*)`, `sum`, `avg`, `min`, and `max` with explicit aggregate-expression and state-byte capacity contracts, fail-closed type/overflow behavior, and grouped resolver integration for post-group `sort`/`where` evaluation (`src/executor/executor.zig`, `src/executor/capacity.zig`).
   Remaining: implement join operators that consume bounded contracts for state/scratch/output sizing, and eventually retire allocator-backed `readFrom` from non-compatibility usage.

7. Deterministic fault injection matrix incomplete.
   Status: `partial`
   Refs: `src/simulator/disk.zig`, simulation tests
   Notes: Added deterministic one-shot fault injection controls for Nth read/write/fsync plus partial-write and bitflip-on-write corruption in `SimulatedDisk` with regression tests for each path; added buffer-pool propagation tests validating deterministic `StorageRead`/`StorageWrite`/`StorageFsync` error surfacing; added WAL end-to-end torn-write recovery regression (`recover` + `readFrom`) and a seeded fault-matrix module covering multi-step replay-deterministic scenarios (partial WAL write + crash + recover, data-page bitflip + checksum detection, WAL fsync failure + WAL-gated page flush). Seeded WAL recovery matrix now exercises bounded decode buffers via `readFromInto`, includes a longer multi-fault interleaving schedule (torn write + failed commit fsync + retry flush + crash/recover replay), includes a combined cross-subsystem schedule (WAL fsync failure/retry + WAL-gated page flush + page-write bitflip corruption + checksum enforcement + WAL recover/replay), validates each seeded schedule across an expanded seed set, and now includes a longer cross-subsystem schedule with mixed WAL fsync failure gating, page-write bitflip corruption, partial-write corruption, repeated crash/recover cycles, and deterministic replay checks (`src/simulator/fault_matrix.zig`). Added CI-oriented deterministic sweep helpers and generated seed corpora with explicit bounded budgets (`ci_short_seed_budget`, `ci_long_seed_budget`), plus dedicated extended-seed replay tests for short and long schedules with seed-index diagnostics on mismatch (`src/simulator/fault_matrix.zig`).
   Remaining: keep growing seed/interleaving breadth in CI (larger seed corpus + longer schedules) as new storage/recovery paths land.

## Current Build State

- `zig build test` passing after the completed changes.

### Session Handoff - 2026-02-18

- Goal this session:
  - Continue Tiger-style hardening by moving deterministic fault testing from single-fault injection toward seeded multi-step scenarios, then advance allocator sealing in WAL decode paths.
- Completed:
  - Added seeded replay-deterministic fault-matrix scenarios in `src/simulator/fault_matrix.zig` for:
    - partial WAL write + crash + recover
    - data-page bitflip + checksum enforcement
    - WAL fsync failure + WAL-gated page flush behavior
  - Wired fault-matrix module into test discovery (`src/pg2.zig`).
  - Extended `SimulatedDisk` with deterministic one-shot `partialWriteAt` and `bitflipWriteAt` injection APIs and tests (`src/simulator/disk.zig`).
  - Added WAL torn-write recovery regression (`src/storage/wal.zig`).
  - Added bounded no-allocation WAL decode API `readFromInto` with explicit `RecordBufferTooSmall` / `PayloadBufferTooSmall` errors and tests (`src/storage/wal.zig`).
  - Migrated seeded WAL partial-write recovery matrix scenario to bounded caller-owned WAL decode buffers via `readFromInto` (`src/simulator/fault_matrix.zig`).
  - Added seeded replay-deterministic multi-fault WAL interleaving scenario (torn write + fsync failure on commit + retry flush + crash/recover) in `src/simulator/fault_matrix.zig`.
  - Added bounded `tableScanInto` with explicit caller capacity contracts and regression coverage (`src/executor/scan.zig`), and switched read query execution to use the bounded scan path directly (`src/executor/executor.zig`).
  - Replaced allocator-backed mutation scan materialization with in-place visible-row iteration in `executeUpdate`/`executeDelete` (`src/executor/mutation.zig`), removing another executor runtime allocation path.
  - Added seeded combined cross-subsystem fault schedule in `src/simulator/fault_matrix.zig` covering WAL fsync failure/retry + WAL-gated page flush + data-page bitflip corruption + checksum detection + WAL recover/replay.
  - Migrated WAL recovery/filter/multi-page regression coverage to bounded `readFromInto` call patterns with explicit fixed capacities (`src/storage/wal.zig`), keeping one compatibility test for allocator-backed `readFrom`.
  - Added explicit executor capacity-contract module for sort/aggregate/join operator intermediates with bounded defaults and invariants (`src/executor/capacity.zig`), and wired executor operator-count limit to shared contract constants (`src/executor/executor.zig`).
  - Added explicit sort/group executor stubs with capacity checks and non-silent failure semantics, with regression coverage for not-implemented and capacity-exceeded paths (`src/executor/executor.zig`).
  - Replaced sort stub with bounded in-place sort execution (supports multi-key `asc`/`desc` and expression keys, with no runtime allocator usage) and added regression coverage for direction and expression-key ordering (`src/executor/executor.zig`).
  - Added explicit sort-key metadata encoding for expression keys in parser output so executor decoding is deterministic (`src/parser/parser.zig`, `src/parser/ast.zig`).
  - Replaced group stub with bounded in-place group-key execution (distinct-by-key row collapse using fixed capacities, no runtime allocator usage) and added regression coverage for grouped output and group-key capacity enforcement (`src/executor/executor.zig`).
  - Added grouped aggregate `count(*)` runtime state and aggregate-aware expression evaluation hooks so post-group `sort`/`where` can evaluate `count(*)` deterministically without heap allocation (`src/executor/executor.zig`, `src/executor/filter.zig`).
  - Extended grouped aggregate runtime to support `sum` / `avg` / `min` / `max` with bounded per-query aggregate-state contracts (`group_aggregate_exprs` + `aggregate_state_bytes`) and fail-closed evaluation/update paths (type mismatch and numeric overflow surface as query errors), with regressions covering grouped aggregate behavior, type safety, and aggregate-expression capacity enforcement (`src/executor/executor.zig`, `src/executor/capacity.zig`).
  - Updated WAL error mappings in taxonomy/boundary adapters (`src/tiger/error_taxonomy.zig`, `src/executor/mutation.zig`, `src/storage/btree.zig`).
  - Broadened fault-matrix replay checks to run all seeded schedules across an expanded multi-seed set (`src/simulator/fault_matrix.zig`).
  - Added a repeated crash/recover WAL schedule with mixed fsync-failure retry and torn-write interleaving, validated replay-deterministically across the seed set (`src/simulator/fault_matrix.zig`).
  - Expanded the seed corpus again and added a longer mixed WAL+buffer-pool interleaving schedule that combines WAL fsync failure gating, page bitflip corruption, page partial-write corruption, and repeated crash/recover validation (`src/simulator/fault_matrix.zig`).
- In progress:
  - Allocator-sealing migration is still partial at the system level; scan/sort/group/aggregate runtime paths now use bounded in-memory contracts, while join operators and remaining buffering paths still need explicit non-allocating contracts.
- Blockers / decisions needed:
  - Define explicit per-call-site WAL decode capacity budgets (record count + payload bytes) for remaining recovery/read paths as those paths are introduced/expanded.
- Tests run:
  - `zig build test`
- Next recommended step:
  1. Implement bounded join operator execution (state/scratch/output contracts, fail-closed capacity errors, deterministic regressions).
  2. Add CI-oriented seed sweeps for fault-matrix scenarios (larger deterministic seed corpus with bounded runtime budget and failure-shrinking output).
  3. Continue retiring allocator-backed WAL `readFrom` usage outside explicit compatibility tests as new recovery/read call sites are added.

### Session Handoff - 2026-02-18 (continued)

- Goal this session:
  - Continue Tiger-style hardening by adding CI-oriented deterministic replay sweeps with explicit runtime bounds and better failure diagnostics.
- Completed:
  - Added deterministic seed-set generator utilities (`splitMix64`, `buildSeedSet`) in `src/simulator/fault_matrix.zig` so seed corpora are reproducible and easy to scale.
  - Added reusable replay-determinism assertion helper (`expectReplayDeterministicAcrossSeeds`) with explicit mismatch diagnostics that include scenario name, seed index, and seed value.
  - Added bounded CI-oriented seed sweeps:
    - `ci_short_seed_budget = 24` for shorter schedules.
    - `ci_long_seed_budget = 12` for longer schedules.
  - Added two dedicated sweep tests to keep runtime bounded while increasing seed breadth:
    - extended deterministic replay coverage for short schedules.
    - bounded deterministic replay coverage for long schedules.
- In progress:
  - Deterministic fault matrix remains `partial`; the matrix now has broader CI seed coverage, but additional interleavings should continue to be added as new persistence/recovery paths land.
- Blockers / decisions needed:
  - None for this increment.
- Tests run:
  - `zig build test`
- Next recommended step:
  1. Implement bounded join operator execution (state/scratch/output contracts, fail-closed capacity errors, deterministic regressions).
  2. Continue retiring allocator-backed WAL `readFrom` usage outside explicit compatibility tests as new recovery/read call sites are introduced.
