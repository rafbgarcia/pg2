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
   Notes: Removed hot-path dynamic allocator usage from B-tree split logic by using fixed-capacity split scratch buffers derived from page-size bounds; removed `BTree` allocator dependency from runtime API/state after split-path sealing (`src/storage/btree.zig`). Added bounded no-allocation WAL decode path `readFromInto` with caller-owned record/payload buffers and explicit capacity errors (`src/storage/wal.zig`), reducing recovery-path dependence on heap-owned record payload copies. Migrated seeded recovery scenario to bounded decode (`src/simulator/fault_matrix.zig`) to exercise call-site capacity contracts. Added bounded `tableScanInto` scan path (`src/executor/scan.zig`) and switched read execution pipeline to scan directly into preallocated query-result storage (`src/executor/executor.zig`), removing one per-query scan allocation/copy path. Reworked mutation update/delete paths to iterate visible rows in-place without allocator-backed `tableScan` materialization (`src/executor/mutation.zig`), preserving predicate and MVCC visibility checks. Migrated WAL recovery/LSN/filter regressions to bounded `readFromInto` buffers by default (using bounded decode coverage only), so validation now primarily exercises sealed decode paths. Added explicit executor operator capacity-contract module (`src/executor/capacity.zig`) with compile-time invariants for sort/aggregate/join paths and wired read pipeline operator-count bound to shared contract constants. Implemented bounded in-place executor sort (multi-key `asc`/`desc`, expression keys, no heap allocation) with explicit key/row capacity enforcement and deterministic insertion-sort ordering (`src/executor/executor.zig`), and disambiguated parser sort-key encoding for expression vs column keys (`src/parser/parser.zig`, `src/parser/ast.zig`). Implemented bounded in-place group-key collapsing in read execution (explicit `group_keys` / `aggregate_groups` enforcement, no runtime heap allocation) with regression coverage for group behavior and key-capacity overflow (`src/executor/executor.zig`). Added bounded grouped aggregate state/evaluation for `count(*)`, `sum`, `avg`, `min`, and `max` with explicit aggregate-expression and state-byte capacity contracts, fail-closed type/overflow behavior, and grouped resolver integration for post-group `sort`/`where` evaluation (`src/executor/executor.zig`, `src/executor/capacity.zig`). Added bounded deterministic inner-join execution foundation in the executor with explicit `join_build_rows`, `join_output_rows`, and `join_state_bytes` fail-closed contracts plus deterministic left-major ordering guarantees and contract-overflow regressions (`src/executor/executor.zig`). Wired nested-relation query execution to invoke bounded join contracts through the user-visible selection-set path with fail-closed association/key-resolution errors and nested-operator application on the relation side (including deterministic nested-sort coverage and missing-association regression), validating non-helper query-surface join behavior (`src/executor/executor.zig`). Extended selection-set execution to support multiple nested relations in one query while preserving bounded join contracts and deterministic ordering, with regression coverage for cross-product join shaping via query-surface nested relation pipelines (`src/executor/executor.zig`). Moved association join-key resolution into catalog metadata contracts so executor joins now consume configured key columns (`local_column_id` / `foreign_key_column_id`) rather than inferring keys at execution time; added explicit key configuration API and fail-closed resolve-time key validation plus executor regression coverage using non-conventional key names (`src/catalog/catalog.zig`, `src/executor/executor.zig`). Added explicit schema-level reference declarations (`reference(alias, local, Target.field, with/withoutReferentialIntegrity(...))`) and parser/loader validation coverage, plus canonical bracketed index declaration syntax with strict empty/missing/trailing-comma rejection (`src/parser/parser.zig`, `src/catalog/schema_loader.zig`, `src/parser/tokenizer.zig`, `docs/QUERY_LANGUAGE.md`). Enforced `withReferentialIntegrity(...)` mutation-time actions with fail-closed checks in `insert`/`update`/`delete`, including outgoing parent-existence validation, incoming `restrict`/`cascade`/`set_null` behavior, and explicit unsupported-path handling for `set_default`; added regressions for insert parent-missing, delete restrict/delete cascade, and update restrict/update cascade/update set-null/update set-default fail-closed paths (`src/executor/mutation.zig`). Removed allocator-backed WAL `readFrom`; WAL decode now uses bounded `readFromInto` only in runtime and tests (`src/storage/wal.zig`). Added Tiger `StaticAllocator` foundation module with explicit `init`/`sealed` phase tracking, fail-closed panic on post-seal allocation/growth attempts, and unit coverage for phase/usage semantics (`src/tiger/static_allocator.zig`, `src/pg2.zig`). Added startup/runtime bootstrap wiring that constructs core storage+MVCC state from `StaticAllocator`, seals before runtime operations, and verifies post-seal operation safety via deterministic tests (`src/runtime/bootstrap.zig`). Hardened startup capacity preallocation to avoid runtime hash-map growth in buffer pool and to lock WAL append growth to startup-reserved capacity with fail-closed `OutOfMemory` on over-budget runtime appends (`src/storage/buffer_pool.zig`, `src/storage/wal.zig`). Removed allocator-backed query-result allocation in executor runtime by requiring caller-owned result/scratch row buffers in `ExecContext` and reworking nested-selection join temp state to use bounded scratch buffers rather than dynamic allocation (`src/executor/executor.zig`). Added runtime query-buffer slot provisioning during startup (startup-carved `result_rows` + two scratch buffers per slot) with deterministic bounded acquire/release APIs so request/session code can pass preallocated buffers into `ExecContext` without runtime allocation (`src/runtime/bootstrap.zig`). Added explicit startup memory-budget validation with fail-closed `error.InsufficientMemoryBudget` (and config validity check `error.InvalidConfig`) so undersized `--memory` budgets are rejected deterministically before runtime (`src/runtime/bootstrap.zig`). Added runtime CLI config parser and `--memory` flag handling with unit-aware parsing (`bytes`, `MiB`, `GiB`) and a default `512MiB` budget surface in the executable entry point (`src/runtime/config.zig`, `src/main.zig`, `src/pg2.zig`). Added request execution leasing primitive `executeWithLeasedQueryBuffers` with deterministic slot acquisition and panic-on-invariant-release semantics so request/session code now has a concrete bounded wrapper around `executor.execute` (`src/runtime/request.zig`, `src/pg2.zig`).
   Remaining: wire `runtime.request.executeWithLeasedQueryBuffers` into the future live wire-protocol connection/session handler once server transport code lands.

7. Deterministic fault injection matrix incomplete.
   Status: `partial`
   Refs: `src/simulator/disk.zig`, simulation tests
   Notes: Added deterministic one-shot fault injection controls for Nth read/write/fsync plus partial-write and bitflip-on-write corruption in `SimulatedDisk` with regression tests for each path; added buffer-pool propagation tests validating deterministic `StorageRead`/`StorageWrite`/`StorageFsync` error surfacing; added WAL end-to-end torn-write recovery regression (`recover` + `readFromInto`) and a seeded fault-matrix module covering multi-step replay-deterministic scenarios (partial WAL write + crash + recover, data-page bitflip + checksum detection, WAL fsync failure + WAL-gated page flush). Seeded WAL recovery matrix now exercises bounded decode buffers via `readFromInto`, includes a longer multi-fault interleaving schedule (torn write + failed commit fsync + retry flush + crash/recover replay), includes a combined cross-subsystem schedule (WAL fsync failure/retry + WAL-gated page flush + page-write bitflip corruption + checksum enforcement + WAL recover/replay), validates each seeded schedule across an expanded seed set, and now includes a longer cross-subsystem schedule with mixed WAL fsync failure gating, page-write bitflip corruption, partial-write corruption, repeated crash/recover cycles, and deterministic replay checks (`src/simulator/fault_matrix.zig`). Added CI-oriented deterministic sweep helpers and generated seed corpora with explicit bounded budgets (`ci_short_seed_budget`, `ci_long_seed_budget`), plus dedicated extended-seed replay tests for short and long schedules with seed-index diagnostics on mismatch (`src/simulator/fault_matrix.zig`).
   Remaining: keep growing seed/interleaving breadth in CI (larger seed corpus + longer schedules) as new storage/recovery paths land.

## Current Build State

- `zig build test` passing after the completed changes.

### Session Handoff

- State summary:
  - Bounded runtime contracts are in place for scan/sort/group/aggregate/join foundations and most WAL read paths.
  - Tiger `StaticAllocator` foundation now exists with explicit seal semantics and tests (`src/tiger/static_allocator.zig`).
  - Core runtime bootstrap now allocates storage/MVCC state in init phase and seals allocator before runtime operations (`src/runtime/bootstrap.zig`).
  - Executor runtime no longer allocates `QueryResult` rows or nested-join temp buffers via allocator-backed paths (`src/executor/executor.zig`).
  - Runtime now pre-provisions bounded query-buffer slots (result + scratch) during startup and exposes deterministic acquire/release APIs (`src/runtime/bootstrap.zig`).
  - Runtime request execution now has a bounded lease wrapper that acquires query slot buffers, builds `ExecContext`, executes, and releases on failure paths (`src/runtime/request.zig`).
  - `--memory` CLI flag is now wired with deterministic parsing/defaulting (`src/runtime/config.zig`, `src/main.zig`), aligning runtime surface with documented startup memory budgeting.
  - User-visible nested relation traversal now routes through bounded join execution in the read pipeline (with fail-closed association/key resolution).
  - Mutation paths now enforce schema-level referential-integrity actions at execution time with fail-closed behavior (`src/executor/mutation.zig`).
  - WAL decode paths now use bounded `readFromInto` only (`src/storage/wal.zig`).
  - Deterministic fault matrix includes multi-step schedules and CI-oriented seed sweeps with bounded budgets (`src/simulator/fault_matrix.zig`).
- Still open:
  - Allocator-sealing migration is still `partial`; request leasing exists but is not yet called from a live wire-protocol connection/session path.
  - Deterministic fault matrix should keep expanding as new persistence/recovery paths land.
- Blockers / decisions:
  - None currently.
- Last validation run:
  - `zig build test` (pass)

#### Next Codex Session Plan

1. Wire server connection/session handling to call `runtime.request.executeWithLeasedQueryBuffers`.
   Done criteria: live wire-protocol request path uses leased execution and releases slot deterministically after response serialization.
2. Align memory budget plumbing from CLI to runtime bootstrap.
   Done criteria: `--memory` parsed in `main` is passed to runtime bootstrap allocation region sizing, with explicit startup rejection messaging on `error.InsufficientMemoryBudget`.
3. Validation before handoff.
   Run `zig build test` and record pass/fail outcome in this file.
