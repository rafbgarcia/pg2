# Workfront 14: Runtime Storage Backend and Memory Accounting Boundaries

## Objective

Run server mode on a real file-backed `Storage` backend (not `SimulatedDisk`) so process memory is bounded by runtime buffers and cache policy, not by dataset size.

## Why

- `main.zig` currently boots runtime with `SimulatedDisk`, which stores durable and pending pages in in-memory hash maps.
- This makes process RSS grow with logical database size even when `--memory` is fixed.
- Memory-facing contracts in WF02/WF03 are incomplete until persistent storage is decoupled from heap growth.

## Non-Negotiables

1. Keep using the `Storage` abstraction; no direct storage-specific calls in executor/runtime code.
2. Preserve deterministic behavior under simulation tests; `SimulatedDisk` remains first-class for tests/fault injection.
3. Fail closed on storage errors with explicit boundary errors (no silent corruption/retry loops).

## Phase 1: File-Backed Storage Implementation

### Scope

- Add a production file-backed `Storage` implementation in `src/storage/`:
  - fixed-size page read/write by `page_id`
  - `fsync` durability boundary
  - sparse growth semantics for new pages
- Add focused unit tests for:
  - write/read round-trip by page id
  - persistence across reopen
  - fsync and error propagation contracts

### Gate

- New storage implementation passes isolated unit tests.
- No allocator growth tied to page cardinality inside the storage layer.

## Phase 2: Runtime Wiring in `main.zig`

### Scope

- Replace `SimulatedDisk` in server runtime path with file-backed storage.
- Add CLI/runtime config for storage path (or explicitly document default path behavior).
- Keep simulation-only paths and test harnesses on `SimulatedDisk`.

### Gate

- Server mode boots and serves requests on file-backed storage.
- Existing feature/integration tests continue to run with simulation storage.

## Phase 3: Memory Accounting and Guardrails

### Scope

- Document and enforce memory boundaries:
  - bootstrap static allocator region
  - buffer pool frame budget
  - WAL buffer budget
  - per-slot query arena and spill budgets
- Add observability counters that separate:
  - memory-resident runtime structures
  - storage file size / logical pages

### Gate

- Under sustained inserts, process RSS remains within expected envelope for configured runtime memory budgets (excluding allocator/system overhead).
- Dataset/file growth does not imply proportional heap growth.

## Dependencies and Cross-Workfront Notes

- Depends on WF01 server runtime path stability for clean boot wiring.
- Consumes WF02 planner outputs for memory budget contracts.
- Must not weaken WF03 spill correctness or determinism guarantees.
