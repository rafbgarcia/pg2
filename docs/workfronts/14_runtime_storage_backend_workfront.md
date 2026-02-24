# Workfront 14: Runtime Storage Backend and Memory Accounting Boundaries

## Status

`active follow-through` (not complete).

### Checklist

- [ ] Phase 1 complete: production file-backed storage exists and is unit-tested.
- [ ] Phase 2 complete: server mode boots on production storage path.
- [ ] Phase 3 complete: memory/storage accounting boundaries are enforced and observable.
- [ ] Phase 4 complete: crash/restart and durability gates proven under file-backed runtime.

## Objective

Run server mode on real file-backed storage (not `SimulatedDisk`) so runtime memory is bounded by configured budgets and is not proportional to dataset size.

## Why

- `main.zig` still bootstraps runtime on `SimulatedDisk`.
- `SimulatedDisk` stores pages in in-memory hash maps, so logical data growth can increase process memory.
- Memory contracts from WF02/WF03 are incomplete until production storage is decoupled from heap growth.

## Non-Negotiables

1. Keep `Storage` abstraction boundaries in core runtime/executor code.
2. Keep deterministic simulation paths first-class (`SimulatedDisk` stays default in tests/fault matrix).
3. Fail closed on storage errors with explicit boundary errors (no silent retries/corruption).
4. Keep runtime allocations independent from storage page cardinality.

## Decision Lock (Recommended)

### 1) Storage Layout: Multi-File (Recommended)

Use three files under one storage root directory:

- `data.pg2` for heap/index/catalog pages.
- `wal.pg2` for WAL pages/metadata.
- `temp.pg2` for spill/temp pages.

Implement this behind a single `Storage` adapter (`RoutingStorage`) that routes by page-id bands. This keeps current core interfaces stable while removing single-file coupling between data/WAL/temp domains.

### Why Multi-File Is Recommended

- Stronger operational isolation (WAL growth and temp churn do not fragment main data file).
- Cleaner accounting (`data bytes`, `wal bytes`, `temp bytes` are direct file sizes).
- Safer long-term evolution (can later tune fsync/checkpoint policies per file domain).
- Removes pressure to reserve huge magic page-id ranges forever.

### Single-File Tradeoffs (Not Recommended as default)

- Simpler initial bootstrap and fewer file descriptors.
- But forces tighter page-id namespace coupling between data/WAL/temp.
- Harder per-domain observability and operational control.
- Higher blast radius for corruption and file churn.

### 2) Concrete Implementation Lock (No Re-Interpretation)

1. CLI flag: `--storage <dir>` is the runtime storage root for server mode.
2. Default storage root when flag is omitted: `.pg2` under current working directory.
3. File names under storage root are fixed:
   - `data.pg2`
   - `wal.pg2`
   - `temp.pg2`
   - `LOCK`
4. Single-writer rule:
   - Server startup creates `LOCK` with exclusive create.
   - If `LOCK` already exists, startup fails closed with explicit message.
   - No automatic stale-lock recovery in this workfront.
5. Page-id routing bands are fixed for this workfront:
   - `0..999_998` -> `data.pg2`
   - `999_999..19_999_999` -> `wal.pg2`
   - `20_000_000+` -> `temp.pg2`
6. Existing constants remain authoritative:
   - WAL metadata page id `999_999`.
   - WAL page base `1_000_000`.
   - Temp region start `20_000_000`.
7. No format-compatibility promises are required for this workfront (greenfield rule).

## Operational Contracts (Must Be Explicit)

1. Storage root contains a lock file; second writer process fails closed on startup.
2. Startup creates missing files atomically; existing files open without truncation.
3. `read(page_id)` on never-written page returns zero page.
4. Writes are page-sized and page-aligned by abstraction contract.
5. WAL durability boundary remains `wal.flush()/wal.fsync()` before commit acknowledgment policy.
6. Data file fsync is not per-row commit; this workfront introduces no background checkpoint thread and keeps existing explicit flush paths only.
7. `temp.pg2` is non-durable across restart; startup truncates temp domain.
8. Any open/read/write/fsync failure maps to explicit storage boundary error and aborts request path.
9. Startup order is fixed: acquire lock -> open/create files -> wire storage -> bootstrap runtime -> accept requests.
10. Shutdown order is fixed: stop accepting requests -> flush runtime state -> close files -> release lock.

## Phase 1: File-Backed Storage Core

### Scope

- Add production storage implementation under `src/storage/`:
  - page read/write/fsync.
  - open/create/close lifecycle.
  - deterministic error mapping.
- Add routing adapter for domain separation (`data/wal/temp`) under one `Storage` interface.
- Add isolated unit tests:
  - round-trip read/write by `page_id`.
  - persistence across close/reopen.
  - zero-fill reads for unwritten pages.
  - fsync error propagation and fail-closed behavior.
  - multi-file routing correctness (same `page_id` domain always maps to same backing file).

### Gate

- Unit suite passes with file-backed implementation.
- No allocator growth tied to number of pages written.
- No core module requires direct filesystem calls.
- Routing tests prove exact band/file mapping for boundary page ids (`999_998`, `999_999`, `19_999_999`, `20_000_000`).

## Phase 2: Runtime Wiring and CLI

### Scope

- Replace server-mode `SimulatedDisk` bootstrap path with file-backed storage.
- Add storage root CLI/config (for example `--storage <dir>`) and document defaults.
- Keep test harnesses/fault matrix on `SimulatedDisk` unless explicitly testing file backend.

### Gate

- Server mode boots and serves requests on file-backed storage.
- Existing feature/integration suites continue to pass on simulation storage.
- New targeted server smoke tests pass on file-backed storage.
- `--storage` path behavior is covered:
  - explicit directory works,
  - default `.pg2` path works,
  - pre-existing `LOCK` fails startup.

## Phase 3: Memory Accounting and Guardrails

### Scope

- Make runtime memory envelope explicit and inspectable:
  - bootstrap memory region.
  - buffer pool frames.
  - WAL in-memory buffer.
  - per-slot arenas/temp page budgets.
- Export storage/accounting counters separately:
  - memory-resident bytes by major runtime bucket.
  - `data.pg2`, `wal.pg2`, `temp.pg2` byte sizes.
  - logical page counts by domain.

### Gate

- Under sustained insert workload, RSS remains <= `1.35 * --memory` after warm-up.
- Dataset growth increases on-disk bytes but does not scale runtime heap proportionally.
- Inspect/diagnostic output clearly separates memory and storage growth vectors.
- Gate workload minimum: at least 1,000,000 inserted rows or 1 GiB on-disk data (whichever occurs first).

## Phase 4: Crash/Recovery Proof for File Backend

### Scope

- Add restart tests on file-backed runtime:
  - committed transactions survive restart.
  - unflushed WAL does not appear committed after restart.
  - temp domain is reset on restart.
- Verify WAL replay works with file-backed storage adapter and does not regress simulation semantics.

### Gate

- Deterministic crash/restart tests pass for file backend.
- No silent corruption under induced read/write/fsync failures.
- Recovery invariants match current WAL/visibility contracts.
- Restart gate explicitly validates:
  - committed-before-fsync-policy boundary survives,
  - not-durable-before-fsync-policy boundary is not visible as committed,
  - `temp.pg2` is truncated/reset on restart.

## Dependencies and Cross-Workfront Notes

- Depends on WF01 server runtime path stability for production boot wiring.
- Consumes WF02 planner outputs for memory budget contracts.
- Must not weaken WF03 spill correctness or deterministic behavior.
- Aligns with post-sequence hardening requirements for crash/recovery proof.
