# Workfront 14: Runtime Storage Backend and Memory Accounting Boundaries

## Status

`active follow-through` (not complete).

### Checklist

- [x] Phase 1 complete: production file-backed storage exists and is unit-tested.
- [x] Phase 2 complete: server mode boots on production storage path.
- [x] Phase 3 complete: memory/storage accounting boundaries are enforced and observable.
- [ ] Phase 4 complete: crash/restart and durability gates proven under file-backed runtime.

### Progress Notes

- Implemented canonical client command surface for runtime diagnostics:
  `pg2 inspect runtime --format json --server <host:port>` (`--server` required, fail-closed when missing).
- Implemented server-side JSON schema contract emission (`schema_version = 1`) with memory/storage/logical/ratio/meta buckets.
- Build lanes split: `zig build test --summary all` excludes stress, and `zig build stress --summary all` runs stress-only gates.
- Added CI workflow with parallel `test` and `stress` jobs on Zig `0.15.2`.
- Added deterministic RSS stress gate:
  `1_000_000` rows or `1 GiB` on-disk (whichever first), 1-second sampling, warm-up exclusion (first 10%), p95 assertion `<= 1.35 * memory_budget`.

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
   - Server startup opens/creates `LOCK` and acquires an OS-level exclusive file lock that stays held for process lifetime.
   - If lock acquisition fails, startup fails closed with explicit message (`another writer is active for this storage root`).
   - `LOCK` stores diagnostic metadata only (`pid`, `hostname`, `started_at_unix_ns`); ownership is determined by OS lock, not file existence.
   - No PID-only stale-lock takeover logic is allowed.
   - Portability lock: use `fcntl(F_SETLK/F_SETLKW)` byte-range exclusive lock on the `LOCK` file (cross-platform behavior target for macOS/Linux in Zig std.os bindings).
   - Lock lifetime contract: lock is considered held while process keeps the lock fd open; lock release occurs on orderly shutdown close and on process death by OS fd cleanup.
5. Page-id routing bands are fixed for this workfront:
   - `0..999_998` -> `data.pg2`
   - `999_999..19_999_999` -> `wal.pg2`
   - `20_000_000+` -> `temp.pg2`
   - These bands are internal engine constants and are not user-configurable.
6. Existing constants remain authoritative:
   - WAL metadata page id `999_999`.
   - WAL page base `1_000_000`.
   - Temp region start `20_000_000`.
7. No format-compatibility promises are required for this workfront (greenfield rule).
8. Inspect runtime client contract for Phase 3 is locked:
   - Command is client-side against a running server: `pg2 inspect runtime --format json --server <host:port>`.
   - `--server` is required (no default endpoint in this workfront).
   - Missing `--server` fails closed with explicit error.

### 3) Routing Page-ID Translation Lock

- Routing remains by global page-id bands, but each backing file uses local page ids:
  - `data.pg2`: `local_page_id = global_page_id`
  - `wal.pg2`: `local_page_id = global_page_id - 999_999`
  - `temp.pg2`: `local_page_id = global_page_id - 20_000_000`
- This avoids sparse giant offsets in WAL/temp files and preserves per-domain byte accounting semantics.

## Operational Contracts (Must Be Explicit)

1. Storage root contains `LOCK`; second writer process fails closed when OS exclusive lock cannot be acquired.
2. Startup creates missing files atomically; existing files open without truncation.
3. `read(page_id)` on never-written page returns zero page.
4. Writes are page-sized and page-aligned by abstraction contract.
5. WAL durability boundary remains `wal.flush()/wal.fsync()` before commit acknowledgment policy.
6. Data file fsync is not per-row commit; this workfront introduces no background checkpoint thread and keeps existing explicit flush paths only.
7. `temp.pg2` is non-durable across restart; startup truncates temp domain.
8. Any open/read/write/fsync failure maps to explicit storage boundary error and aborts request path.
9. Startup order is fixed: open/create `LOCK` -> acquire OS lock -> open/create data files -> wire storage -> bootstrap runtime -> accept requests.
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
- Add lock diagnostics command for operators (`pg2 lock inspect --storage <dir>`) so ownership metadata is visible without manual file parsing.
- Keep test harnesses/fault matrix on `SimulatedDisk` unless explicitly testing file backend.

### Gate

- Server mode boots and serves requests on file-backed storage.
- Existing feature/integration suites continue to pass on simulation storage.
- New targeted server smoke tests pass on file-backed storage.
- `--storage` path behavior is covered:
  - explicit directory works,
  - default `.pg2` path works,
  - concurrent second writer fails startup while first holder is alive,
  - stale `LOCK` file without held OS lock does not block startup.

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
- Lock inspect/diagnostic schema for machine consumption (`schema_version = 1`) with required fields:
  - `memory_bytes`: `bootstrap`, `buffer_pool`, `wal_buffer`, `slot_arenas`, `total`, `budget`.
  - `storage_bytes`: `data_pg2`, `wal_pg2`, `temp_pg2`, `total`.
  - `logical_pages`: `data`, `wal`, `temp`, `total`.
  - `ratios`: `rss_over_budget`, `memory_total_over_budget`.
  - `meta`: `sampled_at_unix_ns`.
- Canonical diagnostics surface for this workfront: `pg2 inspect runtime --format json` (machine contract source of truth).
- Optional human-readable formatting may exist, but gate tests must only validate JSON contract keys/invariants.

### Gate

- Under sustained insert workload, RSS remains <= `1.35 * --memory` after warm-up.
- RSS verification method is fixed:
  - workload inserts until at least `1_000_000` rows or `1 GiB` on-disk data (whichever happens first),
  - warm-up window excluded from decision (first 10% of runtime),
  - RSS sampled every second during steady state,
  - pass criterion uses steady-state p95: `p95(rss_samples) <= 1.35 * --memory`.
- RSS sampler source for gate automation:
  - macOS: `ps -o rss= -p <pid>` (KiB units),
  - Linux: `/proc/<pid>/status` `VmRSS` field (kB units).
- Sampling conversion contract: normalize both sources to bytes before p95 computation.
- Dataset growth increases on-disk bytes but does not scale runtime heap proportionally.
- Inspect/diagnostic output clearly separates memory and storage growth vectors.
- Gate tests assert `schema_version = 1` diagnostics keys/invariants instead of formatted text output.

## Phase 4: Crash/Recovery Proof for File Backend

### Scope

- Add restart tests on file-backed runtime:
  - committed transactions survive restart.
  - unflushed WAL does not appear committed after restart.
  - temp domain is reset on restart.
- Verify WAL replay works with file-backed storage adapter and does not regress simulation semantics.
- Use deterministic crash injection around durability boundaries via storage fault hooks:
  - inject failures at `wal.write`, `wal.fsync`, and WAL metadata/envelope persistence boundaries,
  - run boundary matrix by deterministic operation index,
  - restart after each injected crash and assert durability/visibility invariants.

### Gate

- Deterministic crash/restart tests pass for file backend.
- No silent corruption under induced read/write/fsync failures.
- Recovery invariants match current WAL/visibility contracts.
- Restart gate explicitly validates:
  - committed-before-fsync-policy boundary survives,
  - not-durable-before-fsync-policy boundary is not visible as committed,
  - `temp.pg2` is truncated/reset on restart.
- Crash matrix must be deterministic (seeded, no wall-clock timing races).

## Dependencies and Cross-Workfront Notes

- Depends on WF01 server runtime path stability for production boot wiring.
- Consumes WF02 planner outputs for memory budget contracts.
- Must not weaken WF03 spill correctness or deterministic behavior.
- Aligns with post-sequence hardening requirements for crash/recovery proof.
