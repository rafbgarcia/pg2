# Connection Pool — Implementation Plan

## Status

- [x] Milestone 1: `PoolConn` and `ConnectionPool`
- [x] Milestone 2: Refactor `Session` to use `ConnectionPool`
- [x] Milestone 3: Refactor `main.zig`
- [x] Milestone 4 (partial): reject-on-exhaustion + `pool_exhausted_total`
- [ ] Milestone 4 (remaining): queued overload policy (deferred)

Implemented in commits:
- `3df28ed` — adds `src/server/pool.zig` and pool tests
- `020a5e9` — session/request/main refactor to pooled execution flow

## Current State

- `src/server/pool.zig` defines:
  - `PoolConn` with `tx_id`, `snapshot`, query buffers, and pin state
  - `ConnectionPool` with `checkout()`, `checkin()`, `pin()`, `unpin()`
  - `pool_exhausted_total` counter
- `src/server/session.zig` now checks out a pool connection per request and checks it back in after response serialization.
- `src/runtime/request.zig` now consumes `PoolConn` directly (`executeWithPoolConn`) and no longer owns slot leasing.
- `src/main.zig` now creates one `ConnectionPool` and passes it into `session.serveConnection(...)`.
- `src/tiger/error_taxonomy.zig` classifies `PoolExhausted` as `resource_exhausted`.

The target design is in `docs/CONNECTION_POOL.md`.

---

## Milestones

### Milestone 1: `PoolConn` and `ConnectionPool`
Status: [x] Done

Notes:
- Built on top of `BootstrappedRuntime.acquireQueryBuffers()` / `releaseQueryBuffers()`.
- Exposes pool-level errors (`PoolExhausted`, `InvalidPoolConn`, `PoolConnPinned`).
- Includes inline tests in `src/server/pool.zig`.

### Milestone 2: Refactor `Session` to Use `ConnectionPool`
Status: [x] Done

Notes:
- `Session.handleRequest(...)` now takes `*PoolConn`.
- `Session.serveConnection(...)` now takes `*ConnectionPool` and does checkout/checkin per request.
- Session tests now allocate/use `ConnectionPool` and validate `PoolExhausted` boundary behavior.

### Milestone 3: Refactor `main.zig`
Status: [x] Done

Notes:
- Removed global shared `tx_id`/`snapshot`.
- Server accept loop now passes `&pool` into `session.serveConnection(...)`.

### Milestone 4: Overload Behavior
Status: [~] In progress (reject mode complete; queue deferred)

Add configurable overload policy to `ConnectionPool`:

- **Reject** (default): `checkout()` returns `PoolExhausted` immediately. Session writes an error response and continues to the next request. This already works — it's the current `NoQuerySlotAvailable` behavior with a new name.
- **Queue** (deferred): Not needed yet. The reject path is sufficient for single-threaded io_uring. Queuing only matters with concurrent request dispatch, which requires the event loop work in Phase 4 to be further along.

Add a `pool_exhausted_total: u64` counter to `ConnectionPool` for monitoring.

---

## What to Defer

These are mentioned in `docs/CONNECTION_POOL.md` but should **not** be part of this work:

- **`auth_context` / `ClientSession`** — No authentication system exists. The `ClientSession` struct from the design doc is the right shape but has no backing implementation. Build it when auth is implemented.
- **Prepared statement cache** — No prepared statement support in the parser or executor. Placeholder field in `PoolConn` is not useful.
- **Temp table references** — No temp table support exists.
- **Transaction pinning for multi-statement txns** — The parser does not support `BEGIN`/`COMMIT`/`ROLLBACK` as explicit statements. Each request is auto-commit. `ConnectionPool.pin()` / `unpin()` exist, but session does not call them yet.
- **Queued overload policy** — Reject-on-exhaustion is the right default. Queuing adds complexity that isn't needed until concurrent dispatch exists.

---

## Build Order and Dependencies

```
Milestone 1 (pool.zig)
    │
    ▼
Milestone 2 (session.zig + request.zig refactor)
    │
    ▼
Milestone 3 (main.zig refactor)
    │
    ▼
Milestone 4 (overload counter)
```

Each milestone should end with `zig build test` passing. Milestone 3 additionally requires `zig build` to verify the server entry point compiles.

Progress note:
- `zig build test` and `zig build` pass after Milestones 1-3 and Milestone 4 reject-path updates.

## Files Touched

| File | Action |
|---|---|
| `src/server/pool.zig` | **New** — PoolConn, ConnectionPool |
| `src/server/session.zig` | Modify — remove tx_id/snapshot params, use PoolConn |
| `src/runtime/request.zig` | Modify — take PoolConn instead of separate tx_id/snapshot, remove slot management |
| `src/runtime/bootstrap.zig` | Possibly minor — pool.zig may call acquireQueryBuffers internally, or query buffer allocation moves into pool init |
| `src/main.zig` | Modify — create ConnectionPool, remove global tx/snapshot |
| `src/pg2.zig` | Modify — re-export `pool` module under `server` |

## Verification

After each milestone:

```bash
zig build test    # All inline tests pass (existing + new)
zig build         # Clean build, no warnings
```
