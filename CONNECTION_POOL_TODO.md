# Connection Pool — Implementation Plan

## Current State

The runtime already has a proto-pool: query slots in `src/runtime/bootstrap.zig`.

- `QueryBuffers` — per-slot result + scratch row buffers (`bootstrap.zig:38`)
- `query_slot_in_use` bool array — free-list equivalent (`bootstrap.zig:53`)
- `acquireQueryBuffers()` / `releaseQueryBuffers()` — checkout/return (`bootstrap.zig:130-170`)
- `LeasedExecution` — holds a slot until the caller finishes consuming results (`src/runtime/request.zig:31`)

The session layer (`src/server/session.zig`) receives `tx_id` and `snapshot` from the **caller** — it does not own transaction lifecycle. In `main.zig:107-114`, a single global transaction and snapshot are shared across all connections.

Transport abstractions exist (`src/server/transport.zig`): `Connection` (read/write) and `Acceptor` (accept loop), with TCP and io_uring backends.

The target design is in `docs/CONNECTION_POOL.md`.

---

## What to Build

### Milestone 1: `PoolConn` and `ConnectionPool`

Create `src/server/pool.zig`.

**`PoolConn`** wraps existing `QueryBuffers` and adds per-connection transaction state:

```
PoolConn {
    slot_index: u16,
    query_buffers: QueryBuffers,     // existing — result_rows, scratch_rows_a/b
    tx_id: TxId,                     // owned, not passed in
    snapshot: Snapshot,              // owned, not passed in
    pinned: bool,                   // false = auto-commit mode, true = in explicit txn
}
```

**`ConnectionPool`** is a fixed-capacity free list of `PoolConn`:

```
ConnectionPool {
    conns: []PoolConn,               // pre-allocated array (max pool_size)
    free_bitmap: []bool,             // mirrors current query_slot_in_use pattern
    pool_size: u16,                  // configurable, default from BootstrapConfig
    tx_manager: *TxManager,          // reference, used to begin/commit/abort per checkout
    runtime: *BootstrappedRuntime,   // reference, used to map slot → query buffers
}
```

Methods:

- `checkout() -> error{PoolExhausted}!*PoolConn` — finds a free slot, calls `tx_manager.begin()`, takes a snapshot, marks slot in-use, returns the PoolConn.
- `checkin(conn: *PoolConn) -> void` — if `conn.pinned`, this is a bug (assert). Otherwise: commits or aborts the transaction, marks slot free.
- `pin(conn: *PoolConn) -> void` — marks the conn as pinned (for future multi-statement txn support).
- `unpin(conn: *PoolConn) -> void` — commits/aborts and returns to pool.

**Pre-allocation**: All `PoolConn` structs are allocated during `BootstrappedRuntime.init`, before the static allocator is sealed. No heap allocation at runtime.

**Tests** (inline in `pool.zig`):

- Checkout returns a PoolConn with a valid tx_id and snapshot.
- Checkin makes the slot reusable.
- Checkout when pool is exhausted returns `PoolExhausted`.
- Double-checkin is an invariant violation (assert or error).
- Pool size matches configured `max_query_slots`.

### Milestone 2: Refactor `Session` to Use `ConnectionPool`

Modify `src/server/session.zig`.

**Before** (current):
```zig
pub fn handleRequest(self, tx_id, snapshot, source, out) -> ...
pub fn serveConnection(self, connection, tx_id, snapshot, req_buf, resp_buf) -> ...
```

**After**:
```zig
pub fn handleRequest(self, pool_conn: *PoolConn, source, out) -> ...
pub fn serveConnection(self, connection, pool: *ConnectionPool, req_buf, resp_buf) -> ...
```

`serveConnection` does checkout/checkin per request:

```
while (request = connection.readRequest()) {
    pool_conn = pool.checkout()  // or write PoolExhausted error and continue
    response = handleRequest(pool_conn, request, response_buf)
    connection.writeResponse(response)
    pool.checkin(pool_conn)
}
```

The session no longer receives `tx_id`/`snapshot` — those live inside the `PoolConn`.

**Refactor `request.zig`**: `ExecuteRequest` drops `tx_id` and `snapshot` fields. Instead, `executeWithLeasedQueryBuffers` takes them from the `PoolConn`. Or simpler: delete `LeasedExecution` and the slot-management code from `request.zig` entirely — the pool now owns slot lifecycle. `request.zig` becomes a thin adapter that builds an `ExecContext` from a `PoolConn`.

**Update tests**: All 4 existing session tests need to create a `ConnectionPool` instead of passing tx_id/snapshot directly.

### Milestone 3: Refactor `main.zig`

Remove the global `tx_id`/`snapshot` from `main.zig:107-114`.

**Before**:
```zig
const tx_id = runtime.tx_manager.begin();
var snapshot = runtime.tx_manager.snapshot(tx_id);
// ... passed to every serveConnection call
```

**After**:
```zig
var pool = ConnectionPool.init(&runtime);
// ... passed to session, which does per-request checkout
```

The accept loop becomes:

```zig
while (true) {
    const connection = acceptor.accept() orelse continue;
    session.serveConnection(connection, &pool, request_buf, response_buf);
}
```

### Milestone 4: Overload Behavior

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
- **Transaction pinning for multi-statement txns** — The parser does not support `BEGIN`/`COMMIT`/`ROLLBACK` as explicit statements. Each request is auto-commit. The `pin`/`unpin` methods on `ConnectionPool` should exist as stubs (assert-guarded) so the interface is ready, but the session layer should not call them yet.
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
