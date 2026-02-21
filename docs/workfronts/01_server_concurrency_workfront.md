# Workfront 01: Server Concurrency

## Objective
Remove connection-serial request handling so multiple client connections can make progress concurrently.

## Why
- Current main loop serves one accepted connection until disconnect, which blocks other clients.
- `--concurrency` and pool sizing are not meaningful until multiple in-flight requests are possible.

## Inputs
- `ideas/CONNECTION_POOL.md` two-layer model (client sockets vs pool execution contexts).

## Phase 1: Non-blocking Accept + Session Registry
### Scope
- Accept connections continuously.
- Maintain connection session table (socket + parser state + pending request metadata).
- Do not execute query inline in accept loop.

### Gate
- Two clients can stay connected simultaneously.
- No regression in single-client behavior tests.

## Phase 2: Request Scheduler + Queue
### Scope
- Add central scheduler:
  - ready requests queue
  - waiting-for-pool queue
  - bounded queue timeout
- Default overload behavior: queue with 30s timeout.

### Gate
- Deterministic tests for queue success and queue timeout.
- Queue metrics emitted (wait time, timeouts, depth).

## Phase 3: Execution Dispatch
### Scope
- Dispatch request execution without blocking accept/read/write progress.
- Initial implementation can be single execution worker if needed, but scheduler must keep transport responsive.
- Add explicit `--concurrency` runtime cap for in-flight request execution.

### Gate
- Client B requests make progress while Client A stays connected.
- In-flight request cap is enforced and observable.

## Phase 4: Transaction Pinning Semantics
### Scope
- Implement pinned pool contexts for multi-statement transactions per client session.
- Ensure BEGIN/COMMIT/ROLLBACK correctly pin/unpin execution context.

### Gate
- Deterministic transaction pinning tests across interleaved client sessions.
- Pinned count and pin duration exposed in stats.
