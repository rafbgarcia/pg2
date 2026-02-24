# Workfront 01: Server Concurrency

## Objective
Remove connection-serial request handling so multiple client connections can make progress concurrently.

## Session Handoff Snapshot (2026-02-24)

### Current Status

- Phase 1 baseline is now landed:
  - `src/main.zig` runs a bounded `ServerReactor` loop rather than per-connection blocking `serveConnection`.
  - `src/server/reactor.zig` exists with static session table, round-robin read/dispatch/write progression, and deterministic tests.
  - `Session.dispatchRequest(...)` exists as a reusable single-request execution boundary for reactor integration.
- Transport contract foundation is landed:
  - `ConnectionError.WouldBlock` and `Connection.close()` are part of `src/server/transport.zig`.
  - TCP transport now preserves partial request/write state across retries.
  - io_uring transport moved from blocking waits to non-blocking poll progression (`copy_cqes(..., 0)` with pending op state).
- Deterministic internals coverage is landed for progress contracts:
  - `test/internals/server/transport_progress_test.zig` covers `WouldBlock` retry semantics and idempotent close.
- Phase 2 scheduler layer is now landed:
  - `src/server/reactor.zig` now uses fixed-capacity `ready_queue`, `dispatch_queue`, and `timeout_heap`.
  - Queue timeout uses injected `io.Clock` (`RealClock` in `src/main.zig`, deterministic clocks in tests).
  - One-queued-request-per-session admission is enforced by per-session queue state.
  - Deterministic overload responses are emitted at scheduler boundary:
    - `ERR class=overload code=QueueFull`
    - `ERR class=overload code=QueueTimeout`
  - Deterministic internals coverage added in `test/internals/server/reactor_queueing_test.zig`:
    - queue saturation (`QueueFull`)
    - exact deadline timeout boundary (`QueueTimeout`)
    - round-robin fairness across 4 sessions
- Remaining gaps:
  - `--concurrency` parsing/validation is landed in `src/main.zig` + `src/runtime/config.zig`, but execution remains locked to 1 worker.
  - Single-worker async dispatch handoff is landed in `src/server/reactor.zig` (reactor no longer executes request bodies inline).
  - No transaction pinning semantics in reactor/session state yet (Phase 4 pending).

### Commits Landed (Latest First)

- `8059d72` `server: introduce bounded reactor and wire main loop`
- `0d9e4eb` `server: add non-blocking transport progress contracts`

### Decision Lock (No Ambiguity)

1. **Execution model for WF01:** single transport reactor thread + bounded execution worker set.
2. **No per-connection threads:** connection state is multiplexed by reactor through a static session table.
3. **Queue timeout semantics:** scheduler timeout is deadline-based, measured from enqueue to dispatch, using an injected `io.Clock` (real clock in prod, simulated clock in tests).
4. **Fairness contract:** round-robin at session granularity; each ready session can advance by at most one request frame per scheduler cycle.
5. **Fail-closed overload behavior:** default policy is queue with 30s timeout, deterministic error on timeout.

### Locked Decision Details (Production Contracts)

#### A. Scheduler Data Structures and Capacity

1. Use fixed-capacity structures only (allocated at startup):
   - `ready_queue`: ring buffer of session ids with complete request frame.
   - `dispatch_queue`: ring buffer of request descriptors admitted for worker dispatch.
   - `timeout_heap`: min-heap by deadline tick for queued requests.
2. Keep fd-facing session capacity distinct from execution capacity:
   - `max_sessions` bounds connected client sessions.
   - `max_inflight` (from `--concurrency`) bounds concurrently executing requests.
3. Default queue capacity is `max_sessions`, with explicit configurable override.
4. Enforce at most one queued request per session to prevent one client from monopolizing queue memory.
5. Overload responses are deterministic:
   - full queue: `ERR class=overload code=QueueFull`
   - deadline exceeded before dispatch: `ERR class=overload code=QueueTimeout`

#### B. Reactor and `io_uring` Progress Semantics

1. Reactor tick order is fixed:
   - drain completions (accept/read/write)
   - expire timeout heap entries
   - promote ready sessions to dispatch queue (respect fairness and worker budget)
   - submit new SQEs (accept/read/write)
2. Keep these invariants:
   - one in-flight read per session socket
   - one in-flight write per session socket
   - at least one accept SQE posted while below `max_sessions`
3. Request framing remains bounded newline-delimited accumulation.
4. Response writes are resumable/partial-write safe (`bytes_sent` tracking until full flush).
5. Fatal socket error closes session and runs one idempotent cleanup path.

#### C. Transaction Pinning Failure and Cleanup

1. `BEGIN` pins pool context to session; `COMMIT`/`ROLLBACK` unpins and returns slot.
2. On client disconnect with pinned context:
   - force rollback
   - release pool slot
   - clear pin/session binding
3. On response write failure after execution:
   - close session
   - run disconnect cleanup (rollback if still active)
4. Queue timeout before dispatch does not alter pin state.
5. Session cleanup must be idempotent and safe to call multiple times.

#### D. Observability Contract

1. Required gauges:
   - `sessions_active`
   - `sessions_pending_request`
   - `workers_busy`
   - `queue_depth`
   - `pool_checked_out`
   - `pool_pinned`
2. Required counters:
   - `requests_enqueued_total`
   - `requests_dispatched_total`
   - `requests_completed_total`
   - `queue_full_total`
   - `queue_timeout_total`
   - `session_disconnect_total`
   - `pool_exhausted_total`
3. Required latency/age signals (histogram or fixed buckets):
   - `queue_wait_ticks`
   - `request_exec_ticks`
   - `pin_duration_ticks`
4. Enforce debug/test invariants:
   - `requests_enqueued_total >= requests_dispatched_total >= requests_completed_total`
   - `pool_pinned <= pool_checked_out <= pool_size`
   - `queue_depth <= max_queued_requests`
   - no session can be both `closed` and `inflight`

## Why

- Current server path blocks other clients while one connected client is being served.
- Runtime concurrency knobs are not meaningful until requests are scheduled independently from accept/read/write progress.
- Later workfronts assume stable runtime concurrency boundaries and observability.

## Inputs
- `ideas/CONNECTION_POOL.md` two-layer model (client sockets vs pool execution contexts).
- `docs/workfronts/14_runtime_storage_backend_workfront.md` for production storage backend wiring; WF01 keeps transport/scheduling concerns isolated from storage implementation details.

## Non-Negotiables

1. Keep core execution paths on `Storage`/`Network` abstractions.
2. Preserve deterministic behavior in tests via fake transport + simulated clock.
3. No unbounded dynamic allocation in hot server loop; session/scheduler storage must be bounded.
4. Any overload/timeout path must return explicit deterministic boundary errors.

## Phase 1: Reactor Foundation (Non-blocking Accept + Session Registry)

### Scope

- Add a `ServerReactor` boundary that:
  - continuously accepts new sockets;
  - tracks active sessions in a bounded static session table;
  - reads request frames without entering execute path inline.
- Refactor main server loop to call reactor step functions rather than `serveConnection(...)` directly.
- Add basic per-session lifecycle states (`open`, `has_request`, `closed`).

### Implementation Slices

1. Introduce session registry type in `src/server/` with fixed max sessions.
2. Wire non-blocking/poll-style accept/read loop and request capture.
3. Keep response path temporarily synchronous to de-risk bring-up.
4. Add focused tests for two simultaneously connected clients with no execution dispatch yet.

### Gate

- Two clients can remain connected simultaneously without forced serialization by accept loop.
- Single-client behavior remains green.
- No request execution occurs inline inside acceptor iteration.
- **Status:** ✅ completed by commits `0d9e4eb` and `8059d72`.

## Phase 2: Request Scheduler + Queue

### Scope

- Add central scheduler with:
  - ready queue (sessions with complete request frames),
  - dispatch queue (bounded by execution worker capacity),
  - timeout queue (deadline ordering for queued requests).
- Enforce queue timeout using injected clock.
- Add explicit boundary error for queue timeout response path.
- Add one-queued-request-per-session admission guard.
- Use fixed-capacity queue + timeout heap allocated at reactor init.

### Gate

- Deterministic tests cover:
  - enqueue/dequeue success under temporary saturation;
  - queue timeout at exact configured deadline;
  - fairness across multiple sessions.
- Metrics emitted: queue depth, total enqueued, total timed out, max wait.
- **Status:** ✅ completed by commit `72fc7b6` (2026-02-24).

## Phase 3: Execution Dispatch

### Scope

- Dispatch request execution without blocking accept/read/write progression.
- Introduce bounded execution workers (start with 1 worker, then scale to `--concurrency`).
- Add `--concurrency <n>` CLI flag and runtime validation.
- Ensure one connection with slow/heavy queries does not block reactor progress for others.

### Gate

- Client B request/response progresses while Client A remains connected and active.
- In-flight execution cap is enforced and observable in stats.
- Fail-closed behavior when worker queue is saturated.
- **Status:** ⏳ partially started (reactor progression + single-worker async handoff + `--concurrency` CLI validation landed; worker scaling to `n > 1` pending).

## Phase 4: Transaction Pinning Semantics

### Scope

- Implement session-scoped pool pinning for multi-statement transactions.
- BEGIN pins a pool context to session; COMMIT/ROLLBACK unpins and returns slot.
- Ensure queued requests for pinned sessions preserve ordering and do not steal other session pins.
- Expose pin-related stats (active pins, pin wait, pin duration).

### Gate

- Deterministic tests validate interleaved session transactions with correct pin/unpin lifecycle.
- No pool slot leaks across disconnect/timeout/error paths.
- Pin stats are surfaced in inspect/runtime diagnostics.
- **Status:** ⏳ not started.

## Verification Matrix

- `zig build test --summary all`
- Add targeted deterministic tests under:
  - `test/features/server_concurrency/` for user-facing multi-client behavior.
  - `test/internals/server/` for scheduler/timeout/fairness contracts.

## Next Commit Slice (Start Here)

1. Begin Phase 3 execution dispatch hardening:
   - keep worker count locked at 1 (landed)
   - prove dispatch queue/worker budget semantics under sustained mixed session load (landed by internals tests)
2. Introduce `--concurrency <n>` parser/validation wiring (landed; currently rejects `n > 1`).
3. Scale worker budget to honor validated `--concurrency` values > 1 while preserving bounded static allocation.
4. Extend deterministic tests to cover mixed fast/slow dispatch completion ordering at `n > 1`.

## Hard-Stop Conditions

- Stop immediately if any slice introduces unbounded allocation in reactor/scheduler hot path.
- Stop immediately if timeout behavior depends on wall-clock access that is not injected/controllable in tests.
- Stop immediately if response ordering for a single session can become non-deterministic.
