# Built-In Connection Pool

Design notes for pg2's built-in connection pool.

## Why Built-In

External connection pools (PgBouncer, PgCat) sit as a separate proxy process between clients and the database. A built-in pool eliminates that operational overhead and lets pg2 make smarter decisions — it can distinguish socket types (query vs subscription), tune kernel buffers per socket, and pin pool connections during transactions without external coordination.

## Two-Layer Architecture

```
Client A ──┐                          ┌── Pool Conn 1 ──┐
Client B ──┤── Client Connections ──► │── Pool Conn 2 ──┤──► Query Engine
Client C ──┤   (long-lived TCP)       │── Pool Conn 3 ──┤   (executor, txn mgr)
...        │                          └── ...           ┘
Client N ──┘
```

### Client Connections (left side)

A client connection is a long-lived TCP socket between the application and pg2. It persists for the lifetime of the client session. These are cheap:

- A socket file descriptor
- Auth context (client identity, permissions)
- A pointer to a pool connection (null when idle)

The number of client connections is bounded only by fd limits and kernel memory for socket buffers. With default kernel buffer sizes (~256KB per socket), practical limit is tens of thousands. With tuned buffers, hundreds of thousands.

### Pool Connections (right side)

A pool connection is an internal execution context — the expensive per-session state needed to actually run queries:

- Transaction state (active txn id, snapshot, undo log cursor)
- Scratch buffers for the executor (sort memory, hash join build side, etc.)
- Prepared statement cache
- Temp table references

There is a fixed number of these (configurable, default 64). This is what protects the engine from overload — the buffer pool, lock manager, and executor are sized for this concurrency level.

## Query Lifecycle

1. Client sends a query over its TCP socket.
2. Server I/O loop reads the message from io_uring completion.
3. Server **checks out** a pool connection from the free list.
   - If none available: return a "too busy" error or queue the request (configurable).
4. Query executes using that pool connection's execution context.
5. Result is written back to the client's TCP socket via io_uring.
6. Pool connection state is reset (rollback any uncommitted txn, clear scratch buffers).
7. Pool connection is **returned** to the free list.
8. Client's TCP socket stays open, idle, waiting for the next query.

The client never knows the pool exists.

## Transaction Pinning

A single query is check-out-execute-return. Multi-statement transactions change this:

```
Client A: BEGIN
Client A: INSERT INTO users ...    ← pool conn pinned
Client A: UPDATE accounts ...      ← same pool conn
Client A: COMMIT                   ← pool conn returned
```

The pool connection must be **pinned** to the client for the entire transaction. The transaction state (uncommitted writes, locks held, snapshot) lives in that execution context. Returning it between statements would lose that state.

The pool connection goes back to the free list only after COMMIT or ROLLBACK.

This is why long-running transactions are the primary pool exhaustion risk — they hold a pool slot for their entire duration. Monitoring should expose pinned connection count and pin duration.

## Data Structures

```
ClientSession {
    socket: fd,                // TCP connection to the client (long-lived)
    pool_conn: ?*PoolConn,     // null when idle, set when executing or in a txn
    client_id: u64,
    auth_context: AuthContext,
}

PoolConn {
    txn_state: *TxnContext,
    scratch_buffers: [...]u8,
    pinned_to: ?*ClientSession,  // non-null during a transaction
}

ConnectionPool {
    free_list: BoundedQueue(*PoolConn),
    pool_size: u16,              // configurable, default 64
}
```

Checkout is O(1) (pop from free list). Return is O(1) (push to free list). No contention in a single-threaded io_uring event loop.

## Relationship to Subscriptions

Subscription handles are **not** pool connections. A subscription is a lightweight registration (filter + socket fd) managed by the notification engine (see `docs/REALTIME_SUBSCRIPTIONS.md`). Subscriptions don't need transaction state, scratch buffers, or any executor resources. They don't count toward the pool limit.

Subscription sockets also use different kernel buffer sizes than query sockets — see `docs/REALTIME_SUBSCRIPTIONS.md` for per-socket tuning details.

## Overload Behavior

When all pool connections are in use and a new query arrives:

- **Queue** (default): hold the request in a bounded wait queue. If a pool connection frees up before timeout (default target: 30s), execute it; otherwise reject.
- **Reject**: return an error immediately. Client retries with backoff.

The choice remains configurable per deployment.
