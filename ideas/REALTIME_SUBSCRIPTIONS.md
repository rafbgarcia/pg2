# Real-Time Subscriptions

Design notes for pg2's real-time data mutation subscription system.

Subscription handles are **not** pool connections — they don't consume query execution resources and don't count toward the pool limit. See `docs/CONNECTION_POOL.md` for the connection pool architecture.

## Subscription Model

Subscriptions are lightweight registrations, not database connections. Each subscription is a filter predicate + a socket file descriptor, managed by a **notification engine** that is separate from the query execution path.

### Topic-Based Channel Routing

Subscribers register to channels with hierarchical names:

- `table:users` — all mutations on the users table
- `table:users:row:42` — mutations on a specific row

Channel lookup is a hash map: O(1) to find the subscriber list for a channel. Fan-out within a channel is O(subscribers in that channel).

### Notification Payloads

Notifications are lightweight events, not full row data:

```
{ table: "users", op: "update", row_id: 42 }
```

Clients re-fetch via a query connection if they need the full row. This decouples notification throughput from row size — a 10MB blob column update produces a ~50 byte notification.

### Fan-Out via io_uring

On Linux, fan-out uses io_uring to batch all subscriber sends into a single syscall:

```
for each subscriber in channel:
    sqe = io_uring.get_sqe()
    sqe.prep_send(subscriber.fd, notification)
io_uring.submit()  // one syscall, kernel processes all sends
```

The kernel handles the sends internally, potentially across cores. For very large channels (100k+), the subscriber list can be partitioned across separate io_uring instances on different threads — the notification payload is read-only, so no locking is needed.

Completions are reaped in batch to handle failures (disconnected clients, full send buffers) and retries (short writes).

### Backpressure

A slow subscriber is a client whose socket send buffer is full (high latency, congested network, slow client code). pg2 cannot let one slow client stall fan-out to others.

Three configurable policies per subscription:

- **Drop**: skip the notification for this subscriber. Good for best-effort use cases (dashboards, analytics).
- **Buffer**: queue in userspace memory, retry later. Requires a max queue size, after which it falls back to drop or disconnect.
- **Disconnect**: close the socket, remove the subscriber. Client reconnects when ready.

### Per-Socket Buffer Tuning

Subscription sockets use `setsockopt` to set buffer sizes different from query connections:

| Socket type | Receive buffer | Send buffer | Rationale |
|---|---|---|---|
| Query connection | ~256KB (kernel default) | ~256KB (kernel default) | Queries and result sets can be large |
| Subscription | ~4KB | ~8-16KB | Clients send almost nothing; notifications are small |

At ~24KB kernel memory per subscription socket (vs ~256KB default), 1M subscriptions costs ~24GB kernel memory.

## Modular Design

The notification engine is a standalone module with no dependency on pg2 internals (no direct access to the buffer pool, executor, or catalog). It depends only on:

1. **A change event stream** — (table, operation, row id, optional payload).
2. **A channel registry** — subscribe/unsubscribe API.
3. **A socket I/O interface** — real io_uring in production, fake implementation in simulation/tests.

The same module can be deployed in two modes without code changes. The only difference is where the change event stream comes from.

### Embedded (default)

The notification engine runs inside the pg2 server process and reads WAL events directly from memory.

```
                       ┌──────────────────────────────────────────────┐
                       │                 pg2 server                   │
                       │                                              │
                       │  ┌───────────┐      ┌─────────────────────┐ │
Query clients ────TCP────►│  Conn     │─────►│  Query Engine        │ │
(borrow pool conn)     │  │  Pool     │      │  (executor, txn)    │ │
                       │  └───────────┘      └──────────┬──────────┘ │
                       │                                │ WAL events │
                       │                                │ (memory)   │
                       │                                ▼            │
                       │                     ┌─────────────────────┐ │
Subscription ─────TCP────────────────────►   │  Notification       │ │
clients                │                     │  Engine             │ │
                       │                     │  (channels, fanout) │ │
                       │                     └─────────────────────┘ │
                       └──────────────────────────────────────────────┘
```

Query clients and subscription clients both connect to the same pg2 process. Simple deployment, good for up to tens of thousands of subscriptions.

### Extracted (scale-out)

The notification engine runs as a separate process and connects to pg2 as a replication client (same protocol as read replicas). pg2 sends each WAL event once over that single replication connection. The fan-out server distributes to all subscribers.

```
                       ┌──────────────────────────────────┐
                       │          pg2 server               │
                       │                                   │
                       │  ┌───────────┐   ┌─────────────┐ │
Query clients ────TCP────►│  Conn     │──►│  Query      │ │
                       │  │  Pool     │   │  Engine     │ │
                       │  └───────────┘   └──────┬──────┘ │
                       │                         │        │
                       └─────────────────────────┼────────┘
                                                 │ WAL stream
                                                 │ (replication protocol)
                                                 ▼
                       ┌──────────────────────────────────┐
                       │       Fan-Out Server(s)           │
                       │                                   │
                       │  ┌─────────────────────────────┐  │
Subscription ─────TCP────►│  Notification Engine         │  │
clients                │  │  (channels, fanout,          │  │
                       │  │   io_uring, backpressure)    │  │
                       │  └─────────────────────────────┘  │
                       └──────────────────────────────────┘
```

Multiple fan-out servers can each handle a subset of channels for horizontal scaling. The replication infrastructure (Phase 5: WAL streaming) is shared between read replicas and fan-out servers.

## Scaling Expectations

| Subscription count | Deployment | Notes |
|---|---|---|
| Up to ~50k | Embedded, single io_uring ring | No special tuning needed |
| 50k-500k | Embedded, per-socket buffer tuning | ~12GB kernel memory at 500k |
| 500k+ | Extracted fan-out tier(s) | pg2 sends one event to fan-out, fan-out distributes to subscribers |
