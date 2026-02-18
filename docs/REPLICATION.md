# Replication

## Model

Single primary, multiple async read replicas. All writes go to the primary. Replicas stream the WAL and apply it locally to serve read queries.

This model pairs naturally with the single-writer architecture: the primary serializes all writes through one WAL, and replicas replay that WAL in order. No conflict resolution, no consensus protocol, no multi-writer coordination.

```
         Writes
           │
           ▼
      ┌─────────┐
      │ Primary  │──── WAL ────┬──────────────┐
      └─────────┘              │              │
                               ▼              ▼
                         ┌──────────┐   ┌──────────┐
          Reads ◄────── │ Replica1 │   │ Replica2 │ ──────► Reads
                         └──────────┘   └──────────┘
```

## WAL Streaming

The primary maintains the WAL as described in the storage engine doc. Replicas connect to the primary and request WAL records starting from a given LSN.

### Protocol

1. **Replica → Primary**: `REPLICATE(start_lsn)` — "send me all WAL records from this LSN onward."
2. **Primary → Replica**: streams WAL records as they are written. The primary retains WAL segments up to the `--wal-retention` limit (default: 16 segments, ~1 GiB).
3. **Replica applies records**: redo-only (no undo needed since the primary already decided commit/abort). The replica replays committed transactions into its local pages.

### Replica Lag

Replicas are eventually consistent. A replica may be behind the primary by some number of WAL records. The replica tracks its applied LSN. Clients reading from a replica can query its current LSN to understand staleness.

```
replica.applied_lsn   → 10042
primary.committed_lsn → 10050
lag                    → 8 records
```

The query response from a replica includes the replica's LSN, so the application can make informed decisions about staleness.

### Replica Falls Behind

If a replica disconnects or falls behind beyond the WAL retention window, it cannot resume streaming. The primary no longer has the WAL segments the replica needs.

Recovery options:
1. **Base backup + replay.** Take a snapshot of the primary's data directory, ship it to the replica, and restart streaming from the snapshot's LSN.
2. **Manual re-initialization.** Drop the replica and provision a new one from a fresh base backup.

The primary rejects `REPLICATE(start_lsn)` requests where `start_lsn` is older than the oldest retained WAL segment, returning an explicit error with the oldest available LSN.

## Read-Your-Writes

A client that writes to the primary and then reads from a replica might not see its own write (because the replica hasn't caught up). Two approaches:

1. **Client-side**: after a write, the client receives the commit LSN. When reading from a replica, the client passes `min_lsn` — the replica blocks until it has applied at least that LSN, or returns an error if it takes too long.
2. **Route to primary**: for reads that must see the latest writes, route to the primary.

pg2 implements option 1 — it keeps the replica useful and makes the consistency tradeoff explicit to the application.

## Promotion

If the primary dies, a replica can be promoted. The promoted replica:

1. Stops applying WAL from the old primary.
2. Runs undo recovery for any in-progress transactions from the old primary's WAL.
3. Starts accepting writes.
4. Other replicas re-connect to the new primary.

Initial implementation: promotion is a manual operator action. Automatic failover (heartbeats, leader election) is a separate concern that requires a consensus protocol and is deferred to a later phase.

## Future Work

- Synchronous replication (primary waits for replica acknowledgment before committing)
- Automatic failover with leader election
- Read-your-writes at the protocol level (proxy layer)
- Multi-region replicas with latency-aware routing
