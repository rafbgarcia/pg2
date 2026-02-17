# Replication

## Model

Single primary, multiple async read replicas. All writes go to the primary. Replicas stream the WAL and apply it locally to serve read queries.

This is the simplest replication model that is still useful. It avoids the complexity of multi-writer, consensus protocols, and conflict resolution.

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
2. **Primary → Replica**: streams WAL records as they are written. The primary keeps a configurable amount of WAL history for replicas that fall behind.
3. **Replica applies records**: redo-only (no undo needed since the primary already decided commit/abort). The replica replays committed transactions into its local pages.

### Replica Lag

Replicas are eventually consistent. A replica may be behind the primary by some number of WAL records. The replica tracks its applied LSN. Clients reading from a replica can query its current LSN to understand staleness.

```
replica.applied_lsn   → 10042
primary.committed_lsn → 10050
lag                    → 8 records
```

The query response from a replica includes the replica's LSN, so the application can make informed decisions about staleness.

## Read-Your-Writes

A client that writes to the primary and then reads from a replica might not see its own write (because the replica hasn't caught up). Two approaches:

1. **Client-side**: after a write, the client receives the commit LSN. When reading from a replica, the client passes `min_lsn` — the replica blocks until it has applied at least that LSN, or returns an error if it takes too long.
2. **Route to primary**: for reads that must see the latest writes, route to the primary.

Implement option 1 — it keeps the replica useful and teaches the consistency tradeoffs.

## Promotion

If the primary dies, a replica can be promoted. The promoted replica:

1. Stops applying WAL from the old primary.
2. Runs undo recovery for any in-progress transactions from the old primary's WAL.
3. Starts accepting writes.
4. Other replicas re-connect to the new primary.

Automatic failover detection (heartbeats, leader election) is out of scope for the initial implementation. Promotion is a manual operation. This avoids needing a consensus protocol.

## Build Steps

1. **Implement WAL streaming on the primary** — a simple TCP server that sends WAL records to connected replicas.
2. **Implement WAL receiver on the replica** — connects to the primary, receives records, applies them (redo-only).
3. **Implement LSN tracking on replicas** — expose applied LSN to clients.
4. **Implement `min_lsn` on replica reads** — block or error if replica is behind.
5. **Implement manual promotion** — stop replication, run undo recovery, start accepting writes.
6. **Test under simulation**: network partitions between primary and replica, primary crash + promotion, replica lag under write load, split-brain prevention (two nodes both think they're primary).

## Future (Out of Scope Initially)

- Synchronous replication (primary waits for replica acknowledgment before committing)
- Automatic failover with leader election
- Read-your-writes at the protocol level (proxy layer)
- Multi-region replicas with latency-aware routing
