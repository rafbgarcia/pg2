# Storage Engine

## Page Manager

The fundamental unit of storage is an 8KB page. All disk I/O operates on whole pages.

### Page Layout

```
┌─────────────────────────────────────────┐
│ Page Header (24 bytes)                  │
│   page_id: u64                          │
│   page_type: enum { heap, btree, ... }  │
│   lsn: u64          (last WAL LSN)      │
│   checksum: u32                         │
├─────────────────────────────────────────┤
│ Type-specific content (8168 bytes)      │
│   (heap page: slotted page structure)   │
│   (btree page: sorted keys + children)  │
└─────────────────────────────────────────┘
```

### Buffer Pool

Fixed-size pool of in-memory page frames, allocated from the static memory budget at startup. Pages are loaded from disk on demand and evicted using clock sweep (approximation of LRU, same as PostgreSQL).

Key operations:
- `pin(page_id)` — load page into buffer pool if not present, increment pin count, return pointer
- `unpin(page_id, dirty: bool)` — decrement pin count, mark dirty if modified
- `flush(page_id)` — write dirty page to disk, update LSN

The buffer pool enforces the **WAL protocol**: a dirty page cannot be flushed to disk until its LSN has been flushed to the WAL first.

## Write-Ahead Log (WAL)

Every mutation is first written to the WAL before modifying any data page. The WAL is an append-only sequence of log records.

### WAL Record Format

```
┌───────────────────────────────────────┐
│ lsn: u64                             │
│ tx_id: u64                           │
│ record_type: enum {                  │
│   insert, update, delete,            │
│   tx_begin, tx_commit, tx_abort,     │
│   checkpoint, undo                   │
│ }                                    │
│ page_id: u64                         │
│ payload_len: u16                     │
│ payload: [payload_len]u8             │
│ crc32: u32                           │
└───────────────────────────────────────┘
```

### WAL Protocol

1. Begin transaction: write `tx_begin` record.
2. For each mutation: write the WAL record (containing both redo and undo information), then modify the in-memory page.
3. Commit: write `tx_commit` record, fsync the WAL. Only after fsync returns does the commit become durable.
4. Abort: write `tx_abort` record, apply undo records to roll back in-memory changes.

### WAL Segments and Retention

The WAL is divided into fixed-size segments (64 MiB each). Segments are retained for two purposes:

1. **Crash recovery.** On startup, the recovery process reads from the last checkpoint forward.
2. **Replica catch-up.** Replicas stream WAL from the primary starting at their last applied LSN.

Old segments are recycled once they are older than the last checkpoint AND have been consumed by all connected replicas. The `--wal-retention` flag sets the maximum number of segments retained beyond the checkpoint (default: 16, i.e. ~1 GiB of WAL history).

If a replica falls behind beyond the retention window, it cannot catch up via WAL streaming and must be re-initialized from a base backup.

### Recovery

Current runtime behavior is fail-closed:
1. Clean shutdown flushes buffered WAL, flushes dirty pages, and marks a clean WAL envelope.
2. Startup reads the WAL envelope.
3. If startup detects an unclean shutdown boundary (non-empty durable WAL envelope), boot fails closed and requests recovery replay support.

Full ARIES-style redo/undo replay is planned but not yet enabled in the production startup path.

## Heap Storage

Rows are stored in heap pages using a slotted page layout.

```
┌──────────────────────────────────────┐
│ Page Header                          │
├──────────────────────────────────────┤
│ Slot Array (grows downward) →        │
│  [offset, len] [offset, len] ...     │
├──────────────── free space ──────────┤
│              ← Rows (grow upward)    │
│  [row bytes] [row bytes] ...         │
└──────────────────────────────────────┘
```

A row is identified by a `RowId = (page_id, slot_index)`. Unlike PostgreSQL, updates happen **in-place** — the old version is pushed to the undo log, not kept in the heap.

### V1 String Overflow Topology

For v1, large string payloads spill to overflow pages in the same DB storage file.

- Overflow pages are a dedicated page type.
- Allocation uses a dedicated overflow page-id region (deterministic baseline allocator).
- This is intentionally single-file topology (not separate OS files) to reduce operational complexity while the storage contracts are being hardened.

## B-Tree Index

Standard B+ tree over 8KB pages. Leaf pages contain `(key, RowId)` pairs. Internal pages contain `(key, child_page_id)` pairs.

Operations: point lookup, range scan, insert, delete.

Deletes do not rebalance (same approach as PostgreSQL). Page splits are WAL-logged as atomic operations.

## Undo-Log MVCC

### How It Works

Each row in the heap contains the **latest version**. When a transaction updates a row:

1. Copy the current row content into the undo log (linked to the transaction).
2. Update the row in-place in the heap page.
3. The undo log entry points to the previous undo entry (forming a version chain).

When a transaction reads a row:
1. Read the current version from the heap.
2. Check if the current version is visible to this transaction's snapshot.
3. If not, follow the undo chain backward until finding a version that is visible.

### Transaction Visibility

Each transaction gets a snapshot at `BEGIN`: the set of transaction IDs that were committed at that point. A row version is visible if the transaction that created it is in the snapshot's committed set.

### Garbage Collection

Undo log entries can be discarded once no active transaction could possibly need them (i.e., the oldest active snapshot is newer than the entry). This is a truncation of the undo log tail — no heap page scanning required, unlike PostgreSQL's VACUUM.
