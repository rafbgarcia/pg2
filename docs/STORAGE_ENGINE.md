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

Fixed-size pool of in-memory page frames. Pages are loaded from disk on demand and evicted using clock sweep (approximation of LRU, simpler to implement, used by PostgreSQL).

Key operations:
- `pin(page_id)` — load page into buffer pool if not present, increment pin count, return pointer
- `unpin(page_id, dirty: bool)` — decrement pin count, mark dirty if modified
- `flush(page_id)` — write dirty page to disk, update LSN

The buffer pool must enforce the **WAL protocol**: a dirty page cannot be flushed to disk until its LSN has been flushed to the WAL first.

### Build Steps

1. Implement the page struct and serialization (read/write raw bytes).
2. Implement the buffer pool with pin/unpin, clock sweep eviction.
3. Add checksum verification on page read.
4. Test under simulation: inject read errors, verify checksums catch corruption.

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

### Recovery

On startup:
1. Read WAL forward from the last checkpoint.
2. **Redo phase**: replay all records to bring pages up to date.
3. **Undo phase**: roll back any transactions that didn't commit (no `tx_commit` record found).

This is the standard ARIES-style recovery protocol, simplified.

### Build Steps

1. Implement WAL record serialization and append-only file writing.
2. Implement WAL fsync and group commit (batch multiple commits into one fsync).
3. Implement redo recovery.
4. Implement undo recovery.
5. Test under simulation: crash at every possible point in the commit sequence, verify recovery produces a consistent state.

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

### Build Steps

1. Implement slotted page insert/read/delete.
2. Implement in-place update (modify slot content, push old value to undo log).
3. Implement free space tracking (free list or free space map).

## B-Tree Index

Standard B+ tree over 8KB pages. Leaf pages contain `(key, RowId)` pairs. Internal pages contain `(key, child_page_id)` pairs.

Operations: point lookup, range scan, insert, delete.

### Build Steps

1. Implement leaf page operations (search, insert, split).
2. Implement internal page operations (search, insert, split).
3. Implement tree traversal (root to leaf).
4. Implement page splits and merges.
5. Implement range scan iterator.
6. WAL-log all structural modifications (splits, merges).
7. Test under simulation: concurrent reads during splits, crash during split.

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

Undo log entries can be discarded once no active transaction could possibly need them (i.e., the oldest active snapshot is newer than the entry). This is simpler than PostgreSQL's VACUUM — it's a truncation of the undo log tail, not a scan of heap pages.

### Build Steps

1. Implement the undo log as an append-only buffer (in-memory first, then persisted).
2. Implement snapshot creation (capture committed transaction set).
3. Implement visibility checks.
4. Implement version chain traversal for reads.
5. Implement undo log garbage collection.
6. Test under simulation: long-running transactions, interleaved reads/writes, crash during undo application.
