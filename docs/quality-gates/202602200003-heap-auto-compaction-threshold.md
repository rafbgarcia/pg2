# Quality Gate Artifact: 2026-02-20-heap-auto-compaction-threshold

- Artifact ID: `202602200003-heap-auto-compaction-threshold`
- Commit: `7f4239d208b34b855ece379f95c03618d306ec5d`
- Title: `Auto-compact heap pages on insert/update contiguous-space shortfall`
- Scope: `Adds deterministic in-page compaction primitives and threshold-triggered compact+retry behavior for heap insert and row-growth update paths.`

## PR Checklist

- What invariant was added or changed?
  - `Heap insert/update now guarantee: if contiguous free space is insufficient, but reclaimable fragmented bytes can satisfy the operation shortfall, the page is compacted and the operation is retried within the same page while preserving slot identity.`

- What is the crash-consistency contract for the modified path?
  - `No ordering change in mutation durability contract: undo old row is still pushed before heap mutation, WAL update record is appended after heap mutation, and page LSN is advanced from WAL append. Compaction executes as part of in-memory page mutation before WAL append in the same update path contract.`

- Which error classes can now be returned?
  - `No new public error classes. Existing PageFull/resource_exhausted remains the terminal outcome when shortfall exceeds reclaimable fragmentation or absolute page capacity.`

- Does this change modify any persistent format or protocol?
  - Persistent format: `none (heap page format version unchanged; compaction only rewrites row placement and header free_end values within same format)`
  - Protocol: `none`

- Which deterministic crash/fault tests were added?
  - `Added deterministic heap functional regressions in src/storage/heap.zig:`
  - `test "update auto-compacts when fragmented bytes cover growth shortfall"`
  - `test "insert auto-compacts when fragmented bytes cover insert shortfall"`

- Which performance baseline or threshold was updated (if any)?
  - `none (threshold behavior introduced in code path; no benchmark gate update in this increment)`

