# Tiger Gate Artifact: 2026-02-20-row-growth-update-crud-fix

- Commit: `<pending>`
- Title: `Support row-growth updates in heap pages and lock with server E2E CRUD test`
- Scope: `Extends heap update behavior to relocate rows within a page when payload grows, and adds a session-path E2E regression for Alice -> Alicia update.`

## PR Checklist

- What invariant was added or changed?
  - `Heap update no longer requires new row length <= old slot length. If contiguous free space exists, update relocates row bytes and repoints the slot while preserving slot identity.`

- What is the crash-consistency contract for the modified path?
  - `Unchanged ordering contract: old row is pushed to undo log before heap mutation; WAL update record is appended after the page mutation and page LSN is advanced. Recovery still replays WAL with undo visibility rules.`

- Which error classes can now be returned?
  - `Update growth path can now return page-capacity exhaustion (PageFull/resource_exhausted) when contiguous free space is insufficient, instead of always returning RowTooLarge for growth attempts.`

- Does this change modify any persistent format or protocol?
  - Persistent format: `none (heap page format version unchanged)`
  - Protocol: `none (same request/response framing; success for previously failing growth case)`

- Which deterministic crash/fault tests were added?
  - `None (no new simulator crash matrix case in this increment).`
  - `Added deterministic functional regressions in src/storage/heap.zig and src/server/e2e/update.zig.`

- Which performance baseline or threshold was updated (if any)?
  - `none`
