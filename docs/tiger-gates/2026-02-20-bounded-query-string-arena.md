# Tiger Gate Artifact: 2026-02-20-bounded-query-string-arena

- Commit: `<pending>`
- Title: `Materialize query string values in bounded per-query arena`
- Scope: `Adds deterministic bounded string materialization buffers to runtime query slots and threads them through scan/executor read paths to avoid dangling string slices after page unpin.`

## PR Checklist

- What invariant was added or changed?
  - `All string values in scan/query result rows are copied into bounded arena-owned storage before page unpin; result rows no longer borrow heap-page string slices.`

- What is the crash-consistency contract for the modified path?
  - `No persistence ordering change; this increment only changes in-memory query materialization behavior and memory budgeting.`

- Which error classes can now be returned?
  - `Read paths may now deterministically return OutOfMemory/resource_exhausted when per-query string arena capacity is exceeded.`

- Does this change modify any persistent format or protocol?
  - Persistent format: `none`
  - Protocol: `none (error classification may surface resource_exhausted for oversized materialized result sets)`

- Which deterministic crash/fault tests were added?
  - `No new simulator crash-matrix tests in this increment (memory-only change).`
  - `Behavior validated by existing full deterministic test suite with new arena-backed scan paths.`

- Which performance baseline or threshold was updated (if any)?
  - `Introduced explicit bounded query-string arena threshold in runtime config (default 4 MiB per query slot).`

