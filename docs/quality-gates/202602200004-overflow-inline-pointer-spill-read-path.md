# Quality Gate Artifact: 2026-02-20-overflow-inline-pointer-spill-read-path

- Commit: `76598c4`
- Title: `Implement dedicated overflow region allocator + row pointer encoding + spill/read integration`
- Scope: `Delivers end-to-end overflow string handling: dedicated overflow page-id allocator, row inline-vs-overflow pointer encoding, mutation spill on >1024B strings, bounded read-path overflow materialization, and deterministic exhaustion/roundtrip tests.`

## PR Checklist

- What invariant was added or changed?
  - `String columns now encode an explicit storage mode per non-null value in row format v2: inline payload pointer or overflow first-page pointer.`
  - `Overflow chain page ids must be allocated from an explicit dedicated region and are fail-closed when allocator capacity is exhausted.`
  - `Read-path overflow traversal must stay within overflow region ownership and bounded hop count; out-of-region pointers or invalid page formats are treated as corruption.`

- What is the crash-consistency contract for the modified path?
  - `Unchanged from current mutation ordering: undo snapshot is pushed before heap mutation, then WAL row record append sets page LSN.`
  - `Overflow pages are written before heap row pointer publication in mutation path; full overflow WAL/recovery contract is not yet added in this increment and remains explicitly pending.`

- Which error classes can now be returned?
  - `New mutation error: OverflowRegionExhausted (classified as resource_exhausted).`
  - `Overflow decode/format/ownership violations map to corruption on read and mutation decode paths.`

- Does this change modify any persistent format or protocol?
  - Persistent format: `yes (row format version bump to v2 with explicit string slot tag + overflow pointer encoding)`
  - Protocol: `none (wire framing unchanged; query behavior now supports oversized string spill/read)`

- Which deterministic crash/fault tests were added?
  - `Deterministic storage+executor tests for:`
    - `row inline vs overflow pointer encoding decode coverage,`
    - `legacy v1 row decode compatibility,`
    - `insert spill + scan materialization roundtrip,`
    - `update spill + scan materialization roundtrip,`
    - `overflow region exhaustion fail-closed behavior.`

- Which performance baseline or threshold was updated (if any)?
  - `String inline threshold enforced at 1024 bytes; read-path materialization remains bounded by configured query string arena bytes.`
