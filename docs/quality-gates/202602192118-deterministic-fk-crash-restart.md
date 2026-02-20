# Quality Gate Artifact: 560dd3b

- Commit: `560dd3b`
- Title: `Add deterministic FK crash/restart simulator coverage`
- Scope: Add deterministic crash/restart simulator matrix for FK RI actions.

## PR Checklist

- What invariant was added or changed?
  - FK RI behavior for restrict/cascade remains deterministic across crash and
    restart replay.
  - Replay with same seed must produce identical FK visibility/outcome.

- What is the crash-consistency contract for the modified path?
  - No new durability mechanism; simulator asserts existing recovery contract
    under FK mutation interleavings.

- Which error classes can now be returned?
  - None new. This commit adds deterministic verification coverage.

- Does this change modify any persistent format or protocol?
  - Persistent format: none.
  - Protocol: none.

- Which deterministic crash/fault tests were added?
  - `src/simulator/fk_fault_matrix.zig:607`
    `test "seeded schedule: FK restrict delete remains rejected across crash and restart deterministically"`
  - `src/simulator/fk_fault_matrix.zig:615`
    `test "seeded schedule: FK cascade delete remains deterministic across crash and restart"`

- Which performance baseline or threshold was updated (if any)?
  - None.
