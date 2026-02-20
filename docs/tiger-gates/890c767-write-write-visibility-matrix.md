# Tiger Gate Artifact: 890c767

- Commit: `890c767`
- Title: `sim: add write-write visibility matrix and mark Gate 5 complete`
- Scope: Add deterministic simulation scenario for same-row write-write
  interleaving visibility and update readiness documentation for Gate 5.

## PR Checklist

- What invariant was added or changed?
  - Same-row writer interleavings must resolve to deterministic
    snapshot-visible versions across pre/between/post snapshots.
  - Replay determinism requirement for this scenario is explicit.

- What is the crash-consistency contract for the modified path?
  - Contract unchanged. This commit adds simulator coverage and documentation
    of existing crash/recovery guarantees.

- Which error classes can now be returned?
  - None new.

- Does this change modify any persistent format or protocol?
  - Persistent format: none.
  - Protocol: none.

- Which deterministic crash/fault tests were added?
  - `src/simulator/fault_matrix.zig:1281`
    `test "seeded schedule: write-write interleaving visibility remains replay-deterministic"`
  - Invariant documentation updated in `docs/SIMULATION_TESTING.md:168`.

- Which performance baseline or threshold was updated (if any)?
  - None.
