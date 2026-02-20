# Tiger Gate Artifact: 810a60c

- Commit: `810a60c`
- Title: `Add deterministic inspect plan metadata`
- Scope: Extend inspect output with deterministic plan metadata
  (`source_model`, pipeline operators, join strategy/order,
  materialization mode, nested relation count).

## PR Checklist

- What invariant was added or changed?
  - `inspect` plan metadata is deterministic for a fixed AST/query shape and
    execution path.
  - Nested relation joins always record explicit physical join decisions in
    plan stats.

- What is the crash-consistency contract for the modified path?
  - None. This change only affects in-memory execution stats and response
    serialization.
  - No WAL ordering, fsync ordering, or recovery behavior changed.

- Which error classes can now be returned?
  - None new.
  - Existing query/session error classes are unchanged.

- Does this change modify any persistent format or protocol?
  - Persistent format: none.
  - Protocol: yes, inspect response now includes an additional
    `INSPECT plan ...` line in the textual session response.

- Which deterministic crash/fault tests were added?
  - None, because durability and recovery paths were not modified.
  - Deterministic behavior tests added:
    - `src/executor/executor.zig:2113`
      `test "execute captures deterministic inspect plan metadata"`
    - `src/executor/executor.zig:2213`
      `test "execute nested relation join through selection set"`
      (asserts join plan fields)
    - `src/server/session.zig:478`
      `test "session inspect appends execution and pool stats"`

- Which performance baseline or threshold was updated (if any)?
  - None. This change adds inspect metadata bookkeeping only.
