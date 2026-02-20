# Quality Gate Artifact: 202602201932-parser-modularization

- Artifact ID: `202602201932-parser-modularization`
- Commit: `pending`
- Release: `0.2.0`
- Title: `Parser modularization and test extraction`
- Scope: `Refactor parser internals into domain-focused modules and move parser scenarios into a dedicated test file without changing grammar behavior.`

## PR Checklist

- What invariant was added or changed?
  - No query-surface or syntax behavior changes were introduced by this increment.
  - Parser code organization now separates responsibilities:
    - `parser.zig` orchestrates statements, pipelines, selections, and diagnostics.
    - `parser_ops.zig` owns pipeline operator parsing.
    - `parser_schema.zig` owns schema declaration parsing.
    - `parser_shared.zig` owns shared parser error/result types.
  - Parser scenario tests moved from `parser.zig` to `parser_test.zig`.
  - Test discovery remains wired through `src/pg2.zig`.

- What is the crash-consistency contract for the modified path?
  - No crash-consistency behavior change in this increment.
  - Scope is parser module structure and test organization only.

- Which error classes can now be returned?
  - No new error classes introduced.
  - Existing parse error surface remains unchanged.

- Does this change modify any persistent format or protocol?
  - Persistent format: `none`
  - Protocol: `none`

- Which deterministic crash/fault tests were added?
  - None.
  - Existing parser deterministic tests were preserved and relocated.

- Which performance baseline or threshold was updated (if any)?
  - `none` (maintainability refactor increment).
