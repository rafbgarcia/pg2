# Quality Gates

Purpose: keep pg2 safe and shippable without growing process docs forever.

This file is the stable policy. It should stay short.
Change-specific detail belongs in `docs/quality-gates/*.md` artifacts.

## Scope

A Quality Gate artifact is required for any increment that touches core DB code:

- `src/storage`
- `src/mvcc`
- `src/executor`
- `src/parser`
- `src/server`
- `src/replication`
- `src/catalog`

## Non-Negotiable Rules

1. Fail closed on ambiguity or corruption.
2. Persistent bytes are a compatibility contract (versioned, validated).
3. WAL/durability ordering must be explicit and tested.
4. Recovery behavior must be deterministic and idempotent.
5. Memory/runtime behavior must remain bounded.
6. Public errors must map to stable machine-actionable classes.

## Minimal PR Gate (Required)

Every core-code increment must answer these six questions in its artifact:

1. Invariant change: what invariant changed?
2. Crash contract: what is the durability/crash-consistency contract now?
3. Error taxonomy: which error classes changed?
4. Compatibility: did persistent format/protocol change?
5. Deterministic tests: which crash/fault/replay tests were added or updated?
6. Performance: any baseline/threshold change (or explicit `none`)?

If any answer is `none`, explain why.

## Evidence Expectations

Use the smallest evidence set that proves safety:

- Invariant and crash contract: brief statement + file references.
- Error taxonomy: explicit mapping impact (`retryable`, `resource_exhausted`, `corruption`, `fatal`).
- Compatibility: explicit `none` or exact format/protocol version impact.
- Determinism: test names + deterministic seed/bounded harness details.
- Performance: metric + threshold change, or explicit no-impact rationale.

## Artifact Hygiene

- One artifact per gate-changing increment/commit.
- Filename: `YYYYMMDDHHMM-<short-slug>.md`.
- `Commit:` must be a real SHA (never `TBD`/`<pending>` after session close).
- Artifact must include concrete file references for tests/evidence.

## Keep This Sustainable

To avoid process-doc sprawl:

- Keep this file policy-only and under ~2 pages.
- Put evolving detail in artifacts, not here.
- Remove duplicated checks; keep one obvious gate path.
- Prefer automation for repetitive checks.

## CI/Local Checks (Recommended)

Run these before closing a gate-changing increment:

```bash
zig build test
rg -n 'Commit: .*TBD|Commit: <pending>|<sha>|<answer>|<none or details>' docs/quality-gates
```

If matches exist outside `docs/quality-gates/TEMPLATE.md`, fix them before finalizing.
