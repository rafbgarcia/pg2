# Releases

This directory is the source of truth for release-scoped planning and delivery.

## Layout

- `docs/releases/<x.y.z>.md`
  - Version scope, done definition, decisions, task tracking, and session handoff.
- `docs/releases/<x.y.z>/gates/*.md`
  - Per-increment quality-gate artifacts linked to that release.
- `docs/releases/GATE_TEMPLATE.md`
  - Template for all gate artifacts.

## Rules

1. Every active increment must map to exactly one release file.
2. Core DB increments must add/update exactly one gate artifact under that release's `gates/` directory.
3. Release files track progress and remain current throughout implementation.
4. `Commit:` fields in gate artifacts must be real SHAs before session close.

## Hygiene Check

```bash
rg -n 'Commit: .*T[B]D|Commit: <pending>|<sha>|<answer>|<none or details>' docs/releases
```

Expected matches should only appear in `docs/releases/GATE_TEMPLATE.md`.
