# pg2 User-Facing Docs

This directory is the user-facing baseline for current behavior.

Scope:
- What users can run today through the server session path.
- What is explicitly unsupported or not yet release-gated.
- How responses and errors are shaped.

Current baseline date: `2026-02-20`.

Docs:
- `user-facing-docs/QUERY_SURFACE.md`
- `user-facing-docs/ERRORS_AND_RESPONSES.md`
- `user-facing-docs/OPERATIONS_QUICKSTART.md`

Update rule:
- If a change modifies user-visible syntax, semantics, supported feature status, errors, or operational commands, update at least one file in this directory in the same commit.
