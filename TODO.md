# pg2 Next Milestones

This checklist is for fresh Codex sessions to continue high-priority implementation work in order.
Release-readiness gates live in `V1_READINESS_CHECKLIST.md`.

## Next Milestone: Real-World E2E Examples (Gate 3 / Gate 8)

Scope for this milestone:
- Focus only on realistic, direct server-path E2E tests.
- Keep tests explicit and readable (no `steps` mini DSL in Zig test files).

Milestone checklist:
- [x] Add one schema bootstrap E2E test in `src/server/e2e_specs.zig`.
- [x] Add one insert E2E test in `src/server/e2e_specs.zig`.
- [x] Add one query E2E test in `src/server/e2e_specs.zig`.
- [ ] Complete full mirror of `e2e/specs/03_filter_sort_limit_offset.spec` (remaining `offset/limit` assertion path).
- [ ] Mirror `e2e/specs/02_basic_crud.spec` as direct Zig tests.
- [ ] Mirror `e2e/specs/04_group_aggregates.spec` as direct Zig tests.

Discovered behavior gaps to resolve in this milestone:
- [ ] Row growth update fails in CRUD flow: `User |> where(id = 1) |> update(name = "Alicia")` returns `ERR query: update failed; class=resource_exhausted; code=RowTooLarge`.
