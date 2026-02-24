# Workfront 15: Module Decomposition Drift

## Objective
Capture and resolve drift that accumulated after Workfront 09 was completed, without rewriting the original extraction history.

## Why This Exists
Workfront 09 remains complete as historical extraction work. Current HEAD has coupling/structure drift that no longer satisfies all original decomposition gates.

## Audit Snapshot (2026-02-25)
- Test gate: `zig build test --summary all` passes (`235/235`).
- Structural gate drift exists in module wiring and import boundaries.

### Confirmed Drift
1. Acyclic dependency graph gate is violated by multiple parent/child cross-import cycles.
2. "Extracted module imported only by parent module" gate is violated in several phases.
3. Some post-extraction size targets no longer match current file sizes.

## Scope
1. Re-establish acyclic module boundaries where practical.
2. Re-align extracted module import boundaries with clear ownership rules.
3. Keep behavior unchanged; this is a refactor/hygiene workfront only.
4. Refresh size/ownership expectations to match current architecture where needed.

## Non-Negotiables
1. No behavioral changes.
2. `zig build test --summary all` must pass at every phase gate.
3. Do not modify `test/features/` expectations unless a separate workfront explicitly requires it.
4. Keep dependency direction explicit and documented.

## Suggested Phases
### Phase 1: Dependency Audit + Boundary Contract
- Produce a module dependency map for executor/storage/server decomposition modules.
- Define allowed import directions for each extracted module.

### Phase 2: Cycle Removal
- Remove parent/child cross-import cycles by introducing narrow shared contracts/types in neutral modules.

### Phase 3: Import Boundary Cleanup
- Enforce "single owning importer" where required, or explicitly document sanctioned exceptions.

### Phase 4: Size/Responsibility Re-baseline
- Update decomposition target sizes/responsibilities to reflect current design reality.

### Phase 5: Documentation + Guardrails
- Add lightweight lint/check guidance (manual or scripted) for dependency direction and ownership boundaries.

## Exit Criteria
1. No critical cycles remain in decomposition-targeted modules.
2. Import ownership rules are either satisfied or explicitly documented with rationale.
3. `zig build test --summary all` remains green.
4. Workfront 09 stays unchanged as historical completion record.
