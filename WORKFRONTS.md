# pg2 Workfronts

This file is the top-level index for multi-session delivery work.  
Each workfront has phased gates so fresh Codex sessions can resume safely.

## Active Workfronts
1. `docs/workfronts/01_server_concurrency_workfront.md`
2. `docs/workfronts/02_self_tune_memory_workfront.md`
3. `docs/workfronts/03_degrade_spill_workfront.md`
4. `docs/workfronts/04_advisor_observability_workfront.md`
5. `docs/workfronts/05_test_matrix_workfront.md`
6. `docs/workfronts/06_variables_and_multi_statement_workfront.md`
7. `docs/workfronts/07_adaptive_planning_workfront.md`

## Recommended Execution Order
1. Server concurrency foundation.
2. Self-tune planner and admission semantics.
3. Spill/degrade execution model.
4. Variables and multi-statement execution semantics.
5. Adaptive planning policy and inspect explainability.
6. Advisor/observability module.
7. Cross-profile and stress test matrix.

## Cross-Workfront Rules
1. Core code continues to use `Storage`/`Network` abstractions.
2. New user-facing behavior must be deterministic under simulation.
3. Any phase introducing new defaults must include migration-safe tests.
4. Do not mark a phase complete without passing its gate criteria.
