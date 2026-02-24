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
8. `docs/workfronts/08_expression_language_v1_workfront.md`
9. `docs/workfronts/09_module_decomposition_workfront.md`
10. `docs/workfronts/10_iterator_execution_model_workfront.md`
11. `docs/workfronts/11_write_performance_workfront.md`
12. `docs/workfronts/12_storage_reclamation_workfront.md`
13. `docs/workfronts/13_nested_spill_hash_join_workfront.md`
14. `docs/workfronts/14_runtime_storage_backend_workfront.md`

## Recommended Execution Order
1. Server concurrency foundation.
2. Self-tune planner and admission semantics.
3. Spill/degrade execution model.
4. **Write performance — PK indexing, WAL batching, index-backed constraints, bulk insert, and B+ tree cursor (no dependencies, high impact on test/dev velocity). Phases 1-3 complete; Phase 4 (index-backed constraint enforcement), Phase 5 (bulk insert path), and Phase 6 (B+ tree bulk insert cursor) remain.**
5. Variables and multi-statement execution semantics.
6. Adaptive planning policy and inspect explainability.
7. Advisor/observability module.
8. Cross-profile and stress test matrix.
9. Expression language v1 readiness.
10. Module decomposition (can run in parallel with any workfront).
11. Iterator execution model (deferred — needed when subqueries, CTEs, or window functions are prioritized).
12. Storage reclamation without VACUUM (after write performance and iterator execution model — depends on B+ tree index maintenance patterns from WF11).
13. Nested selection performance-first hash join spill path (after WF03 foundations; can run in parallel with WF11/WF12 once row-set contract is stable).
14. Runtime storage backend and memory-accounting hard boundaries (can start once WF01 transport/runtime boot paths are stable).

## Cross-Workfront Rules
1. Core code continues to use `Storage`/`Network` abstractions.
2. New user-facing behavior must be deterministic under simulation.
3. Any phase introducing new defaults must include migration-safe tests.
4. Do not mark a phase complete without passing its gate criteria.
