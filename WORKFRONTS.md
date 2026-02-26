# pg2 Workfronts

This file is the top-level index for multi-session delivery work.
Each workfront has phased gates so fresh Codex sessions can resume safely.

## Strict Sequential Lane (One Workfront at a Time)

Suggested implementation order.

| Order | Workfront                                                                       | Why This Position                                                                                       |
| ----- | ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| 10    | `docs/workfronts/08_expression_language_v1_workfront.md`                        | Expression work is mostly parity/diagnostic closeout and is safer after core runtime contracts settle.  |
| 11    | `docs/workfronts/05_test_matrix_workfront.md`                                   | Cross-profile reliability matrix should validate the consolidated behavior from prior workfronts.       |
| 14    | `docs/workfronts/10_iterator_execution_model_workfront.md` (deferred)           | Structural executor rewrite; only start when subqueries/CTEs/window functions become active priorities. |
| 15    | `docs/workfronts/15_module_decomposition_drift_workfront.md` (maintenance-only) | Tracks post-completion drift from decomposition gates without re-opening original extraction history.   |

## 1.0.0 Release Hardening (Post-Sequence)

Complete this section after the strict sequential lane before tagging `v1.0.0`.

| Item                                  | Why It Is Required For 1.0.0                                                                                                    |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Crash/Recovery Proof                  | Proves no silent corruption or data loss across kill/restart and WAL replay boundaries.                                         |
| Backup/Restore Tooling                | Ensures you can recover from operator mistakes and host failures in real usage.                                                 |
| On-Disk Format Versioning             | Prevents unsafe boot on incompatible data formats and enables explicit upgrade policy.                                          |
| Operational Safety Contracts          | Makes failure modes explicit: deterministic errors, limits, and fail-closed boundaries.                                         |
| Soak + Performance Baseline           | Validates long-running stability (memory/storage growth, reclaim behavior, latency envelopes).                                  |
| Security Baseline (Context-Dependent) | Local single-user only: minimal auth may be optional. Any shared/remote access: require auth + role separation before `v1.0.0`. |
