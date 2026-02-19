# Schema Evolution (Declarative, Always-Online)

This document defines what pg2 will do when your declared schema changes.

It is a user-facing behavior contract, not an internal implementation spec.

## Goal

You declare the target schema in your project (for example `db/schema.pg2`).
pg2 computes and applies the required changes safely, idempotently, and online.

Application code should not need handwritten migration scripts for common evolution paths.

## Mental Model

Schema evolution has two phases:

1. `pg2 generate` (development/planning)
- Reads your current desired schema and prior metadata.
- Computes a semantic diff.
- Resolves unambiguous changes automatically.
- For ambiguous/destructive changes, asks for explicit intent.
- Writes a plan artifact to commit with code.

2. `pg2 apply` (deployment/execution)
- Reads desired schema + plan artifact.
- Compares with current catalog state.
- Applies required steps idempotently.
- Runs long operations in background when needed.
- Exposes status/progress/errors through catalog introspection.

## What pg2 Guarantees

- Online-first: schema evolution does not require full database downtime.
- Idempotent apply: running `pg2 apply` repeatedly is safe.
- Fail-closed on ambiguity: pg2 never guesses destructive intent.
- Transparent progress: users can see pending/running/failed/completed work.
- Stable behavior under retries: interrupted work can resume.
- Compatibility window: query execution honors transition state until cutover.

## Operation Classes

Every schema change is classified into one of these classes:

1. Instant metadata change
- Applied immediately in a short catalog transaction.
- No data rewrite.

2. Online background build/backfill
- Queued and executed with bounded resource usage.
- Existing reads/writes continue.
- New capability becomes planner-visible only when ready.

3. Expand/contract transition
- New and old representations coexist during migration.
- pg2 performs validation and only then final cutover.

4. Manual intent required
- pg2 cannot infer semantics safely.
- `pg2 generate` blocks and asks for explicit mapping or override.

## User-Visible States

A schema change unit may appear as:

- `planned`: generated but not applied.
- `queued`: accepted by `pg2 apply`, waiting for worker.
- `running`: currently building/backfilling/validating.
- `ready_for_cutover`: background work finished; cutover pending.
- `completed`: fully active and used by planner.
- `failed_retryable`: transient failure; safe to retry.
- `failed_manual`: needs explicit user action.
- `canceled`: superseded by newer desired schema.

## Scenario Matrix

### 0) No-op and drift scenarios

#### No schema diff

- Situation: desired schema and active catalog already match.
- Behavior: `pg2 generate` emits no new plan steps; `pg2 apply` exits successfully with no changes.

#### Plan artifact unchanged but cluster mid-migration

- Situation: same desired schema, prior apply was interrupted.
- Behavior: `pg2 apply` resumes incomplete units from durable state.

#### Out-of-band catalog drift detected

- Situation: live catalog differs from expected lineage recorded by plan metadata.
- Behavior: apply fails closed with drift diagnostics and requires re-generation against current state.

### 1) Adding schema objects

#### Add table

- Behavior: create catalog entry and storage metadata.
- Availability: immediate for new queries after apply commit.
- Background work: none.
- Failure mode: explicit error if limits are exceeded (for example max tables).

#### Add nullable column (no default)

- Behavior: instant metadata addition.
- Existing rows: read as `null`.
- Write path: can start writing values immediately.
- Background work: none.

#### Add nullable column (with default)

- Behavior: metadata default is registered immediately.
- Existing rows: default may be materialized lazily or during backfill depending on policy.
- Planner visibility: immediate for query semantics.
- Background work: optional low-priority backfill.

#### Add notNull column

- Behavior: requires one of:
  - constant default with safe backfill path, or
  - explicit staged plan (add nullable -> backfill -> enforce notNull).
- If neither exists: `generate` fails closed and requests intent.

#### Add index (non-unique)

- Behavior: index declared immediately, built in background.
- Query usage: planner does not use until state is `completed`.
- Writes during build: captured so final index is consistent.
- Failure: index remains unavailable; base table behavior unchanged.

#### Add unique index

- Behavior: same as non-unique plus uniqueness validation.
- If duplicates exist: build transitions to `failed_manual` with duplicate diagnostics.
- Until completed: uniqueness not enforced by the new index.

#### Add reference with `withoutReferentialIntegrity`

- Behavior: metadata-only relation for traversal/planning.
- Background work: none.
- Data validation: none.

#### Add reference with `withReferentialIntegrity(...)`

- Behavior: relation metadata plus online validation scan.
- Existing violations: transition to `failed_manual` with sample violations.
- Enforcement activation: only after validation succeeds.

### 2) Updating schema objects

#### Rename table (unambiguous)

- Behavior: metadata rename preserving stable object identity.
- Data rewrite: none.
- Existing data/indexes: retained.
- If ambiguous: requires explicit mapping.

#### Rename column (unambiguous)

- Behavior: alias/name remap on stable column identity.
- Data rewrite: none.
- Index/reference bindings: preserved by identity.
- Query compatibility: old name may be supported during transition window if configured.

#### Rename column (ambiguous)

Example: one column removed and multiple added (`name` -> `first_name`, `last_name`).

- Behavior: pg2 does not guess.
- `generate` asks for mapping strategy:
  - rename one-to-one, or
  - split transform expression/backfill policy, or
  - manual migration intent.
- Without explicit mapping: plan generation fails.

#### Change column type (widening, compatible)

- Behavior: classified as online transform if representation change is needed.
- Reads/writes: continue through transition adapter.
- Completion: cutover after validation.

#### Change column type (narrowing or potentially lossy)

- Behavior: manual intent required with explicit policy for invalid rows.
- Default: fail closed.

#### Add/remove `nullable` / `notNull`

- `nullable` -> `notNull`:
  - validation scan required.
  - fails if null rows exist unless explicit remediation is provided.
- `notNull` -> `nullable`:
  - instant metadata change.

#### Add/remove default

- Behavior: metadata change.
- Existing stored rows: unchanged.
- New writes: use new default rules immediately after commit.

#### Change reference action (e.g. `onDeleteRestrict` -> `onDeleteCascade`)

- Behavior: metadata/policy update.
- Preconditions: may require validation that transition is safe.
- If unsafe/ambiguous: manual intent required.

#### Change index definition

- Behavior: treated as create-new-index + switchover + retire-old-index.
- Planner: old index remains eligible until new one completes.
- Cutover: atomic planner metadata update.

### 3) Removing schema objects

#### Drop index

- Behavior: planner stops using index after metadata change.
- Physical reclaim: may run asynchronously.
- Safety: idempotent if already absent.

#### Drop column

- Behavior: two-phase by default.
  - phase 1: mark deprecated / deny new references (optional grace window).
  - phase 2: physical reclaim asynchronously when safe.
- If column participates in active constraints/indexes/references:
  - either blocked with diagnostics or auto-plan dependent removals only when explicitly requested.

#### Drop table

- Behavior: metadata tombstone + asynchronous physical cleanup.
- Safeguard: may require explicit confirmation policy in production profiles.
- Dependents: blocked unless explicit cascade intent is present.

#### Drop reference/constraint

- Behavior: enforcement disabled at cutover point.
- Existing data: unchanged.
- Cleanup: metadata and dependent structures handled idempotently.

### 4) Data-shape migrations (expand/contract)

#### Split one column into many

Example: `name` -> `first_name`, `last_name`.

- Requires explicit transform intent.
- pg2 executes:
  - add target columns,
  - backfill in background,
  - dual-read/dual-write behavior as configured,
  - validate completeness,
  - cutover query bindings,
  - retire source column when requested.
- Application expectation: both old and new shapes may coexist during transition.

#### Merge many columns into one

- Same expand/contract semantics.
- Requires explicit conflict/null handling policy.

#### Computed replacement

- If replacement is deterministic and declared, pg2 can backfill online.
- If expression is non-deterministic or depends on external state, manual strategy is required.

### 4.1) Custom query backfills

When built-in rename/split/merge semantics are not enough, pg2 supports a declarative custom backfill unit.

- Purpose: populate or transform data using a query-defined mapping.
- Execution: online, low-priority, checkpointed background job.
- Idempotency: target writes must be keyed so retries do not duplicate logical rows.
- Cutover: only after validation succeeds.

#### Required declarations

- source snapshot or source model/query
- target model
- row identity/upsert key for idempotent writes
- transform expression/query
- invalid-row policy (`fail`, `skip`, or `quarantine`)
- cutover condition/validation rule

If any required piece is missing, `pg2 generate` fails closed.

#### Runtime behavior

- bulk phase: reads source snapshot in bounded batches and writes target rows.
- catch-up phase: applies changes that happened during bulk phase.
- validation phase: checks row counts/invariants/user-declared predicates.
- cutover phase: switches bindings/planner visibility atomically.

If the process restarts, pg2 resumes from durable checkpoints.

### 4.2) Custom backfill examples

The syntax below is illustrative and may evolve.

#### Example A: New table backfilled from an existing table

```pg2
backfill(user_profiles_from_users,
  source(User),
  target(UserProfile),
  key([user_id]),
  query(
    User {
      user_id: id
      display_name: name
      created_at
    }
  ),
  writeMode(upsert),
  invalidRows(quarantine),
  validate(row_count_match(User.id, UserProfile.user_id)),
  cutover(enable_model(UserProfile))
)
```

Expected behavior:

- `UserProfile` can be declared and queried immediately.
- planner does not depend on incomplete backfill artifacts.
- backfill runs online; retries are safe due to `key([user_id]) + upsert`.
- invalid rows are quarantined and reported.
- cutover happens only after validation passes.

#### Example B: Split `name` into `first_name`/`last_name`

```pg2
backfill(user_name_split,
  source(User),
  target(User),
  key([id]),
  query(
    User {
      id
      first_name: split_part(name, " ", 1)
      last_name: split_part(name, " ", 2)
    }
  ),
  writeMode(update),
  invalidRows(fail),
  validate(no_nulls(User.first_name)),
  cutover(bind_field_alias(name -> first_name))
)
```

Expected behavior:

- old and new shape can coexist during transition.
- if transformation cannot parse a row, job moves to `failed_manual` (`invalidRows(fail)`).
- after validation, alias/cutover is applied atomically.
- source field can be retired in a later explicit step.

### 5) Concurrency and deploy races

#### `pg2 apply` runs while app is live

- Behavior: supported.
- Ongoing queries: run against a consistent catalog snapshot.
- New queries: see post-commit metadata when catalog version advances.

#### Two deploys run `pg2 apply` concurrently

- Behavior: one coordinator wins; others observe and converge.
- Result: no duplicate work; idempotent final state.

#### Desired schema changes again before prior background work finishes

- Behavior: pg2 re-plans from current actual state.
- In-flight units become:
  - continued if still relevant, or
  - canceled/superseded if obsolete.
- User visibility: superseded units remain queryable in history.

### 6) Failure and recovery behavior

#### Process restart during migration

- Behavior: migration state is durable.
- On restart: resume from last durable checkpoint.
- Guarantee: no partial cutover exposure.

#### Crash during cutover

- Behavior: cutover is atomic from user perspective.
- After recovery: old or new version is active, never half-applied visibility.

#### Transient resource failures (disk/network/backpressure)

- Behavior: transition to `failed_retryable` with bounded retries.
- Operator action: rerun `pg2 apply` or let policy retry.

#### Permanent semantic failure (duplicate keys, invalid transform)

- Behavior: `failed_manual`.
- pg2 keeps serving prior safe behavior.
- User action: update schema/plan intent and re-apply.

### 7) Idempotency contract

Running `pg2 apply` multiple times with the same desired schema must:

- not duplicate objects,
- not re-run completed steps unnecessarily,
- resume incomplete background work,
- preserve successful prior cutovers,
- return success once converged.

### 8) Introspection contract

Users can inspect:

- desired schema version known to pg2,
- currently active catalog version,
- pending/running/completed/failed migration units,
- progress metrics for long-running units,
- blocking reasons for manual intervention,
- history of applied/superseded plans.

This data is always-on and queryable via pg2 catalog/introspection interfaces.

### 9) Ambiguity policy (fail closed)

pg2 will require explicit intent for at least these cases:

- remove + add where rename is not provably one-to-one,
- lossy type conversions,
- destructive drops with dependents,
- uniqueness enforcement when existing data may violate constraints,
- transform steps that need domain-specific parsing/logic.

If intent is missing, `pg2 generate` fails with concrete options.

### 10) What pg2 does not promise

- Instant completion for large backfills/index builds.
- Automatic guessing of business semantics.
- Silent destructive behavior.
- Planner use of not-yet-ready physical structures.

## Suggested Team Workflow

1. Edit `db/schema.pg2`.
2. Run `pg2 generate` and resolve any ambiguity prompts.
3. Commit schema + generated plan artifact.
4. Deploy.
5. Run `pg2 apply` (or let deploy hook run it).
6. Monitor migration status via introspection until converged.

## Status

This document describes the intended behavior contract for pg2 declarative schema evolution.
As implementation lands, each scenario will be marked as supported/partial/planned in release notes.
