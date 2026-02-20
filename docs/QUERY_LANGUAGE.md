# pg2 Query Language

This document defines the current pg2 query-language design.

## Core Principles

1. You declare intent. pg2 chooses execution strategy.
2. One obvious way for common tasks.
3. Selection sets define shape; pipelines define transformation.
4. Schema metadata drives relation traversal and planning.
5. Bounded execution: queries fail explicitly when limits are exceeded.

## Conventions

- Model names: `PascalCase` (`User`, `OrderItem`)
- Field names: `snake_case` (`user_id`, `created_at`)
- Keywords/operators: lowercase (`where`, `sort`, `group`)
- Referential-action constants: camel case (`onDeleteRestrict`)

---

## Schema

Schema is declarative. A model contains field declarations plus optional metadata declarations.

```pg2
User {
  field(id, bigint, notNull, primaryKey)
  field(email, string, notNull)
  field(active, boolean, notNull)

  reference(posts, id, Post.user_id, withoutReferentialIntegrity)

  scope(active, where(active = true))
  index(idx_email, [email], unique)
}

Post {
  field(id, bigint, notNull, primaryKey)
  field(user_id, bigint, notNull)
  field(title, string, notNull)

  reference(author, user_id, User.id, withReferentialIntegrity(onDeleteRestrict, onUpdateCascade))

  index(idx_user_id, [user_id])
}
```

### Field Declaration

```pg2
field(name, type, nullability[, modifier...])
```

Examples:

```pg2
field(id, bigint, notNull, primaryKey)
field(name, string, nullable)
field(created_at, timestamp, notNull)
```

Supported scalar types:

- `bigint`
- `int`
- `float`
- `boolean`
- `string`
- `timestamp`

Nullability:

- `notNull`
- `nullable`

Common modifiers:

- `primaryKey`
- `default(...)` (when supported by execution path)

### References (Unified Association Model)

Use `reference(...)` for all relationships.

```pg2
reference(alias, local_field, TargetModel.target_field, policy)
```

Policy must be explicit:

- `withoutReferentialIntegrity`
- `withReferentialIntegrity(onDeleteX, onUpdateY)`

#### Referential-Integrity Actions

Delete actions:

- `onDeleteRestrict`
- `onDeleteCascade`
- `onDeleteSetNull`
- `onDeleteSetDefault`

Update actions:

- `onUpdateRestrict`
- `onUpdateCascade`
- `onUpdateSetNull`
- `onUpdateSetDefault`

Current implementation status (fail-closed):

- `onDeleteRestrict`, `onDeleteCascade`, `onDeleteSetNull` are supported.
- `onUpdateRestrict`, `onUpdateCascade`, `onUpdateSetNull` are supported.
- `onDeleteSetDefault` and `onUpdateSetDefault` are currently rejected as invalid configuration.

#### Semantics

- `withoutReferentialIntegrity`:
  - Relationship exists as query/planner metadata only.
  - No FK enforcement.
- `withReferentialIntegrity(...)`:
  - Relationship metadata plus enforced RI semantics.
  - `onDelete...` and `onUpdate...` are mandatory and explicit.

### Scope Declarations

```pg2
scope(name, pipeline_expression)
```

Example:

```pg2
scope(active, where(active = true))
scope(recent, where(created_at >= now() - 30.days) |> sort(created_at desc))
```

### Index Declarations

Index syntax is parenthesized:

```pg2
index(index_name, [field_name[, field_name...]][, unique])
```

Examples:

```pg2
index(idx_email, [email], unique)
index(idx_created_at, [created_at])
index(idx_author_created, [author_id, created_at])
```

Rules:

- Index names are unique per model (table-scoped), not global.
- `unique` marks a unique index.

---

## Queries

### Read Queries

```pg2
Model |> operator(...) |> ... { selection }
```

Examples:

```pg2
User { id email }
User |> where(active = true) |> sort(id desc) |> limit(10) { id email }
```

### Pipeline Operators

Supported read-side operators:

- `where(...)`
- `sort(...)`
- `group(...)`
- `limit(...)`
- `offset(...)`
- `unique`
- `inspect`
- scope references by name

### Inspect Output

`inspect` appends deterministic execution diagnostics after the normal query rows.

Output lines:

- `INSPECT exec ...` counters for scan/match/return and mutation/page stats.
- `INSPECT pool ...` connection-pool saturation state.
- `INSPECT overflow ...` overflow reclaim backlog depth and deterministic throughput counters.
- `INSPECT plan ...` planner/executor decision summary:
  - `source_model`: resolved root model for the pipeline.
  - `pipeline`: operator chain in execution order (`scan_only` when no operators).
  - `join_strategy`: currently `none` or `nested_loop`.
  - `join_order`: currently `none` or `source_then_nested`.
  - `materialization`: currently `none` or `bounded_row_buffers`.
  - `sort_strategy`: currently `none` or `in_place_insertion`.
  - `group_strategy`: currently `none` or `in_memory_linear`.
  - `nested_relations`: number of nested relation joins executed.
- `INSPECT explain ...` plain-language wording for sort/group physical decisions.

Example:

```text
OK rows=0
INSPECT exec rows_scanned=0 rows_matched=0 rows_returned=0 rows_inserted=0 rows_updated=0 rows_deleted=0 pages_read=0 pages_written=0
INSPECT pool policy=reject size=1 checked_out=1 pinned=0 exhausted_total=0
INSPECT overflow reclaim_queue_depth=0 reclaim_enqueued_total=0 reclaim_dequeued_total=0 reclaim_chains_total=0 reclaim_pages_total=0 reclaim_failures_total=0
INSPECT plan source_model=User pipeline=where>sort>inspect join_strategy=none join_order=none materialization=none sort_strategy=in_place_insertion group_strategy=none nested_relations=0
INSPECT explain sort=rows sorted in place with insertion order swaps group=not_applied
```

### Selection Sets

Selection sets define output shape.

```pg2
User {
  id
  email
}
```

Computed fields:

```pg2
User {
  id
  lower_name: lower(name)
}
```

### Nested Relation Traversal

Relation traversal uses `reference` alias names.

```pg2
User {
  id
  posts {
    id
    title
  }
}
```

Nested pipelines are allowed:

```pg2
User {
  id
  posts |> where(title != null) |> sort(id desc) |> limit(5) {
    id
    title
  }
}
```

Multiple nested relations in one selection set are allowed:

```pg2
User {
  id
  posts { id }
  comments { id }
}
```

### Aggregation

Aggregates are expression functions:

- `count(*)`
- `sum(field)`
- `avg(field)`
- `min(field)`
- `max(field)`

Examples:

```pg2
Post |> group(user_id) { user_id post_count: count(*) }
Post |> group(user_id) |> where(count(*) > 1) |> sort(count(*) desc) { user_id }
```

---

## Mutations

### Insert

```pg2
User |> insert(id = 1, email = "a@x.com", active = true)
```

### Update

```pg2
User |> where(id = 1) |> update(email = "new@x.com")
```

### Delete

```pg2
User |> where(id = 1) |> delete
```

---

## Expression Features

Expressions support:

- Comparison: `=`, `!=`, `<`, `<=`, `>`, `>=`
- Boolean: `and`, `or`, `not`
- Membership: `in`, `not in`
- Arithmetic: `+`, `-`, `*`, `/`
- Built-ins: `lower`, `upper`, `trim`, `length`, `abs`, `sqrt`, `round`, `coalesce`, `now`

Examples:

```pg2
User |> where(active = true and email != null) { id }
User |> where(id in [1, 2, 3]) { id }
User { abs_id: abs(id) }
```

---

## Compatibility Notes

- Legacy association declarations (`hasMany`, `hasOne`, `belongsTo`) are considered legacy forms.
- `reference(...)` is the canonical relationship declaration going forward.
- `belongsTo` without explicit RI policy is rejected (fail-closed). Use `reference(...)` with an explicit policy.
- Parenthesized field declarations are canonical.
