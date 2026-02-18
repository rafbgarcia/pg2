# pg2 Query Language

## Design Principles

1. **You write what. The DB handles how.** Developers express logical intent (models, filters, relations, result shape). The DB chooses physical execution (join strategy/order, dataflow, materialization) using runtime observations plus catalog stats when needed. Every automatic decision is visible in stats, and critical decisions are developer-steerable through query shape, schema metadata, and explicit control constructs.
2. **Selection sets for shape. Pipelines for transforms.** Queries combine GraphQL-like selection sets (nested relation traversal) with pipeline operators (filter, sort, group). Selection sets define *what to return*. Pipelines define *how to transform*.
3. **Always-on stats.** Every query returns execution stats — rows per operator, pages read, join strategies chosen, and the reasoning behind every automatic decision. No EXPLAIN needed.
4. **Low cognitive load.** The language has a small surface area. Scopes, filters, aggregates, and relation traversal all compose with one consistent mechanism (pipelines + selection sets).
5. **One obvious way to do it.** For any given task, there is one clear way to express it. No implicit joins, no alternative syntax, no optional shorthands that fragment how queries are written.
6. **Bounded execution.** Every query operates within the fixed memory budget of its connection arena. Queries that would exceed the budget fail with an explicit error — no silent allocation or unbounded spill.

## Conventions

- Model names: PascalCase (`User`, `LineItem`)
- Field names: snake_case (`user_id`, `created_at`)
- Type names: PascalCase (`Bigint`, `String`, `Boolean`)
- Scope and index names: snake_case (`active`, `past_month`, `idx_email`)
- Keywords and operators: lowercase (`where`, `sort`, `and`, `or`, `in`)
- Commas between fields in selection sets are optional (whitespace separates)

---

## Schema

### Overview

The schema is **declarative** — it defines the desired state, not a sequence of changes. pg2 diffs the current state against the schema and handles migrations automatically with always-online operations. Developers update the schema; pg2 figures out how to get there (or returns an error for invalid transitions).

```
User {
    field(id, Bigint, notNull, primaryKey, autoIncrement)
    field(email, String, notNull)
    field(name, String, notNull)
    field(active, Boolean, notNull)
    field(bio, String, nullable)
    field(created_at, Timestamp, notNull)

    hasMany(posts, Post.author_id)

    validate(email, unique, "email is already taken")
    validate(email, format(email), "must be a valid email")

    scope(active, where(active = true))
    scope(recent, where(created_at >= now() - 30.days))

    index(idx_email, [email])
    index(idx_created_at, [created_at])
}

Post {
    field(id, Bigint, notNull, primaryKey, autoIncrement)
    field(title, String, notNull)
    field(body, String, notNull)
    field(published, Boolean, notNull)
    field(views, Bigint, notNull)
    field(created_at, Timestamp, notNull)
    field(author_id, Bigint, notNull)

    belongsTo(author, User.id)

    validate(title, notEmpty, "title cannot be empty")

    scope(published, where(published = true))
    scope(past_month, where(created_at >= now() - 30.days))

    index(idx_author_id, [author_id])
    index(idx_created_at, [created_at])
}
```

A model definition has five sections, all optional, in any order: fields, associations, validations, scopes, and indexes.

### Fields & Types

```
field(name, Type, notNull)
field(name, Type, nullable)
field(name, Type, notNull, modifier, ...)
```

Every field **must** specify `notNull` or `nullable` explicitly. There is no default — this forces a conscious decision about every column. `primaryKey` requires `notNull` (combining `nullable` with `primaryKey` is a schema error).

| Type | Size | Description |
|---|---|---|
| `Bigint` | 8 bytes | 64-bit signed integer |
| `Int` | 4 bytes | 32-bit signed integer |
| `Float` | 8 bytes | 64-bit IEEE 754 |
| `Boolean` | 1 byte | `true` / `false` |
| `String` | variable | UTF-8 text |
| `Timestamp` | 8 bytes | Microseconds since epoch |

| Modifier | Description |
|---|---|
| `primaryKey` | Designates the primary key (requires `notNull`) |
| `autoIncrement` | Monotonically increasing integer ID (assigned by the DB) |
| `tsid` | Time-sorted unique ID (recommended for distributed use) |

### Associations

```
hasOne(name, ForeignModel.foreign_key)
hasMany(name, ForeignModel.foreign_key)
belongsTo(name, ForeignModel.primary_key)
```

Associations declare relationships between models. They serve two purposes:

1. **Enable nested relation traversal** in queries — `posts { ... }`, `author { ... }`
2. **Provide cardinality metadata** to the join strategy selector — many-to-one joins don't increase row count, one-to-many joins do

Associations do **not** enforce referential integrity — no checks on insert, update, or delete. They are schema metadata.

All relation traversal in queries goes through declared associations. There is no ad-hoc join syntax — if you need to connect two models, declare the association in the schema first.

```
-- User has many posts (1:N via Post.author_id)
hasMany(posts, Post.author_id)

-- Post belongs to a user (N:1 via User.id)
belongsTo(author, User.id)

-- User has one profile (1:1 via Profile.user_id)
hasOne(profile, Profile.user_id)
```

When multiple associations connect the same pair of models, nested traversal uses the association name to disambiguate:

```
Transfer {
    field(id, Bigint, notNull, primaryKey, autoIncrement)
    field(amount, Float, notNull)
    field(from_account_id, Bigint, notNull)
    field(to_account_id, Bigint, notNull)

    belongsTo(from_account, Account.id)
    belongsTo(to_account, Account.id)
}

-- Uses the named associations
Transfer { id amount from_account { name } to_account { name } }
```

### Validations

```
validate(field, rule)
validate(field, rule, "error message")
```

Validations are domain rules that return human-readable error messages on failure. They run on `insert` and `update`. When no message is provided, the DB generates a default from the field name and rule.

| Rule | Description |
|---|---|
| `unique` | No two rows share the same value (backed by a unique index) |
| `notEmpty` | String length > 0 |
| `format(email)` | Valid email format |
| `format(url)` | Valid URL format |
| `regex(pattern)` | Matches regex pattern |
| `minLength(n)` | String min length |
| `maxLength(n)` | String max length |
| `min(n)` | Numeric min value |
| `max(n)` | Numeric max value |

```
validate(email, unique, "email is already taken")
validate(email, format(email), "must be a valid email address")
validate(name, minLength(2), "name must be at least 2 characters")
validate(bio, maxLength(500))  -- default: "bio must be at most 500 characters"
```

### Scopes

```
scope(name, pipeline_expression)
```

Scopes are named, reusable pipeline fragments defined in the model. They are used as pipeline operators with `|>`, not dot notation.

```
scope(active, where(active = true))
scope(recent, where(created_at >= now() - 30.days))
scope(popular, where(views > 1000) |> sort(views desc))
```

Usage — as pipeline operators:

```
User |> active { id email }
-- expands to: User |> where(active = true) { id email }

User |> active |> recent { id email }
-- expands to: User |> where(active = true)
--                  |> where(created_at >= now() - 30.days) { id email }
```

Scopes compose by chaining — each `|>` appends the scope's pipeline fragment. They work identically on nested relations:

```
User |> active {
    id
    email
    posts |> published |> sort(created_at desc) |> limit(5) {
        id
        title
    }
}
```

### Indexes

```
index(name, [columns...])
```

Indexes support exact-match lookups (`find`) and range scans (`between`). Dot notation on the model name accesses an index.

```
index(idx_email, [email])
index(idx_created_at, [created_at])
index(idx_author_status, [author_id, status])
```

---

## Queries

### Basic Reads

```
-- All users, specific fields
User { id email name }

-- All fields
User { * }

-- Multi-line (commas optional)
User {
    id
    email
    name
}
```

### Pipeline Operators

Pipelines transform data between the model reference and the selection set:

```
User
    |> where(active = true)
    |> sort(created_at desc)
    |> limit(10) {
        id
        email
        created_at
    }
```

### Scopes

Scopes are pipeline operators — they slot into the pipeline like any other operator:

```
User |> active { id email }
User |> active |> recent { id email created_at }

-- Scopes compose with other operators
User |> active |> where(email != null) |> sort(name asc) |> limit(20) {
    id
    email
    name
}
```

### Filtering

```
User |> where(active = true) { id email }
User |> where(age >= 18 and age <= 65) { id name }
User |> where(status = "active" or role = "admin") { id name }
User |> where(id in [1, 2, 3]) { id name }
User |> where(not active) { id email }
User |> where(email != null) { id email }
```

### Nested Relations

Associations enable nested relation traversal inside selection sets. The DB resolves join conditions automatically from the association metadata — the developer never writes join conditions.

```
User |> active {
    id
    email
    posts {
        id
        title
    }
}
```

Nested relations can have their own pipelines and scopes:

```
User |> active {
    id
    email
    posts |> published |> sort(created_at desc) |> limit(5) {
        id
        title
        created_at
    }
}
```

Deep nesting:

```
User |> active {
    id
    email
    posts |> published {
        id
        title
        comments |> sort(created_at desc) |> limit(3) {
            body
            author { name }
        }
    }
}
```

The DB picks the join strategy at runtime for each nesting level. Stats show the strategy for every level.

### Computed Fields

Use `name: expression` in selection sets for derived values:

```
User |> active {
    id
    email
    post_count: posts |> count(*)
    recent_posts: posts |> past_month |> count(*)
    avg_views: posts |> published |> avg(views)
}
```

Each computed field on a relation is executed per parent row. The engine picks the optimal strategy (correlated subquery, lateral join, or batch lookup) based on the parent row count.

### Let Bindings

`let` stores an intermediate query for reuse. Let bindings do not have selection sets — they produce queryable references.

```
let active_users = User |> active

Post |> where(author in active_users) {
    id
    title
    author { id email }
}
```

When `where(author in active_users)` uses an association name (`author`), the DB resolves the join condition from the association metadata automatically.

Let bindings compose:

```
let recent_orders = Order |> where(created_at >= now() - 7.days)
let high_value = recent_orders |> where(total > 1000)

high_value {
    id
    total
    customer { id name }
}
```

Each `let` binding can be tested independently during development:

```
let user_orders = Order.idx_user_id
    |> find(5)
    |> where(status = "shipped")

user_orders { * }  -- run just this step to see what it produces
```

The DB decides whether to stream, materialize, or tee let bindings based on reference count and estimated size. Independent bindings can execute in parallel. These decisions are always visible in stats.

### Index Access

Dot notation on the model name accesses an index. `find` does exact match, `between` does range scan:

```
User.idx_email |> find("alice@example.com") { id email name }

Order.idx_created_at
    |> between(now() - 30.days, now())
    |> where(status = "shipped")
    |> sort(total desc)
    |> limit(10) {
        id
        total
        status
    }
```

---

## Aggregation

### Aggregates in Selection Sets

Aggregate functions appear directly in the selection set. No `aggregate()` pipeline operator — the selection set is the single place where output shape is defined.

Whole-table aggregation (no grouping):

```
Order {
    total: count(*)
    revenue: sum(amount)
    avg_order: avg(amount)
}
```

When the selection set contains only aggregate functions (and no plain fields), the entire input is treated as one group.

Aggregate functions: `count(*)`, `sum(field)`, `avg(field)`, `min(field)`, `max(field)`.

### Group

`group()` partitions rows. After `group()`, aggregate functions and grouping keys are available everywhere — in `where`, `sort`, and the selection set:

```
Post |> group(author_id) {
    author { id name }
    post_count: count(*)
    avg_views: avg(views)
}
```

Filtering on aggregates (equivalent to SQL `HAVING`) — just use `where` after `group`:

```
Post
    |> group(author_id)
    |> where(count(*) > 10)
    |> sort(count(*) desc)
    |> limit(10) {
        author { id name }
        post_count: count(*)
    }
```

No separate `HAVING` keyword. The pipeline is sequential — `where` after `group` filters on aggregated values naturally.

### Inline Aggregates on Relations

Inside selection sets, relations can be aggregated per parent row:

```
User |> active {
    id
    email
    post_count: posts |> count(*)
    avg_views: posts |> published |> avg(views)
    latest_post: posts |> sort(created_at desc) |> limit(1) { title created_at }
}
```

### End-to-End Example

Top 10 authors by total post views in the last 30 days:

```
Post |> past_month |> where(published = true) |> group(author_id)
    |> sort(sum(views) desc)
    |> limit(10) {
        author { id name email }
        posts: count(*)
        total_views: sum(views)
        avg_views: avg(views)
    }
```

SQL equivalent:

```sql
SELECT u.id, u.name, u.email,
       COUNT(*) as posts,
       SUM(p.views) as total_views,
       AVG(p.views) as avg_views
FROM posts p
JOIN users u ON p.author_id = u.id
WHERE p.published = true
  AND p.created_at >= NOW() - INTERVAL '30 days'
GROUP BY u.id, u.name, u.email
ORDER BY total_views DESC
LIMIT 10;
```

---

## Mutations

### Insert

```
User |> insert(email = "alice@example.com", name = "Alice", active = true) {
    id
    email
}
```

The selection set specifies which fields to return from the inserted row.

Bulk insert:

```
User |> insert([
    (email = "alice@example.com", name = "Alice"),
    (email = "bob@example.com", name = "Bob")
]) { id email }
```

### Update

A pipeline identifies which rows to update:

```
User |> where(id = 5) |> update(email = "new@example.com") { id email }

User.idx_email
    |> find("old@example.com")
    |> update(email = "new@example.com") { id email }
```

### Delete

```
User |> where(id = 5) |> delete { id }

-- No return value
Post |> where(created_at < now() - 365.days) |> delete
```

---

## Parameters

`$name` declares a typed scalar parameter. Parameters are bound at execution time — the wire protocol separates query text from parameter values structurally (see Security).

```
let $uid: Bigint = 5
let $min_total: Float = 100.0
let $status: String = "shipped"

Order.idx_user_id
    |> find($uid)
    |> where(total > $min_total and status = $status) {
        id
        total
        created_at
    }
```

Client-side binding:

```python
conn.execute(
    "Order.idx_user_id |> find($uid) |> where(total > $min_total)",
    uid=5, min_total=100.0
)
```

Parameters enable prepared statements (parse once, execute many), injection safety (typed values, not string interpolation), and readability (`$min_total` is clearer than a bare `100.0`).

---

## Scripts

Multiple statements in sequence. Each statement is a `let` binding, a query, or a mutation. The last expression is the result returned to the client.

```
let $uid: Bigint = 42

let pending = Order.idx_user_id
    |> find($uid)
    |> where(status = "pending")

let target = pending |> sort(total desc) |> limit(1)

target |> update(status = "cancelled") { id total status }
```

Stats show all steps:

```
Stats:
  [pending]
    find          idx_user_id = 42   → 8 rows, 1 page, 0.01ms
    where         status = "pending" → 3 rows (37%), 0.01ms

  [target]
    sort          total desc         → 3 rows sorted, 0.01ms
    limit         1                  → 1 row emitted

  [mutation]
    update        status = "cancel.."→ 1 row updated, 1 page written, 0.03ms
    wal_write     1 record           → 0.02ms

  total                              → 2 pages read, 1 page written, 0.08ms
```

---

## Inspect

`inspect` is a passthrough pipeline operator that prints intermediate rows without changing the query. The database equivalent of `console.log`.

```
let pending = Order.idx_user_id
    |> find(42)
    |> inspect              -- prints all rows at this point
    |> where(status = "pending")
    |> inspect              -- prints rows after where

pending |> sort(total desc) |> limit(1) { * }
```

Output:

```
[inspect @ line 3]  8 rows:
  {id: 10, user_id: 42, total: 250.0, status: "pending"}
  {id: 11, user_id: 42, total: 50.0,  status: "shipped"}
  ...

[inspect @ line 5]  3 rows:
  {id: 10, user_id: 42, total: 250.0, status: "pending"}
  {id: 14, user_id: 42, total: 180.0, status: "pending"}
  {id: 19, user_id: 42, total: 90.0,  status: "pending"}

Result: 1 row
  {id: 10, user_id: 42, total: 250.0, status: "pending"}
```

---

## Conditional Logic

Simple `if/else` on scalar values for conditional query plans:

```
let $use_index: Boolean = true

let results = if $use_index then
    Order.idx_user_id |> find(5)
  else
    Order |> where(user_id = 5)

results |> where(total > 100) { id total }
```

This lets developers A/B test execution strategies — run both paths, compare stats, decide which to keep.

---

## Functions

### Scalar Functions

Scalar functions transform individual values inside expressions. They are pure, stateless, and evaluated per-row.

Built-in scalar functions:

| Function | Description |
|---|---|
| `now()` | Current timestamp |
| `lower(s)` | Lowercase string |
| `upper(s)` | Uppercase string |
| `trim(s)` | Remove leading/trailing whitespace |
| `length(s)` | String length in characters |
| `abs(n)` | Absolute value |
| `sqrt(n)` | Square root |
| `round(n, precision)` | Round to decimal places |
| `coalesce(a, b, ...)` | First non-null value |

Scalar functions compose in any expression context — `where`, `sort`, computed fields:

```
User |> where(lower(email) = "alice@example.com") { id email }
User |> sort(length(name) desc) { id name }
User { id, name_length: length(name) }
Order { id, total: round(subtotal * tax_rate, 2) }
```

### User-Defined Scalar Functions

`fn` defines a named scalar function. Functions are registered at startup and available in all queries.

```
fn full_name(first: String, last: String): String =
    first + " " + last

fn discount_price(price: Float, pct: Float): Float =
    round(price * (1.0 - pct), 2)
```

Usage:

```
User { id, display: full_name(first_name, last_name) }
Product |> where(discount_price(price, 0.2) < 50.0) { id name price }
```

Constraints:

- **Pure.** No I/O, no side effects, deterministic output for the same inputs.
- **No allocation.** Operates on fixed-size values. Cannot grow memory.
- **Bounded execution.** Maximum instruction count per invocation, enforced at registration.
- **Type-checked at parse time.** Argument types and return type validated against the declared signature.

The optimizer sees the expression structure around scalar functions — it can still push predicates, merge filters, and apply index lookups on the surrounding operators. The function body is opaque to the optimizer.

### Pipeline Macros

`pipe` defines a parameterized, reusable pipeline fragment. Pipeline macros generalize scopes: they work across models and accept parameters.

```
pipe recent(field, days: Bigint) = where(field >= now() - days.days)
pipe top(field, n: Bigint) = sort(field desc) |> limit(n)
pipe paginate(page: Bigint, size: Bigint) = offset((page - 1) * size) |> limit(size)
```

Untyped parameters (`field`) are field references — resolved at expansion site. Typed parameters (`days: Bigint`) are values.

Usage:

```
Post |> recent(created_at, 30) |> top(views, 10) { id title views }
User |> recent(created_at, 7) { id email }
Order |> where(status = "shipped") |> paginate(3, 20) { id total }
```

Pipeline macros expand at parse time — the optimizer sees the expanded form and applies all standard transformations (predicate pushdown, range tightening, redundant sort elimination). Zero runtime overhead.

Rules:

- **No recursion.** A macro cannot reference itself. Expansion is a single pass.
- **No closures.** Parameters are values or field references, not predicates.
- **Bounded expansion.** Nesting depth is checked at parse time.

Model-local scopes (`scope` in the schema) remain unchanged. Pipeline macros are the cross-model, parameterized counterpart.

### Extensions

Scalar functions are the primary extension point. Domain-specific packages (geometry, text search, time series) ship as sets of scalar functions registered with the catalog at startup.

```
-- A geometry extension registers:
fn st_distance(a: Geometry, b: Geometry): Float
fn st_contains(a: Geometry, b: Geometry): Boolean
fn st_point(lon: Float, lat: Float): Geometry
fn st_buffer(geom: Geometry, radius: Float): Geometry

-- Usage
Location |> where(st_distance(location, st_point($lon, $lat)) < 10.0)
         |> sort(st_distance(location, st_point($lon, $lat)) asc) {
    id
    name
    dist: st_distance(location, st_point($lon, $lat))
}
```

Extension functions follow the same constraints as user-defined scalars: pure, no allocation, bounded, type-checked. They are registered during server initialization — the catalog is sealed before accepting connections.

Why scalar functions and not new operators:

- **No new syntax.** Extensions compose through `where`, `sort`, and selection sets — existing operators the optimizer understands.
- **Optimizer transparency.** The pipeline structure around extension functions remains visible. The DB can still push predicates, choose join strategies, and apply all automatic optimizations.
- **Simulation safe.** Extension functions are deterministic. Simulation testing works unchanged.

---

## Expressions

### Operators and Precedence

From highest to lowest:

| Precedence | Operators | Kind | Example |
|---|---|---|---|
| 1 | `not` | prefix | `not published` |
| 2 | `*`, `/` | left assoc | `qty * price` |
| 3 | `+`, `-` | left assoc | `total + tax` |
| 4 | `=`, `!=`, `<`, `>`, `<=`, `>=` | non-assoc | `age >= 18` |
| 5 | `in`, `not in` | non-assoc | `id in [1, 2, 3]` |
| 6 | `and` | left assoc | `a = 1 and b = 2` |
| 7 | `or` | left assoc | `a = 1 or b = 2` |

Parentheses override precedence:

```
where((a = 1 or b = 2) and c = 3)
```

### Literals

| Type | Examples |
|---|---|
| Integer | `42`, `-1`, `0` |
| Float | `3.14`, `-0.5` |
| String | `"hello"`, `"it's"` |
| Boolean | `true`, `false` |
| Null | `null` |
| Duration | `30.days`, `1.month`, `2.hours`, `500.ms` |
| List | `[1, 2, 3]`, `["a", "b"]` |

---

## Execution Stats

Every query returns stats. Not opt-in.

### Flat Query

```
User |> where(active = true) |> sort(name asc) |> limit(10) { id email name }

Stats:
  scan          User                   → 12000 rows, 94 pages, 2.1ms
  where         active = true          → 8340 rows (69%), 0.4ms
  sort          name asc               → 8340 rows sorted, 1.8ms
  limit         10                     → 10 rows emitted
  total                                → 94 pages read (71 cached), 4.3ms
```

### Nested Query (Tree-Structured)

```
User |> active {
    id
    email
    posts |> where(published = true) |> limit(5) { id title }
}

Stats:
  scope         active                 → 8340 rows, 94 pages, 2.1ms
  ├─ posts      strategy: batch index lookup (idx_author_id)
  │             → 8340 lookups, 24891 total rows
  │  where      published = true       → 18202 rows (73%)
  │  limit      5 per parent           → capped at 41700, 31420 emitted
  total                                → 312 pages read (208 cached), 18.4ms
```

Stats are tree-structured, mirroring the selection set. Each nesting level shows its join strategy, row counts, and timing.

### Let Binding Stats

Each `let` binding gets its own stats block, including the DB's dataflow decision:

```
Stats:
  [active_users]
    scan          User                 → 12000 rows, 94 pages, 2.1ms
    where         active = true        → 8340 rows (69%), 0.4ms
    dataflow: streamed (est. 8340 × 32B = 260KB, referenced 1×)

  [result]
    where         author in [8340]     → 24891 rows, 0.8ms
    ├─ author     strategy: nested loop (index) → 24891 lookups
    total                              → 412 pages read (310 cached), 12.6ms
```

### Hints and Warnings

When the DB detects an obviously suboptimal pattern, stats include warnings:

```
Stats:
  scan          Post                   → 500000 rows, 12000 pages, 340ms
  where         author_id = 42         → 3 rows (0.0006%), 120ms
  total                                → 12000 pages read, 460ms
    ⚠ where passed 3 of 500000 rows (0.0006%) — consider an index on Post.author_id
```

Warnings are based on observed post-execution stats, not estimates.

---

## Runtime-Adaptive Join Strategy

When the DB executes a relation traversal, it picks the strategy at runtime based on **actual row counts** — not table-level estimates.

Rules, applied in order:

1. **Index exists on join key** and **left input < 1000 rows** → nested loop with index lookup.
2. **Both inputs sorted on join key** → merge join.
3. **Smaller side fits in memory** (< `query_memory_budget × 0.25`) → hash join.
4. **Neither side fits** → partitioned hash join (spill-to-disk).

Because decisions use actual row counts, selective filters are handled correctly. A scope that reduces 100K users to 12 rows will get nested loop, not hash join.

```
Stats:
  posts         strategy: nested loop (index)
                left: 12 users (after scope) → 12 lookups on idx_author_id
                → 47 rows, 0.1ms
```

```
Stats:
  posts         strategy: hash join
                left: 8340 users, right: 500000 posts
                hash table: 8340 entries (201 KB)
                → 24891 rows, 8.2ms
```

### Automatic Join Ordering

When a query traverses multiple relations, the DB determines execution order based on observed row counts and association cardinality:

1. Start from the input with the most selective filter (fewest rows — observed at runtime).
2. Traverse to the relation that produces the smallest intermediate result (many-to-one doesn't increase row count, one-to-many does).
3. Repeat for remaining relations.

```
Stats:
  join order    (runtime): Order → Customer → LineItem
                reason: Order filtered to 340 rows; Customer is N:1 (340 output);
                        LineItem is 1:N (est. 1700 via association + distinct_count)
  Order × Customer    strategy: nested loop (index) → 340 rows, 0.4ms
  result × LineItem   strategy: hash join → 1683 rows, 1.2ms
```

---

## Automatic Optimizations

These never change semantics — only physical execution. All visible in stats.

| Optimization | Description |
|---|---|
| Redundant sort elimination | Input already sorted on requested key → sort skipped |
| Limit pushdown | `limit(n)` short-circuits upstream after n rows |
| Projection pushdown | Only selection set fields are read through the pipeline |
| Dead let elimination | Unreferenced `let` binding is not executed |
| Count from catalog | `Model { total: count(*) }` with no filters returns catalog row_count (zero I/O) |
| Range tightening | `where` on indexed column after `between` folds into range bounds |
| Scope inlining | Scopes expand to pipeline operators at parse time (zero overhead) |

---

## Catalog Introspection

Stats are queryable directly — not hidden internals. All stats are O(1) to read — no sampling, no ANALYZE command, no stale statistics.

```
stats(User)

  ┌─────────────┬──────────────┬───────────────┬─────────────┐
  │ row_count   │ avg_row_size │ total_pages   │ size_bytes  │
  ├─────────────┼──────────────┼───────────────┼─────────────┤
  │ 1200000     │ 64 bytes     │ 9375          │ 75 MB       │
  └─────────────┴──────────────┴───────────────┴─────────────┘

stats(User.idx_email)

  ┌─────────────┬──────────┬─────────────┬─────────────────┐
  │ entries     │ distinct │ min         │ max             │
  ├─────────────┼──────────┼─────────────┼─────────────────┤
  │ 1200000     │ ~1200000 │ aa@test.com │ zz@test.com     │
  └─────────────┴──────────┴─────────────┴─────────────────┘

associations(User)

  ┌──────────┬──────┬───────────────────┐
  │ name     │ kind │ target            │
  ├──────────┼──────┼───────────────────┤
  │ posts    │ 1:N  │ Post.author_id    │
  └──────────┴──────┴───────────────────┘
```

### What the DB Tracks

| Stat | Updated on | Cost |
|---|---|---|
| `row_count` per model | +1 insert, -1 delete | O(1) per mutation |
| `row_count` per index | +1/-1 on entry change | O(1) per mutation |
| `avg_row_size` per model | Running average on insert/update | O(1) per mutation |
| `min`/`max` per indexed column | Insert; lazy-corrected on delete | O(1) amortized |
| `distinct_count` per indexed column | HyperLogLog sketch on insert | O(1), approximate |
| `total_pages` per model | Updated on page alloc/dealloc | O(1) |

### How the DB Uses Stats

Stats serve as a baseline for automatic decisions when inputs haven't been transformed. When inputs have been through filters or other operators, the DB uses **runtime observation** of actual row counts instead.

The formula for estimating result set size in memory:

```
estimated_bytes = row_count * row_size
```

Where `row_size` is computed from the columns at that point in the pipeline:

- **Fixed-size columns** (`Bigint` = 8B, `Float` = 8B, `Boolean` = 1B, `Timestamp` = 8B): exact from schema.
- **Variable-size columns** (`String`): `avg_row_size` running average from catalog, per column.
- **Mixed**: exact for fixed + average for variable.

This is compared against the memory budget to decide whether to stream or materialize. Both the estimate and the budget are visible in stats output.

---

## Security

### Injection is Structurally Impossible

The wire protocol separates query text from parameter values. They are two distinct fields. The server never interpolates parameter values into the query string.

```
Wire protocol message:
  field 1 (query):  "Order.idx_user_id |> find($uid) |> where(status = $s)"
  field 2 (params): {uid: 5, status: "shipped"}
```

The parser sees `$uid` as a typed placeholder. A malicious value like `5) |> delete; User` is just the string `"5) |> delete; User"` — it's a parameter value, not query text. It never reaches the parser.

There is no client API for sending a query string with interpolated values. The unsafe path doesn't exist.

For model and field names (which can't be parameterized): the parser validates them against the catalog at parse time. If a model or field doesn't exist, it's a validation error.

In interactive mode (REPL), inline literal values are allowed — the developer is typing the query directly, with no untrusted input.

---

## Operator Reference

| Operator | Position | Description |
|---|---|---|
| `Model` | start | Read all rows |
| `Model.index \|> find(val)` | start | Exact-match index lookup |
| `Model.index \|> between(lo, hi)` | start | Range scan (inclusive) |
| `where(predicate)` | pipeline | Keep rows matching predicate |
| `scope_name` | pipeline | Apply a named scope (expands to pipeline operators) |
| `select(fields...)` | pipeline | Narrow to specific columns |
| `sort(field [asc\|desc])` | pipeline | Sort rows |
| `limit(n)` | pipeline | Take first n rows |
| `offset(n)` | pipeline | Skip first n rows |
| `group(fields...)` | pipeline | Group by columns |
| `unique` | pipeline | Deduplicate rows |
| `inspect` | pipeline | Print intermediate rows (passthrough, debugging) |
| `insert(fields...)` | mutation | Insert row(s) |
| `update(fields...)` | mutation | Update matched rows |
| `delete` | mutation | Delete matched rows |
| `{ fields... }` | terminal | Selection set — shape output |
| `count(*)` | aggregate | Count rows |
| `sum(field)` | aggregate | Sum values |
| `avg(field)` | aggregate | Average values |
| `min(field)` | aggregate | Minimum value |
| `max(field)` | aggregate | Maximum value |
| `let name = expr` | binding | Bind intermediate query |
| `let $name: Type = val` | parameter | Declare typed scalar parameter |
| `if cond then a else b` | control | Conditional query plan |
| `fn name(args): Type = expr` | definition | Define a scalar function |
| `pipe name(args) = pipeline` | definition | Define a reusable pipeline macro |
| `name(args)` | scalar | Call a scalar function (built-in or user-defined) |
| `stats(Model)` | introspection | Catalog stats for a model |
| `stats(Model.index)` | introspection | Catalog stats for an index |
| `associations(Model)` | introspection | List model associations |

---

## Parser Design

The parser is hand-written and iterative (not a parser generator, not recursive). This keeps dependencies at zero and is straightforward in Zig. The pipeline structure is a linear chain parsed in a loop. Expressions inside operators (predicates in `where`, `on:` conditions) use shunting-yard parsing with explicit fixed-capacity stacks — no recursion. This follows Tiger Style: all resource usage is explicit and bounded.

The parser disambiguates schema definitions from queries by inspecting the first token inside braces: `field`, `validate`, `scope`, `index`, `hasMany`, or `belongsTo` → schema definition. Identifiers, `*`, or `name:` expressions → query selection set.

The AST represents the logical query as a tree of operator nodes (reflecting nested selection sets), each containing its parameters. The executor walks this tree, choosing physical execution strategies at runtime.
