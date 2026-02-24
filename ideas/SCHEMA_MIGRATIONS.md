# Schema Migrations

pg2 manages schema changes automatically. You declare the desired schema; pg2 computes the diff, plans the migration, and applies it — always online, always safe.

## How It Works

### Workflow

```
1. Edit db/schema.pg2          — declare the schema you want
2. pg2 generate                — pg2 diffs against current state, asks about ambiguities
3. Commit the result           — schema file + migration plan go into version control
4. pg2 apply                   — on deploy, pg2 applies unapplied changes idempotently
```

`pg2 generate` is interactive. It runs locally, compares your schema file against the last-known state, detects what changed, and asks you when the intent is ambiguous (e.g., "did you rename `name` to `first_name`, or drop `name` and add `first_name`?"). It produces a migration plan file that records your answers.

`pg2 apply` is deterministic. It reads the migration plan, checks the current catalog, and applies any unapplied operations. Safe to run multiple times — operations that are already complete are skipped.

### Migration Plan Files

Migration plans live in `db/migrations/` and are written in pg2 syntax. They record only the disambiguation decisions and data transforms that pg2 cannot infer from the schema diff alone.

```pg2
migration("0002", from: "a1b2c3", to: "d4e5f6") {
  rename(User.name -> User.first_name)
  backfill(User.last_name, "")
}
```

`from` and `to` are content hashes of the schema file. pg2 derives the full operation plan from the diff between those two versions; the migration file captures human intent where needed.

For unambiguous changes (adding a new column, adding an index), the migration plan file is generated automatically with no user input required.

### Tracking Progress

All migration state lives in the pg2 catalog and is queryable:

```pg2
pg2.migrations { id status started_at completed_at }
pg2.operations { id migration_id type target status progress }
```

Background operations report their progress. There is no hidden state.

---

## Column Naming

Column names in pg2 are aliases. On disk, columns are identified by stable internal IDs. The human-facing name is a catalog mapping.

This means every rename is a catalog-only operation — instant, no data movement, no multi-step safety dance. On-disk data is never touched.

This applies to all column name operations uniformly. When this document says "instant (catalog-only)", it means: a single catalog write, committed in the same transaction, visible to all subsequent queries immediately.

---

## Operations Reference

For each schema change, this section describes what pg2 does, whether it happens instantly or in the background, and what the application can expect during the transition.

### Add Field

```pg2
# before                          # after
User {                             User {
  field(id, bigint, notNull)         field(id, bigint, notNull)
  field(email, string, notNull)      field(email, string, notNull)
}                                    field(bio, string, nullable)
                                   }
```

**What pg2 does:**

- Adds the column to the catalog with its default value (or NULL for nullable columns).
- Existing rows on disk are **not rewritten**. They simply don't contain the new column yet.
- When a row without the new column is read, pg2 returns the declared default (or NULL). This is transparent to the application.
- On the next write to that row (any update), the new column is materialized on disk.

**Availability:** Instant. The column is usable in queries immediately after `pg2 apply` completes.

**Disk impact:** None at apply time. Storage grows incrementally as rows are updated.

### Add Field (notNull with default)

```pg2
# before                          # after
User {                             User {
  field(id, bigint, notNull)         field(id, bigint, notNull)
  field(email, string, notNull)      field(email, string, notNull)
}                                    field(role, string, notNull,
                                       default("member"))
                                   }
```

**What pg2 does:**

- Adds the column to the catalog with a virtual default stored in catalog metadata.
- Existing rows are not rewritten. Reads return the virtual default for rows that predate the column.
- The virtual default is indistinguishable from a physically stored value. Queries, filters, and indexes all see it.

**Availability:** Instant. No background work required.

**Disk impact:** None at apply time. The default is stored once in the catalog, not per-row.

### Add Field (notNull without default)

```pg2
# before                          # after
User {                             User {
  field(id, bigint, notNull)         field(id, bigint, notNull)
  field(email, string, notNull)      field(email, string, notNull)
}                                    field(role, string, notNull)
                                   }
```

**What pg2 does:**

- **Rejects the migration** if the table contains existing rows. A `notNull` column with no default and no backfill expression has no valid value for existing data.
- `pg2 generate` will ask you to provide either a `default(...)` or a `backfill(...)` expression.

**Availability:** N/A — requires user input to resolve.

### Drop Field

```pg2
# before                          # after
User {                             User {
  field(id, bigint, notNull)         field(id, bigint, notNull)
  field(email, string, notNull)      field(email, string, notNull)
  field(bio, string, nullable)     }
}
```

**What pg2 does:**

- Removes the column from the catalog. Queries can no longer reference it.
- On-disk data for that column remains in existing rows but is ignored on read.
- Disk space is reclaimed incrementally: when a row is rewritten for any reason (update, compaction), the dropped column's bytes are omitted.

**Availability:** Instant. The column disappears from the application's view immediately.

**Disk impact:** None at apply time. Space reclaimed gradually.

**Safety:** `pg2 generate` flags this as a destructive change and requires explicit confirmation. The column data is retained on disk until physically overwritten — it is not immediately destroyed — but there is no built-in undo. Drops are permanent.

### Rename Field

```pg2
# before                          # after
User {                             User {
  field(name, string, notNull)       field(display_name, string, notNull)
}                                  }
```

**What pg2 does:**

- Updates the name-to-column-ID mapping in the catalog.
- No data is read, moved, or rewritten.

**Availability:** Instant (catalog-only). The new name is usable immediately; the old name stops working immediately.

**Disk impact:** Zero.

**Ambiguity:** If a field disappears and a new field of the same type appears, `pg2 generate` asks whether this is a rename or a drop-and-add.

### Change Field Type

```pg2
# before                          # after
User {                             User {
  field(age, string, notNull)        field(age, int, notNull)
}                                  }
```

**What pg2 does:**

- Adds a shadow column (new type) to the catalog, invisible to the application.
- A background worker reads every row, converts the value, and writes it to the shadow column.
- New writes during the transition write to both the old and shadow columns.
- Once all rows are converted, pg2 atomically swaps: the shadow column becomes the real column, the old column becomes a dropped column.

**Availability:** The column remains usable throughout the migration under the old type. The new type becomes visible only after the background conversion completes.

**Disk impact:** Temporary increase — two copies of the column data exist during conversion. Reclaimed after swap.

**Failure:** If conversion fails for any row (e.g., a string that cannot be parsed as an integer), the migration is paused and the failure is reported in `pg2.operations`. No data is lost. The application continues using the old type.

### Change Nullability (nullable to notNull)

```pg2
# before                          # after
User {                             User {
  field(bio, string, nullable)       field(bio, string, notNull)
}                                  }
```

**What pg2 does:**

- A background worker scans every row to verify no NULLs exist in the column.
- If the scan passes, the constraint is enabled in the catalog. New inserts/updates enforce it immediately.
- If any NULL is found, the migration is paused and reported.

**Availability:** Reads and writes continue normally during the scan. The constraint only takes effect after validation completes.

### Change Nullability (notNull to nullable)

Instant (catalog-only). The constraint is relaxed immediately. No scan required.

### Change Default Value

Instant (catalog-only). The new default applies to subsequent inserts. Existing rows are not affected.

### Add Index

```pg2
# before                          # after
User {                             User {
  field(email, string, notNull)      field(email, string, notNull)
}                                    index(idx_email, [email], unique)
                                   }
```

**What pg2 does:**

- Registers the index in the catalog with status `building`.
- A background worker scans the table and populates the index.
- During the build, all new writes to the table also update the in-progress index, so it stays current.
- Once the build completes, the index status changes to `ready` and the query planner begins using it.
- For unique indexes: uniqueness violations discovered during the build pause the migration and are reported.

**Availability:** The application can reference the index in queries immediately (pg2 parses and accepts the query). The index is simply not used for execution until the build finishes. Queries are never blocked.

**Disk impact:** Index size depends on the indexed columns and row count. Built incrementally.

### Drop Index

Instant (catalog-only). The index is removed from the catalog and the planner stops using it. Disk pages are reclaimed during compaction.

### Add Reference (withoutReferentialIntegrity)

```pg2
reference(posts, id, Post.user_id, withoutReferentialIntegrity)
```

Instant (catalog-only). This is query metadata only — it tells the planner how to traverse relations. No data validation, no constraints enforced.

### Add Reference (withReferentialIntegrity)

```pg2
reference(author, user_id, User.id, withReferentialIntegrity(onDeleteRestrict, onUpdateCascade))
```

**What pg2 does:**

- Registers the reference in the catalog.
- A background worker scans the referencing table to verify every foreign key value exists in the referenced table.
- Once validation passes, the constraint is enforced on all subsequent mutations.
- If orphaned rows are found, the migration is paused and reported.

**Availability:** Queries can traverse the reference immediately (it works as metadata while validation runs). Constraint enforcement begins only after the validation scan completes.

### Change Reference Policy

Changing from `withoutReferentialIntegrity` to `withReferentialIntegrity` (or changing the actions) follows the same pattern as adding a new RI reference: background validation scan, then enforcement.

Changing from `withReferentialIntegrity` to `withoutReferentialIntegrity` is instant — the constraint is dropped from the catalog.

### Add Model

```pg2
# new model
Order {
  field(id, bigint, notNull, primaryKey)
  field(total, float, notNull)
}
```

Instant. pg2 creates the catalog entries (table metadata, column mappings, primary key index). No disk pages are allocated until the first insert. The model is queryable immediately (returns empty results).

### Drop Model

**What pg2 does:**

- Removes the model from the catalog. All queries referencing it fail immediately.
- If other models have `withReferentialIntegrity` references pointing to it, the migration is rejected. Remove or change those references first.
- If other models have `withoutReferentialIntegrity` references, those references are also removed (with a warning).
- On-disk pages (heap and indexes) are reclaimed during compaction.

**Safety:** `pg2 generate` requires explicit confirmation. This is an irreversible, destructive operation.

### Rename Model

Instant (catalog-only). Same principle as field renames — models are identified internally by stable IDs. The name is a catalog alias.

### Add Scope

```pg2
scope(active, where(active = true))
```

Instant (catalog-only). Scopes are query metadata stored in the catalog. No data changes.

### Drop / Change Scope

Instant (catalog-only).

---

## Summary Table

| Operation                       | Apply speed            | Background work         | Data rewritten        |
| ------------------------------- | ---------------------- | ----------------------- | --------------------- |
| Add field (nullable)            | Instant                | None                    | No                    |
| Add field (notNull + default)   | Instant                | None                    | No                    |
| Add field (notNull, no default) | Rejected               | —                       | —                     |
| Drop field                      | Instant                | None                    | No (lazy reclaim)     |
| Rename field                    | Instant                | None                    | No                    |
| Change field type               | Instant (old type)     | Row-by-row conversion   | Yes (shadow column)   |
| nullable → notNull              | Instant (unenforced)   | Validation scan         | No                    |
| notNull → nullable              | Instant                | None                    | No                    |
| Change default                  | Instant                | None                    | No                    |
| Add index                       | Instant (not yet used) | Index build             | Yes (new index pages) |
| Drop index                      | Instant                | None                    | No (lazy reclaim)     |
| Add reference (no RI)           | Instant                | None                    | No                    |
| Add reference (with RI)         | Instant (unenforced)   | Validation scan         | No                    |
| Change RI policy                | Instant or scan        | Validation if adding RI | No                    |
| Add model                       | Instant                | None                    | No                    |
| Drop model                      | Instant                | None                    | No (lazy reclaim)     |
| Rename model                    | Instant                | None                    | No                    |
| Add/drop/change scope           | Instant                | None                    | No                    |

---

## Handling Ambiguity

When `pg2 generate` cannot determine intent from the schema diff alone, it asks. Common ambiguities:

| Diff observed                           | Possible intents                 | pg2 asks                                                                          |
| --------------------------------------- | -------------------------------- | --------------------------------------------------------------------------------- |
| Field disappears, similar field appears | Rename vs. drop-and-add          | "Was `name` renamed to `first_name`, or is this a new field?"                     |
| Field disappears, no replacement        | Drop vs. accidental deletion     | "Confirm dropping `bio` and its data?"                                            |
| Field type changes                      | Intentional type change vs. typo | "Convert `age` from `string` to `int`? Provide conversion expression or default." |
| Model disappears                        | Drop vs. accidental deletion     | "Confirm dropping model `TempData` and all its data?"                             |

Answers are recorded in the migration plan file. `pg2 apply` never asks questions — it executes the plan as written.

---

## Concurrent Migrations

If a background operation is already in progress (e.g., an index build) and a new `pg2 apply` runs with additional changes:

- Non-conflicting operations proceed in parallel (e.g., adding a column while an index builds on a different column).
- Conflicting operations are queued (e.g., two operations that both rewrite the same table).
- The catalog always reflects the latest desired state. Operations are ordered to preserve consistency.

---

## Failure and Recovery

All background migration operations are crash-safe:

- Progress is checkpointed to the catalog periodically.
- On crash recovery, incomplete operations resume from their last checkpoint, not from the beginning.
- A failed operation (e.g., a type conversion that encounters invalid data) is paused, not rolled back. The table remains usable under the pre-migration schema. Fix the data, then re-run `pg2 apply`.

No migration operation ever leaves the database in an inconsistent state. Either the operation completes and the new schema takes effect, or it doesn't and the old schema remains fully functional.
