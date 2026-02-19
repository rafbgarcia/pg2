# Packages & User-Defined Functions

This document defines pg2's package system: how developers author reusable types and functions, how users consume them, and how pg2 resolves and executes them.

## Core Design Decisions

**pg2's own language is the implementation language.** A package is a `.pg2` text file — no binary artifacts, no WASM, no compilation step, no SDK. pg2 parses it with the same parser used for schemas and queries and registers the types and functions in the catalog.

This eliminates:

- Language-specific SDKs and ABI contracts
- Binary distribution and platform targeting
- Serialization between host and plugin

**Packages are local files, not runtime dependencies.** pg2 never fetches code from the network. A package is a `.pg2` file that lives in your project alongside your schema. You get it there however you want — copy it from a repository, paste it from documentation, write it yourself, or pull it in via a git submodule. `pg2 apply` reads local files only.

This means:

- Production databases on private networks work without internet access.
- No supply chain attack vector — you review the code when you add it.
- You own the code and can modify it locally.
- No registry, no package manager, no lockfile.

## Package Authoring

A package is a single `.pg2` file containing type and function declarations.

### Structure

```pg2
package("pg2-geo", version = "0.1.0")

type Point {
  field(x, float, notNull)
  field(y, float, notNull)
}

type Polygon {
  field(points, list(Point), notNull)
}

function st_distance(a: Point, b: Point) -> float {
  let dx = a.x - b.x
  let dy = a.y - b.y
  sqrt(dx * dx + dy * dy)
}

function st_within_radius(center: Point, target: Point, radius: float) -> boolean {
  st_distance(center, target) <= radius
}

function st_area(poly: Polygon) -> float {
  let n = length(poly.points)
  let double_area = abs(fold(range(0, n), 0.0, fn(sum, i) {
    let j = if i = n - 1 then 0 else i + 1
    let pi = poly.points[i]
    let pj = poly.points[j]
    sum + (pi.x * pj.y) - (pj.x * pi.y)
  }))
  double_area / 2.0
}

function st_contains(poly: Polygon, p: Point) -> boolean {
  let n = length(poly.points)
  fold(range(0, n), false, fn(inside, i) {
    let j = if i = 0 then n - 1 else i - 1
    let pi = poly.points[i]
    let pj = poly.points[j]
    if (pi.y > p.y) != (pj.y > p.y)
       and p.x < (pj.x - pi.x) * (p.y - pi.y) / (pj.y - pi.y) + pi.x
    then not inside
    else inside
  })
}
```

### Rules

- `package(name, version = "X.Y.Z")` must be the first declaration.
- A package may declare types, functions, and scopes. It must not declare models (tables) — models belong to the application schema.
- Functions may call other functions defined in the same package.
- Packages may depend on other packages via `use(...)` at the top of the manifest, after the `package(...)` line.

### Sharing

Package authors share their `.pg2` files however they like — a Git repository, a gist, documentation, or direct file sharing. There is no central registry.

---

## Package Consumption

### Adding a Package

Copy the `.pg2` file into your project. A conventional location is `pg2_packages/` but any path works:

```
my-app/
  schema.pg2
  pg2_packages/
    pg2-geo.pg2
    pg2-money.pg2
```

### Using a Package

Reference the local file in your schema with `use(...)`:

```pg2
use("pg2_packages/pg2-geo.pg2")

Restaurant {
  field(id, bigint, notNull, primaryKey)
  field(name, string, notNull)
  field(location, Point, notNull)
  field(delivery_zone, Polygon, nullable)
}
```

### Resolution

On `pg2 apply`:

1. Parse the schema file, encounter `use("pg2_packages/pg2-geo.pg2")`.
2. Read the local file. Error if it does not exist.
3. Parse it with the standard pg2 parser.
4. Register types and functions in the catalog.
5. Continue parsing the rest of the application schema — imported types now resolve.

No network access. No caching layer. Just reading a file from disk.

---

## User-Defined Types

Types can be defined in packages or directly in application schemas.

### Composite Types

```pg2
type Address {
  field(street, string, notNull)
  field(city, string, notNull)
  field(zip, string, notNull)
  field(country, string, notNull)
}

type Money {
  field(amount, bigint, notNull)
  field(currency, string, notNull)
}

Customer {
  field(id, bigint, notNull, primaryKey)
  field(name, string, notNull)
  field(billing_address, Address, notNull)
  field(shipping_address, Address, nullable)
}
```

Composite types are stored inline in the row. Fields are accessed with dot notation:

```pg2
Customer
  |> where(billing_address.city = "London")
  { id name billing_address.zip }
```

### Enum Types

```pg2
type OrderStatus = enum(pending, confirmed, shipped, delivered, cancelled)

Order {
  field(id, bigint, notNull, primaryKey)
  field(status, OrderStatus, notNull)
}

Order |> where(status = OrderStatus.shipped) { id }
```

### Parameterized Built-in Types

`list(T)` is a built-in parameterized type. Users cannot define their own generic types.

```pg2
type Polygon {
  field(points, list(Point), notNull)
}
```

### What Is Intentionally Excluded

- **Generic / parametric user types** — keeps the type system simple and storage layout predictable.
- **Inheritance / union types** — creates ambiguity in storage layout and query planning.
- **Methods on types** — functions are standalone, not attached to types. Keeps the function namespace flat and searchable.

---

## User-Defined Functions

### Syntax

```pg2
function name(param: Type, ...) -> ReturnType {
  body_expression
}
```

### Expression Language Extensions for Function Bodies

Function bodies use an extended expression language. Beyond the existing operators and built-ins, function bodies support:

**Let bindings** — immutable local bindings:

```pg2
function st_distance(a: Point, b: Point) -> float {
  let dx = a.x - b.x
  let dy = a.y - b.y
  sqrt(dx * dx + dy * dy)
}
```

**Conditionals** — `if/then/else` (expression-level, always returns a value):

```pg2
function clamp(val: float, lo: float, hi: float) -> float {
  if val < lo then lo
  else if val > hi then hi
  else val
}
```

**Fold** — iteration over lists without mutable state:

```pg2
function sum_points_x(pts: list(Point)) -> float {
  fold(pts, 0.0, fn(acc, p) { acc + p.x })
}
```

`fold(collection, initial, fn(accumulator, element) { body })` iterates over `collection`, threading the accumulator. `range(start, end)` produces an integer sequence for index-based iteration.

**Dot access** — field access on composite types:

```pg2
function full_address(addr: Address) -> string {
  concat(addr.street, ", ", addr.city, " ", addr.zip)
}
```

**List indexing**:

```pg2
poly.points[i]
```

### Execution Model

Functions are compiled to the same expression evaluation path as query expressions. At call time, pg2 looks up the function in the catalog, substitutes arguments, and evaluates the body on the expression work stack. No separate VM or interpreter.

### Bounded Execution

pg2 enforces resource limits on function evaluation:

- Maximum `fold` iterations (prevents infinite loops from malformed data).
- Maximum expression depth (prevents stack overflow from deep recursion).
- These limits are configurable but have safe defaults.

Functions that exceed limits fail explicitly with an error, consistent with pg2's bounded execution principle.

---

## Dynamic Function Registry

Functions (both built-in and user-defined) are stored in the catalog as entries with:

- Name
- Parameter types
- Return type
- Body (expression AST for user-defined; native implementation tag for built-ins)

The parser resolves function calls by looking up the catalog at parse time. This replaces the current hardcoded `isFunctionToken()` switch. Built-in functions (`abs`, `sqrt`, `lower`, etc.) are pre-registered catalog entries with native implementations.

---

## Scope of Exclusion

Things that are explicitly out of scope for the package system:

- **Binary / native extensions** — if logic cannot be expressed in pg2's expression language, it is either a core pg2 feature or an external service call, not a package.
- **I/O from functions** — functions are pure. No network, disk, or clock access.
- **Mutable state in functions** — `let` bindings are immutable. `fold` threads state functionally. No variable reassignment.
- **Package registry or fetching** — pg2 never downloads packages. Users manage files in their project.

---

## Example: End-to-End

Someone writes `pg2-money.pg2`:

```pg2
package("pg2-money", version = "0.1.0")

type Money {
  field(amount, bigint, notNull)
  field(currency, string, notNull)
}

function money_add(a: Money, b: Money) -> Money {
  if a.currency != b.currency
  then error("currency mismatch")
  else Money(a.amount + b.amount, a.currency)
}

function money_to_string(m: Money) -> string {
  concat(to_string(m.amount), " ", m.currency)
}
```

A user copies it into their project and references it:

```pg2
use("pg2_packages/pg2-money.pg2")

Invoice {
  field(id, bigint, notNull, primaryKey)
  field(total, Money, notNull)
}

-- query
Invoice |> where(total.currency = "GBP") { id total_str: money_to_string(total) }
```

```bash
pg2 apply
# → Loaded pg2-money@0.1.0 (1 type, 2 functions)
# → Applied schema: Invoice (created)
```

The user can modify `pg2-money.pg2` directly if they want different behavior. It's their file.
