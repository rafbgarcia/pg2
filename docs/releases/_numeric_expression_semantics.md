## Focus Item

Numeric expression semantics are now explicit and fail-closed.

- Integer arithmetic stays in integer domain when both operands are integers.
- Same-type integer arithmetic preserves exact integer type (`u16 + u16 -> u16`, etc.).
- Mixed integer arithmetic promotes to `i64` when either operand is signed, otherwise to `u64`.
- `f64` arithmetic is used only when at least one operand is `f64`.
- Unsafe promotion and overflow return `NumericOverflow`.

## Behavior Matrix

| lhs type class | rhs type class | result domain | result type |
| --- | --- | --- | --- |
| signed int `T` | signed int `T` | integer | `T` |
| unsigned int `T` | unsigned int `T` | integer | `T` |
| signed int | unsigned int | integer | `i64` (or `NumericOverflow` if coercion is unsafe) |
| unsigned int | unsigned int (mixed widths) | integer | `u64` |
| any integer | `f64` | float | `f64` |
| `f64` | any integer | float | `f64` |

## Design Tradeoffs To Remember

1. Mixed signed/unsigned expressions prefer deterministic safety over permissive widening.
2. Exact-type preservation for same-type integers avoids silent precision/semantic drift in updates.
3. Overflow is an explicit error, never an implicit wrap or float fallback.
