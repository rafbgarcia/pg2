# TODO

1. Add a concrete server transport implementation.
   - Move per-connection recv/send off blocking stream calls to `io_uring` operations (accept is already `io_uring`-driven on Linux).
   - Remove/limit fallback behavior once `io_uring` path is robust on target kernels.
   - Preserve deterministic/fake transport path for simulation and unit tests.
   - Keep bounded request/response behavior and fail closed on overflow.

2. Continue expanding deterministic fault-matrix coverage as new durability/recovery paths land.
   - Grow seeded interleaving breadth in CI (larger seed sets and longer schedules) while keeping bounded budgets.
