# Deterministic Simulation Testing

## Why This Matters

Databases fail in production due to edge cases that are nearly impossible to hit in normal testing: crashes mid-fsync, partial page writes, network partitions during replication, OOM at the worst possible moment. Deterministic simulation testing finds these bugs by:

1. Running the entire database in a single process with simulated I/O.
2. Controlling all sources of nondeterminism (time, scheduling, randomness) with a single seed.
3. Injecting faults systematically.
4. Making any failure reproducible: re-run with the same seed, get the same execution.

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Simulator                       │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │ Client 1 │  │ Client 2 │  │ Client N │      │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘      │
│       │              │              │             │
│       ▼              ▼              ▼             │
│  ┌──────────────────────────────────────────┐   │
│  │         Deterministic Scheduler          │   │
│  │    (seeded PRNG picks next action)       │   │
│  └──────────────────┬───────────────────────┘   │
│                     │                            │
│       ┌─────────────┼─────────────┐             │
│       ▼             ▼             ▼              │
│  ┌─────────┐  ┌──────────┐  ┌──────────┐       │
│  │ Primary │  │ Replica1 │  │ Replica2 │       │
│  └────┬────┘  └────┬─────┘  └────┬─────┘       │
│       │             │             │               │
│       ▼             ▼             ▼               │
│  ┌──────────────────────────────────────────┐   │
│  │         Simulated I/O Layer              │   │
│  │  ┌─────────┐ ┌─────────┐ ┌───────────┐  │   │
│  │  │  Disk   │ │ Network │ │   Clock   │  │   │
│  │  │ (fault) │ │ (fault) │ │ (manual)  │  │   │
│  │  └─────────┘ └─────────┘ └───────────┘  │   │
│  └──────────────────────────────────────────┘   │
└─────────────────────────────────────────────────┘
```

Everything runs in one OS thread. The scheduler uses a seeded PRNG to decide which actor (client, primary, replica) gets to execute next. This makes the execution fully deterministic.

## Allocation Rules for Simulation Code

The Tiger Style static allocation rules apply to **production code** — the database engine itself. Simulation code (the harness, simulated disk, simulated network) is exempt because:

1. The simulation binary is a test tool, not deployed in production.
2. Simulated components need to model arbitrary states (e.g., a hash map of pages to simulate a disk) which don't map to fixed-capacity structures.
3. The simulation's purpose is to test production code behavior, not its own.

Simulation code uses `std.testing.allocator` (which detects leaks) and standard library data structures. The production code under test uses the `StaticAllocator` and is subject to all Tiger Style constraints.

## Simulated Components

### Simulated Disk

```zig
const SimulatedDisk = struct {
    pages: std.AutoHashMap(u64, [8192]u8),
    pending_writes: std.ArrayList(PendingWrite),
    prng: std.rand.DefaultPrng,

    // Fault injection knobs
    partial_write_probability: f64,    // write only half the page
    read_error_probability: f64,       // return I/O error on read
    bit_flip_probability: f64,         // flip a random bit in read data
    fsync_failure_probability: f64,    // fsync returns success but doesn't persist

    pub fn write(self: *Self, offset: u64, data: []const u8) !void {
        // Data goes to pending_writes, not directly to pages.
        // Only fsync moves pending_writes into pages (simulating durability).
    }

    pub fn fsync(self: *Self) !void {
        // With some probability, "crash" here (don't persist all pending writes).
        // With some probability, persist only partial pages.
        // Otherwise, move all pending_writes to pages.
    }

    pub fn read(self: *Self, offset: u64, buf: []u8) !void {
        // With some probability, return an error or flip bits.
        // Otherwise, return the page from pages map.
    }
};
```

### Simulated Network

```zig
const SimulatedNetwork = struct {
    in_flight: std.ArrayList(InFlightMessage),
    prng: std.rand.DefaultPrng,

    // Fault injection knobs
    drop_probability: f64,
    reorder_probability: f64,
    duplicate_probability: f64,
    max_latency_ticks: u64,

    pub fn send(self: *Self, from: PeerId, to: PeerId, msg: []const u8) void {
        // Add to in_flight with a delivery tick based on latency + jitter.
        // With some probability, drop the message entirely.
        // With some probability, duplicate it.
    }

    pub fn deliver(self: *Self, tick: u64) ?Message {
        // Return messages whose delivery tick <= current tick.
        // With some probability, reorder delivery.
    }
};
```

### Simulated Clock

```zig
const SimulatedClock = struct {
    current_tick: u64 = 0,

    pub fn now(self: *Self) u64 {
        return self.current_tick;
    }

    pub fn advance(self: *Self, ticks: u64) void {
        self.current_tick += ticks;
    }
};
```

## Scheduler

The scheduler is the core of the simulation. Each "tick":

1. Pick a random actor to run (weighted by the PRNG).
2. Let that actor execute one step (e.g., process one query, handle one WAL record, deliver one network message).
3. Optionally inject a fault (crash a node, drop a write, partition the network).
4. Check invariants.

```zig
const Scheduler = struct {
    prng: std.rand.DefaultPrng,
    actors: []Actor,
    tick: u64 = 0,

    pub fn run(self: *Self, max_ticks: u64) !void {
        while (self.tick < max_ticks) : (self.tick += 1) {
            const actor_idx = self.prng.random().uintLessThan(usize, self.actors.len);
            try self.actors[actor_idx].step();
            try self.checkInvariants();
        }
    }
};
```

## Invariant Checks

After every tick (or every N ticks for expensive checks), the simulator verifies:

1. **WAL consistency**: every committed transaction's effects are present in the data pages (after recovery).
2. **MVCC correctness**: no transaction sees uncommitted data from another transaction; snapshot isolation holds.
3. **Crash recovery**: after simulating a crash, restart the database, run recovery, and verify all committed transactions are present and all aborted transactions are rolled back.
4. **Replication correctness**: replicas eventually converge to the primary's committed state.
5. **No lost writes**: every acknowledged commit is durable (survives crashes after the acknowledgment).

Current deterministic matrix scenarios also enforce these concrete recovery and
visibility invariants:

1. **Rollback visibility edge**: an aborted head version does not become visible,
   and readers consistently resolve to the correct prior version.
2. **WAL+undo crash consistency**: when a mutation fails before WAL durability,
   pre-crash undo-based visibility matches post-restart persisted row visibility.
3. **Replay determinism across seed sets**: running the same scenario twice with
   the same seed must produce the same signature.

## Running Simulations

```bash
# Run deterministic simulation regression tests
zig build sim

# Run simulator executable with a specific seed (reproducible)
zig build sim-run -- --seed 12345 --ticks 1000000

# Run with random seeds in a loop (fuzzing)
zig build sim-run -- --fuzz --iterations 1000

# Run with aggressive fault injection
zig build sim-run -- --seed 12345 --crash-probability 0.01 --partial-write-probability 0.05
```

When a simulation fails, it prints the seed. Re-running with that seed reproduces the exact failure.

## Reference

- TigerBeetle's VOPR simulator: https://github.com/tigerbeetle/tigerbeetle (src/simulator.zig)
- FoundationDB's simulation testing talk: "Testing Distributed Systems w/ Deterministic Simulation" (Will Wilson, Strange Loop 2014)
- Jepsen (external black-box testing, complementary approach): https://jepsen.io
