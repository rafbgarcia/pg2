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

## Running Simulations

```bash
# Run with a specific seed (reproducible)
zig build sim -- --seed 12345 --ticks 1000000

# Run with random seeds in a loop (fuzzing)
zig build sim -- --fuzz --iterations 1000

# Run with aggressive fault injection
zig build sim -- --seed 12345 --crash-probability 0.01 --partial-write-probability 0.05
```

When a simulation fails, it prints the seed. Re-running with that seed reproduces the exact failure.

## Build Steps

1. **Implement SimulatedDisk** — in-memory page store with pending writes and fsync semantics. No fault injection yet, just correct simulation of write-then-fsync durability.
2. **Implement SimulatedClock** — trivial, just a counter.
3. **Implement Scheduler** — single-threaded loop that picks actors and steps them.
4. **Wire the storage engine to use the Storage interface** — verify that the buffer pool + WAL work correctly under simulation with no faults.
5. **Add crash-recovery testing** — simulate crash (discard pending writes), restart, run recovery, verify consistency.
6. **Add fault injection to SimulatedDisk** — partial writes, read errors, bit flips, fsync failures.
7. **Implement SimulatedNetwork** — needed once replication is added.
8. **Add replication invariant checks** — once replication exists.

## Reference

- TigerBeetle's VOPR simulator: https://github.com/tigerbeetle/tigerbeetle (src/simulator.zig)
- FoundationDB's simulation testing talk: "Testing Distributed Systems w/ Deterministic Simulation" (Will Wilson, Strange Loop 2014)
- Jepsen (external black-box testing, complementary approach): https://jepsen.io
