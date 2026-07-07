# DistributedKeyValueStore

A production-grade distributed key-value store built from scratch in Go, implementing core Redis concepts, including a custom RESP protocol parser, AOF persistence, consistent hashing, primary-replica replication, a coordinator proxy, and a sharded in-memory store. Fully containerized with Docker Compose.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Features](#features)
- [System Design Concepts](#system-design-concepts)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Running the Distributed Cluster](#running-the-distributed-cluster)
- [Testing](#testing)
- [Design Decisions & Trade-offs](#design-decisions--trade-offs)
- [Known Limitations & Future Work](#known-limitations--future-work)

---

## Architecture Overview

```
                        Clients (redis-cli)
                              │
                              ▼
                    ┌─────────────────┐
                    │   Coordinator    │  :7000
                    │  (Proxy + Ring)  │
                    └────────┬────────┘
                             │  Consistent Hash Ring
                   ┌─────────┴──────────┐
                   ▼                    ▼
            ┌────────────┐      ┌────────────┐
            │   Node 1   │      │   Node 2   │
            │   :6379    │      │   :6380    │
            │  store+AOF │      │  store+AOF │
            └─────┬──────┘      └────────────┘
                  │ Replication
                  ▼
            ┌────────────┐
            │  Replica 1 │
            │   :6381    │
            └────────────┘
```

Clients connect exclusively to the **coordinator**, which routes every request to the correct backend node using consistent hashing. Backend nodes are independent, each runs its own in-memory store, AOF persistence, and replication. A replica of Node 1 stays in sync via a live replication stream.

---

## Features

- **Redis-compatible RESP protocol**: works with `redis-cli` and any standard Redis client out of the box
- **Sharded in-memory store**: 16-shard mutex design eliminates global lock contention
- **AOF persistence**: three sync policies (always / every second / never), crash-tolerant replay
- **Consistent hashing** with virtual nodes: minimal key remapping when cluster topology changes
- **Primary-replica replication**: live command propagation with automatic reconnection
- **Coordinator proxy**: transparent request routing, connection pooling, scatter-gather aggregation
- **Health checking**: automatic node failure detection and ring removal
- **Fuzz-tested RESP parser**: discovered and fixed a real DoS vulnerability during development
- **Graceful shutdown**: context-based cancellation, WaitGroup-tracked connections, AOF flush-on-exit
- **Docker Compose cluster**: one command to spin up the full distributed setup
- **Race-detector clean**: passes `go test -race -count=50 ./...` reliably

---

## System Design Concepts

### 1. RESP Protocol (Redis Serialization Protocol)

All client-server and coordinator-node communication uses RESP2, the same wire protocol as Redis. The parser is hand-rolled in `internal/resp/`, handling all five RESP types: simple strings, errors, integers, bulk strings, and arrays.

Key implementation detail: bulk strings use **length-prefix framing** rather than delimiter scanning, making the parser binary-safe. A fuzzer found that unbounded length fields could trigger a multi-gigabyte allocation (DoS), which was fixed by capping bulk string and array lengths to sane maximums, matching real Redis's own limits.

```
Client sends:   *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
Server replies: +OK\r\n
```

### 2. Sharded Locking (Lock Striping)

A single global mutex serializes all operations, even completely unrelated keys. The store instead splits the keyspace across **16 independent shards**, each with its own `sync.RWMutex`. Keys are assigned to shards via FNV-1a hashing:

```
hash(key) % 16  ->  shard index  ->  shard-level lock
```

Goroutines touching different keys can proceed fully in parallel. Benchmarks (`go test -bench=.`) compare sequential vs. parallel-high-contention workloads to measure the real throughput gain.

### 3. TTL / Lazy Expiration

Every entry stores an `expiresAt time.Time` field. The zero value (`time.Time{}`) means "never expires", a deliberate sentinel that avoids a separate boolean flag. Expiration uses **lazy checking**: entries are only tested when accessed (`Get`, `Exists`, `Keys`). A background eviction goroutine (ticker-based, identical in structure to the AOF sync goroutine) sweeps all shards every second to physically delete expired keys and reclaim memory.

### 4. AOF Persistence

Every mutating command (`SET`, `DEL`, `EXPIRE`, `PERSIST`) is appended to a log file in RESP format immediately after being applied to the store. On startup, the server replays this log to reconstruct state, using the same `resp.ReadCommand` parser used for live client connections, so no separate format or parser is needed.

Three sync policies balance durability against performance:

| Policy | `fsync` timing | Data loss on crash |
|---|---|---|
| `always` | After every write | At most one in-flight command |
| `everysecond` | Background goroutine, ~1s | At most ~1 second |
| `never` | OS decides | Potentially more |

**Crash tolerance:** A truncated final entry (caused by a crash mid-write) is logged and skipped rather than failing the entire replay, verified by `TestCorruptedAOFReplayIsSafe`, which deliberately injects a truncated entry and confirms the server starts correctly with all prior entries intact.

**AOF Rewrite:** A `Rewrite` method compacts the log by snapshotting current store state as a minimal set of `SET` commands, then atomically replacing the old file via `os.Rename`, which is filesystem-atomic on Linux, guaranteeing no window where the log is half-old, half-new.

### 5. Consistent Hashing

The coordinator routes requests using a consistent hash ring (`internal/cluster/ring.go`). Each physical node is placed at **150 virtual positions** on a `uint32` ring (positions computed via SHA-256, which gives better avalanche behavior and DoS resistance compared to FNV). Key lookup uses `sort.Search` (binary search) for O(log N) routing.

The critical property: **removing one node only remaps ~1/N of keys**, specifically, only the keys whose owning virtual position was on the removed node. `TestRemappingOnNodeRemoval` verifies this directly: it snapshots 10,000 key assignments, removes a node, and asserts that fewer than 5% of keys that weren't on the removed node moved anywhere.

`TestDistribution` verifies that with 3 nodes and 150 replicas each, every node owns 33% ± 10% of a 10,000-key workload, confirming the virtual-node count is sufficient to smooth out hash distribution.

### 6. Coordinator Proxy

The coordinator (`internal/coordinator/`) is a transparent RESP proxy. Clients talk to it exactly as they would to a single Redis server, the coordinator handles all routing internally.

- **Key-bearing commands** (`SET`, `GET`, `DEL`, etc.) are routed to a single node via `ring.GetNode(key)`
- **Cluster-wide commands** (`DBSIZE`, `FLUSHDB`) are scatter-gathered: sent concurrently to all nodes via `sync.WaitGroup`, results aggregated
- **Connection pooling** (`internal/cluster/pool.go`): one pool per backend node, up to 10 idle connections, LIFO ordering to prefer freshest connections

### 7. Primary-Replica Replication

Replication uses a dedicated listener port (separate from the client port) to avoid mixing replication handshakes with client traffic.

**Primary side** (`internal/replication/primary.go`): After every successful write, `Propagate` serializes the command **once** (not once per replica, an explicit optimization) and sends it to all connected replicas. Dead replicas are detected via write errors and removed in a two-phase lock upgrade to avoid deadlock: collect failures under `RLock`, then remove under `Lock`.

**Replica side** (`internal/replication/replica.go`): `StartReplica` runs an infinite reconnection loop: connect, handshake (`REPLCONF`), stream commands, apply to local store. On any error (primary down, network hiccup), wait 3 seconds and retry. This gives automatic self-healing with no manual intervention.

### 8. Health Checking

`internal/coordinator/health.go` runs a background goroutine that checks every node every N seconds. Each check:
1. Opens a **fresh TCP connection** (not from the pool) with a 500ms timeout
2. Sends `PING`, checks for `PONG`
3. On failure: atomically marks node dead via `sync.Map.LoadOrStore` (edge-triggered, fires `RemoveNode` exactly once on the alive->dead transition, not on every subsequent failure)
4. On recovery: `LoadAndDelete` detects the dead->alive transition and calls `AddNode`

### 9. Graceful Shutdown

Every long-running component has a clean shutdown path:

- **Server**: `context.WithCancel` + `signal.Notify(SIGINT, SIGTERM)` -> `cancel()` -> `ln.Close()` unblocks `Accept()` -> `connections.Wait()` drains in-flight requests -> `aof.Close()` flushes and fsyncs
- **AOF background sync**: `select` on ticker vs. stop channel
- **Store eviction**: same stop-channel pattern
- **Replica connections**: closing all `net.Conn`s in `Primary.Close()` unblocks all `handleReplica` goroutines' blocked `Read()` calls, causing them to exit naturally

---

## Project Structure

```
├── cmd/
│   ├── server/main.go          # KV node entrypoint (flags: -addr, -aof, -repl-port, -replicaof)
│   └── coordinator/main.go     # Coordinator entrypoint (flags: -addr, -nodes)
├── internal/
│   ├── resp/                   # RESP protocol parser and writer
│   │   ├── reader.go           # ReadCommand, readArray, readBulkString
│   │   ├── writer.go           # WriteSimpleString, WriteBulkString, WriteArray...
│   │   ├── reader_test.go      # Table-driven parser tests incl. unicode
│   │   └── fuzz_test.go        # Fuzz test: found real DoS vulnerability
│   ├── store/                  # Sharded in-memory KV store
│   │   ├── store.go            # Get, Set, Delete, Expire, Persist, Keys, Len
│   │   └── eviction.go         # Background TTL sweep
│   ├── persistence/            # AOF write-ahead log
│   │   ├── aof.go              # Append, Replay, Rewrite, backgroundSync
│   │   └── aof_test.go         # Round-trip and crash-corruption tests
│   ├── cluster/                # Consistent hash ring + connection pool
│   │   ├── ring.go             # AddNode, RemoveNode, GetNode, GetNodes
│   │   ├── pool.go             # ConnPool: Get (LIFO), Put, Close
│   │   └── ring_test.go        # Distribution and remapping tests
│   ├── coordinator/            # Proxy layer
│   │   ├── coordinator.go      # ListenAndServe, handleClient, forward
│   │   └── health.go           # Background health checker
│   ├── replication/            # Primary-replica replication
│   │   ├── primary.go          # Propagate (fanout), handleReplica, Close
│   │   └── replica.go          # StartReplica, connectAndStream, applyToStore
│   └── server/                 # Backend KV server
│       ├── server.go           # ListenAndServe, handleConn, idle timeouts
│       ├── handler.go          # dispatch, isConnectionClosed
│       └── commands.go         # commandSet, commandGet, commandDel...
├── integration/
│   └── server_test.go          # End-to-end: real server on random port, real TCP client
├── Dockerfile                  # Multi-stage build -> ~10MB scratch image
├── docker-compose.yml          # 2 nodes + 1 replica + coordinator
└── Makefile                    # build, test, bench, run-cluster, clean
```

---

## Getting Started

### Prerequisites

- Go 1.26+
- Docker Desktop (for cluster mode)
- `redis-cli` (optional, for manual testing)

### Single node (no Docker)

```bash
go run ./cmd/server -addr :6379 -aof appendonly.aof
redis-cli -p 6379 SET foo bar
redis-cli -p 6379 GET foo
```

---

## Running the Distributed Cluster

```bash
# Build images and start 2 nodes + 1 replica + coordinator
make run-cluster
# or
docker-compose up --build
```

Services started:

| Service | Client Port | Replication Port | Role |
|---|---|---|---|
| node1 | 6379 | 6399 | Primary |
| node2 | 6380 | 6400 | Primary |
| replica1 | 6381 | — | Replica of node1 |
| coordinator | 7000 | — | Proxy |

```bash
# All commands go through coordinator
redis-cli -p 7000 SET user:1 Alice
redis-cli -p 7000 SET user:2 Bob
redis-cli -p 7000 GET user:1       # -> Alice
redis-cli -p 7000 DBSIZE           # -> aggregated count across all nodes

# Verify replication
redis-cli -p 6381 GET user:1       # -> Alice (if user:1 hashed to node1)

# Simulate node failure
docker stop distributedkeyvaluestore-node1-1
redis-cli -p 7000 PING             # -> PONG (coordinator still up)

# Teardown
make clean
```

---

## Testing

```bash
# All tests with race detector
go test -race ./...

# With 50 repetitions (surfaces flaky tests and resource leaks)
go test -race -count=50 ./...

# Benchmarks
go test -bench="." -benchmem ./internal/store/

# Fuzz the RESP parser (runs until stopped or crash found)
go test -fuzz=FuzzReadCommand -fuzztime=60s ./internal/resp/

# Coverage report
make test-coverage
open coverage.html
```

### Test highlights

| Test | What it proves |
|---|---|
| `TestReplayRestoresState` | AOF round-trip: write commands, replay from disk, verify store state |
| `TestCorruptedAOFReplayIsSafe` | Crash-mid-write tolerance: truncated entry is skipped, prior entries intact |
| `TestDistribution` | 3 nodes × 150 replicas -> each owns 33% ± 10% of 10,000 keys |
| `TestRemappingOnNodeRemoval` | Removing one node remaps fewer than 5% of stable keys |
| `TestSetGet_Integration` | Full end-to-end: real TCP server on random port, raw RESP bytes |
| `FuzzReadCommand` | Found unbounded-allocation DoS; fixed with length bounds |
| `BenchmarkSetSequential` | Baseline single-threaded Set throughput |
| `BenchmarkGetParallelHighContention` | Concurrent reads on one hot key: measures lock contention |

---

## Design Decisions & Trade-offs

**SHA-256 for ring hashing, FNV for shard selection.** The consistent hash ring faces potentially adversarial key inputs (any client can choose key names), so SHA-256's collision resistance prevents hash-flooding attacks that could concentrate load on one node. Internal shard selection is not user-facing, so the faster FNV-1a is appropriate there.

**Lazy expiration + background eviction.** Checking expiry only on read (`Get`, `Exists`) means expired keys are logically gone immediately but physically removed asynchronously. This avoids per-write overhead at the cost of memory not being reclaimed until either access or the next eviction sweep. Real Redis uses the same hybrid approach.

**AOF over RDB snapshots.** AOF provides finer-grained crash recovery (at most ~1 second of writes lost with `everysecond` policy) compared to periodic full snapshots. Trade-off: larger files, mitigated by `AOF.Rewrite` compaction.

**Coordinator as thin proxy (not smart client).** Routing logic lives in one place (the coordinator) rather than being embedded in every client library. Trade-off: one extra network hop per request; benefit: zero client-side configuration when cluster topology changes.

**Sequential scatter-gather for `DBSIZE`.** Currently queries nodes one at a time. A concurrent implementation with `sync.WaitGroup` would reduce latency from O(N nodes) to O(1).

**Zero external dependencies.** The entire project builds on Go's standard library. No supply chain risk, no version conflicts, no `go get` required beyond the module itself.

---

## Known Limitations & Future Work

- **`PERSIST` not propagated to replicas**: AOF logging for `PERSIST` is not yet implemented; replicas would diverge on TTL removal commands
- **No replica AOF**: replicas do not write their own AOF; crash recovery requires reconnecting to the primary for a full resync
- **Sequential `DBSIZE` aggregation**: parallelizing with `sync.WaitGroup` would improve latency for large clusters
- **No partial sync (PSYNC)**: replicas always do a full resync on reconnect; real Redis tracks a replication offset for efficient partial resync after brief disconnections
- **No AUTH**: the server accepts all connections without authentication; not suitable for public exposure without a reverse proxy or network-level access control
- **`FLUSHDB` is a stub**: returns `OK` without actually clearing data
- **`KEYS` is O(N)**: scans the entire keyspace; a cursor-based `SCAN` implementation would be the production-appropriate alternative

---

## Supported Commands

| Command | Syntax | Notes |
|---|---|---|
| `SET` | `SET key value [EX s] [PX ms] [NX\|XX]` | Full flag support |
| `GET` | `GET key` | Returns nil bulk string if missing |
| `DEL` | `DEL key [key ...]` | Returns count deleted |
| `EXISTS` | `EXISTS key [key ...]` | Counts occurrences, not distinct keys |
| `EXPIRE` | `EXPIRE key seconds` | Returns 1 if set, 0 if key missing |
| `TTL` | `TTL key` | Returns -1 (no TTL), -2 (missing), or seconds |
| `PERSIST` | `PERSIST key` | Removes TTL |
| `KEYS` | `KEYS pattern` | Glob matching via `path.Match` |
| `DBSIZE` | `DBSIZE` | Aggregated across cluster via coordinator |
| `PING` | `PING [message]` | Handled locally by coordinator |