# Mole

Mole is a fast, in-memory key-value server written in Go.

## Key Features

- **Binary-safe**: stores arbitrary byte sequences (images, protobuf, encrypted data, etc.)
- **Sharded hash map**: low-latency concurrent access via 64 shards
- **TTL-first caching**: automatic expiration (passive + active janitor)
- **Memory eviction**: LRU-based eviction with configurable maxmemory limit
- **AOF persistence**: optional append-only file for warm restarts
- **Master/replica replication**: async replication with read-only replicas
- **Sentinel-style failover**: quorum-based automatic failover (basic implementation)
- **RESP-compatible**: works with existing Redis clients in any language

## Quick start

Mole runs as a **TCP server** and speaks a **RESP-compatible protocol** (Redis protocol family), so it can be used from **any language** via Redis client libraries.

Run the server:

```bash
go run ./cmd/mole -addr 127.0.0.1:7379
```

Talk to it via `nc` (inline commands are supported for dev convenience):

```bash
printf "PING\r\n" | nc 127.0.0.1 7379
printf "SET hello world\r\n" | nc 127.0.0.1 7379
printf "GET hello\r\n" | nc 127.0.0.1 7379
printf "DEL hello\r\n" | nc 127.0.0.1 7379
```

Expected responses are RESP-like:
- `+PONG` for `PING`
- `+OK` for `SET`
- `$5\r\nworld` for `GET hello`
- `:1` for `DEL hello` (integer replies)

Supported commands today: `PING`, `SET`, `GET`, `DEL`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`.

## TTL (cache semantics)

Mole is a cache-first server. In the current TTL milestone:
- `SET key value` applies a **default TTL** (20 days).
- `SET key value EX <seconds>` / `PX <milliseconds>` sets a custom TTL, but it is **capped** at 20 days.
- Expired keys are deleted via:
  - **Passive expiry** (on `GET`, `TTL`, etc.)
  - **Active expiry** (a background janitor loop)

Supported TTL commands: `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`.

## Memory eviction (maxmemory + LRU)

Mole can enforce a memory limit and automatically evict keys when full.

Enable eviction (example: 1GB limit with LRU policy):

```bash
go run ./cmd/mole -maxmemory 1073741824 -maxmemory-policy allkeys-lru
```

Supported policies:
- **`noeviction`**: refuse writes when memory limit reached (returns OOM error)
- **`allkeys-lru`**: evict least-recently-used keys (approximated, sample-based)

How LRU works:
- Each key tracks `lastAccessedAt` (updated on `GET`)
- On eviction, Mole samples ~5 random keys and evicts the oldest one
- Repeats until enough space is freed

## AOF persistence (optional)

Mole can optionally persist writes to an **AOF (append-only file)** so the in-memory dataset can be rebuilt after a restart.

Enable AOF:

```bash
go run ./cmd/mole -aof -aof-path ./mole.aof -aof-fsync everysec
```

Quick demo:

```bash
printf \"SET a b EX 60\\r\\n\" | nc 127.0.0.1 7379
# stop Mole, start it again with the same -aof-path, then:
printf \"GET a\\r\\n\" | nc 127.0.0.1 7379
```

## Master/replica replication

Mole supports **master-replica replication** where replicas asynchronously replicate writes from the master.

### Running a master:

```bash
go run ./cmd/mole -role master -addr 127.0.0.1:7379
```

### Running a replica:

```bash
go run ./cmd/mole -role replica -master-addr 127.0.0.1:7379 -addr 127.0.0.1:7380
```

**How it works:**
- Replica connects to master and performs **full sync** (snapshot of all keys)
- Master **broadcasts** every write operation (`SET`, `DEL`, `EXPIRE`) to all replicas
- Replicas apply operations to their local store
- Replicas are **read-only** for client connections (reject `SET`/`DEL`/`EXPIRE`)

**Replication format:**
- Uses same `MOLE.*` operation format as AOF
- Includes absolute `expireAtMs` timestamps for TTL correctness

## Sentinel-style failover (basic)

Mole includes a basic **Sentinel** implementation for automatic failover.

### Running Sentinels:

```bash
# Sentinel 1
go run ./cmd/sentinel -id sentinel-1 -master 127.0.0.1:7379 -replicas "127.0.0.1:7380" -peers "127.0.0.1:26379,127.0.0.1:26380"

# Sentinel 2
go run ./cmd/sentinel -id sentinel-2 -master 127.0.0.1:7379 -replicas "127.0.0.1:7380" -peers "127.0.0.1:26379,127.0.0.1:26380"
```

**How it works:**
- Sentinels monitor master health (heartbeat)
- On master failure, Sentinels use **quorum voting** (majority must agree)
- Elect best replica to promote
- Update all nodes to follow new master

**Note:** The current implementation is basic. A production system would need:
- Proper sentinel-to-sentinel protocol
- Replica promotion mechanism
- Client discovery API

## Project Structure

- `cmd/mole/`: Mole server binary entrypoint.
- `cmd/sentinel/`: Sentinel coordinator binary for failover.
- `internal/config/`: Runtime configuration and defaults.
- `internal/protocol/resp/`: RESP reader/writer + reply types.
- `internal/core/`: Storage interfaces and implementations (sharded in-memory).
- `internal/commands/`: Command routing/execution.
- `internal/server/`: TCP listener and per-connection loops.
- `internal/replication/`: Master/replica replication logic.
- `internal/sentinel/`: Sentinel-style failover coordination.
- `internal/persistence/aof/`: AOF persistence implementation.
