# Mole

Fast, in-memory key-value cache server written in Go. RESP-compatible protocol with built-in replication and high availability.

## Features

- **In-memory storage** with sharded hash maps for low latency
- **TTL-based expiration** (default 20 days, max 20 days)
- **LRU eviction** with configurable memory limits
- **AOF persistence** for warm restarts
- **Master-replica replication** for high availability
- **Sentinel failover** for automatic master promotion
- **Binary-safe** values
- **29 commands** including strings, hashes, lists, counters

## Quick Start

```bash
# Start server (default port 7379)
./mole

# With custom config
./mole -addr 127.0.0.1:7379 -maxmemory 1073741824 -aof true

# Connect with mole-cli (or any RESP client)
any-resp-client -p 7379
```

## Commands

### Basic Operations
- `PING` - Test connection
- `SET key value [EX seconds] [PX milliseconds]` - Set key
- `GET key` - Get key
- `DEL key` - Delete key
- `EXISTS key` - Check if key exists

### TTL Management
- `EXPIRE key seconds` - Set expiration
- `PEXPIRE key milliseconds` - Set expiration (ms)
- `TTL key` - Get remaining TTL (seconds)
- `PTTL key` - Get remaining TTL (milliseconds)

### Counters
- `INCR key` - Increment by 1
- `DECR key` - Decrement by 1
- `INCRBY key delta` - Increment by delta
- `DECRBY key delta` - Decrement by delta

### Batch Operations
- `MSET key1 val1 key2 val2 ...` - Set multiple keys
- `MGET key1 key2 ...` - Get multiple keys

### Conditional Set
- `SETNX key value` - Set if not exists (returns 1 if set)
- `SETEX key seconds value` - Set with expiration

### String Operations
- `APPEND key value` - Append to key
- `STRLEN key` - Get string length

### Key Iteration
- `SCAN cursor [MATCH pattern] [COUNT count]` - Iterate keys

### Hash Operations
- `HSET key field value` - Set hash field
- `HGET key field` - Get hash field
- `HGETALL key` - Get all hash fields
- `HDEL key field [field ...]` - Delete hash fields
- `HEXISTS key field` - Check if field exists
- `HLEN key` - Get hash field count

### List Operations
- `LPUSH key value [value ...]` - Push to list head
- `RPUSH key value [value ...]` - Push to list tail
- `LPOP key` - Pop from list head
- `RPOP key` - Pop from list tail
- `LLEN key` - Get list length
- `LRANGE key start stop` - Get list range

## Architecture

```
┌─────────────────────────────────────┐
│     TCP Server (Port 7379)          │
├─────────────────────────────────────┤
│  RESP Protocol Parser/Writer        │
├─────────────────────────────────────┤
│  Command Handler                    │
├─────────────────────────────────────┤
│  Sharded Memory Store (256 shards)  │
│  ├─ Strings                         │
│  ├─ Hashes                          │
│  └─ Lists                           │
├─────────────────────────────────────┤
│  AOF Persistence (optional)         │
├─────────────────────────────────────┤
│  Replication (Master/Replica)       │
└─────────────────────────────────────┘
```

## Configuration

```bash
-addr string              # TCP address (default "127.0.0.1:7379")
-default-ttl duration     # Default TTL (default 480h = 20 days)
-max-ttl duration         # Max TTL cap (default 480h = 20 days)
-maxmemory int            # Max memory in bytes (0 = no limit)
-maxmemory-policy string  # Eviction policy: noeviction|allkeys-lru
-aof                      # Enable AOF persistence
-aof-path string          # AOF file path (default "mole.aof")
-aof-fsync string         # Sync policy: always|everysec|no
-role string              # Server role: master|replica
-master-addr string       # Master address (for replicas)
```

## Replication

### Master-Replica Setup
```bash
# Start master
./mole -role master -addr 127.0.0.1:7379

# Start replica
./mole -role replica -master-addr 127.0.0.1:7379 -addr 127.0.0.1:7380
```

### Sentinel Failover
```bash
# Start sentinel
./sentinel -id s1 -master 127.0.0.1:7379 \
  -replicas 127.0.0.1:7380,127.0.0.1:7381 \
  -peers 127.0.0.1:9001,127.0.0.1:9002
```

## Use Cases

- **Session storage** - Fast user session cache
- **Rate limiting** - Counter-based API throttling
- **Leaderboards** - Sorted rankings
- **Real-time analytics** - Event counting
- **Cache layer** - Database query caching
- **Message queues** - List-based queues

## Performance

- **Sharded storage** - 256 shards for concurrent access
- **O(1) operations** - Most commands are constant time
- **Low latency** - In-memory operations
- **Batch operations** - MGET/MSET reduce round-trips by 10x

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup and development guide.

## License

MIT License - See [LICENSE](LICENSE) for details.

## Project Status

⚠️ **Beta** - Production-ready for caching use cases. Not recommended for persistent data storage.
