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
- **Pub/Sub messaging** with pattern matching for real-time events

## Installation

### Via npm (Recommended)

```bash
npm install -g moledb
```

This installs pre-compiled binaries for your platform (macOS, Linux, Windows).

### From source

```bash
git clone https://github.com/shivang-16/mole_db.git
cd mole_db
go build -o mole ./cmd/mole
go build -o mole ./cmd/mole
```

## Quick Start

### Interactive Mode (Easiest)
```bash
# Install via npm
npm install -g moledb

# Start interactive mode (server + client in one)
mole
# OR
mole server
127.0.0.1:7379> SET mykey "Hello World"
OK
127.0.0.1:7379> GET mykey
"Hello World"
```

### Daemon Mode
```bash
# Start daemon server (background)
mole server -d
# OR
mole -d

# Connect to running server
mole connect
127.0.0.1:7379> SET mykey "Hello World"
OK
```

### Smart Connect
```bash
# Try to connect to existing server, fallback to interactive mode
mole connect
# If server exists: connects as client
# If no server: starts interactive mode
```

### From Source
```bash
git clone https://github.com/shivang-16/mole_db.git
cd mole_db
go build -o mole ./cmd/mole
./mole
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

### Pub/Sub Messaging
- `MOLE.PUB channel message` - Publish message to channel
- `MOLE.SUB channel [channel ...]` - Subscribe to channels
- `MOLE.PSUB pattern [pattern ...]` - Subscribe to patterns (supports `*` and `?`)
- `MOLE.UNSUB [channel ...]` - Unsubscribe from channels
- `MOLE.PUNSUB [pattern ...]` - Unsubscribe from patterns
- `MOLE.PUBSUB CHANNELS [pattern]` - List active channels
- `MOLE.PUBSUB NUMSUB [channel ...]` - Get subscriber count per channel
- `MOLE.PUBSUB NUMPAT` - Get total pattern subscriptions

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
│  Pub/Sub Manager (Zero-Copy)        │
│  ├─ Channel Subscriptions           │
│  └─ Pattern Matching (Trie-based)   │
├─────────────────────────────────────┤
│  AOF Persistence (optional)         │
├─────────────────────────────────────┤
│  Replication (Master/Replica)       │
└─────────────────────────────────────┘
```

## Configuration

### Server Options
```bash
mole server [options]

-addr string              # TCP address (default "127.0.0.1:7379")
-d                        # Run in daemon mode (background)
-aof                      # Enable AOF persistence
-aof-path string          # AOF file path (default "mole.aof")
-maxmemory int            # Max memory in bytes (0 = no limit)
-maxmemory-policy string  # Eviction policy: noeviction|allkeys-lru
-role string              # Server role: master|replica
-master-addr string       # Master address (for replicas)
```

### Connect Options
```bash
mole connect [options]

-h string                 # Server host (default "127.0.0.1")
-p int                    # Server port (default 7379)
```

## Replication

### Master-Replica Setup
```bash
# Start master
mole server -role master -addr 127.0.0.1:7379

# Start replica
mole server -role replica -master-addr 127.0.0.1:7379 -addr 127.0.0.1:7380
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
- **Pub/Sub messaging** - Real-time notifications and chat systems

## Pub/Sub Examples

### Terminal 1 - Subscribe to channels
```bash
mole connect
127.0.0.1:7379> SUB news sports
Subscribed to news (total: 1)
Subscribed to sports (total: 2)
Entering subscription mode. Press Ctrl+C to exit.
```

### Terminal 2 - Publish messages
```bash
mole connect
127.0.0.1:7379> PUB news "Breaking: Mole DB released!"
(integer) 1
127.0.0.1:7379> PUB sports "Game starts at 3pm"
(integer) 1
```

### Terminal 1 - Receives messages
```
[14:30:15] news: Breaking: Mole DB released!
[14:30:20] sports: Game starts at 3pm
```

### Pattern Subscriptions
```bash
# Subscribe to all user events
127.0.0.1:7379> MOLE.PSUB user:*
Subscribed to user:* (total: 1)

# Publish to specific user
127.0.0.1:7379> MOLE.PUB user:123 "logged in"
(integer) 1  # Delivered to pattern subscriber
```

## Performance

- **Sharded storage** - 256 shards for concurrent access
- **O(1) operations** - Most commands are constant time
- **Low latency** - In-memory operations
- **Batch operations** - MGET/MSET reduce round-trips by 10x
- **Zero-copy pub/sub** - Message delivery via pointer sharing
- **Non-blocking publishes** - Slow subscribers don't affect publishers
- **Pattern matching** - O(1) trie-based channel matching

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup and development guide.

## License

MIT License - See [LICENSE](LICENSE) for details.

## Project Status

⚠️ **Beta** - Production-ready for caching use cases. Not recommended for persistent data storage.
