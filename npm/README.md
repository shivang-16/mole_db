# Mole DB

Fast, lightweight in-memory database with Redis-compatible protocol.

## Features

- **Redis-compatible protocol** - Use existing Redis clients
- **Persistence** - AOF (Append-Only File) support
- **Replication** - Master-replica setup
- **Sentinel** - High availability monitoring
- **TTL support** - Automatic key expiration
- **Memory management** - LRU eviction policies
- **Fast** - Built in Go for performance

## Installation

```bash
npm install -g moledb
```

## Quick Start

### Start the server

```bash
mole
```

Server starts on `127.0.0.1:7379` by default.

### Connect with CLI

```bash
mole-cli
127.0.0.1:7379> SET mykey "Hello"
OK
127.0.0.1:7379> GET mykey
"Hello"
```

## Server Options

```bash
mole [options]

Options:
  -addr string
        TCP address to listen on (default "127.0.0.1:7379")
  -aof
        Enable AOF persistence
  -aof-path string
        AOF file path (default "./mole.aof")
  -aof-fsync string
        AOF fsync policy: always|everysec|no (default "everysec")
  -maxmemory int
        Max memory in bytes (0 = no limit)
  -maxmemory-policy string
        Eviction policy: noeviction|allkeys-lru (default "noeviction")
  -role string
        Server role: master|replica (default "master")
  -master-addr string
        Master address (required for replica)
```

### Examples

Start with persistence:
```bash
mole -aof -aof-path ./data/mole.aof
```

Start replica:
```bash
mole -role replica -master-addr 127.0.0.1:7379 -addr 127.0.0.1:7380
```

## CLI Usage

Interactive mode:
```bash
mole-cli
```

Single command:
```bash
mole-cli SET key value
mole-cli GET key
```

Connect to different host:
```bash
mole-cli -h 192.168.1.100 -p 7379
```

## Supported Commands

- `GET`, `SET`, `DEL`, `EXISTS`
- `EXPIRE`, `TTL`
- `INCR`, `DECR`
- `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`
- `SADD`, `SMEMBERS`, `SISMEMBER`
- `HSET`, `HGET`, `HGETALL`
- `KEYS`, `PING`, `INFO`
- And more...

## Use with Node.js

```javascript
const redis = require('redis');
const client = redis.createClient({ port: 7379 });

await client.connect();
await client.set('key', 'value');
const value = await client.get('key');
console.log(value); // 'value'
```

## License

MIT
