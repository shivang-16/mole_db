# Contributing to Mole

## Setup

### Prerequisites
- Go 1.22 or higher
- Git

### Clone & Build
```bash
git clone <repository-url>
cd mole_db

# Build main server
go build -o mole ./cmd/mole

# Build sentinel (optional)
go build -o sentinel ./cmd/sentinel
```

### Run Tests
```bash
go test ./...
```

## Running Mole

### Basic Server
```bash
# Default configuration
./mole

# Custom port and memory limit
./mole -addr 127.0.0.1:7379 -maxmemory 1073741824

# With AOF persistence
./mole -aof true -aof-path data/mole.aof -aof-fsync everysec
```

### Development Mode
```bash
# Run with verbose logging
./mole -addr 127.0.0.1:7379

# In another terminal, test with redis-cli
redis-cli -p 7379
127.0.0.1:7379> SET test hello
OK
127.0.0.1:7379> GET test
"hello"
```

### Testing with netcat
```bash
# SET command
printf "*3\r\n\$3\r\nSET\r\n\$4\r\nname\r\n\$5\r\nalice\r\n" | nc 127.0.0.1 7379

# GET command
printf "*2\r\n\$3\r\nGET\r\n\$4\r\nname\r\n" | nc 127.0.0.1 7379
```

## Replication Setup

### Master-Replica
```bash
# Terminal 1: Start master
./mole -role master -addr 127.0.0.1:7379

# Terminal 2: Start replica
./mole -role replica -master-addr 127.0.0.1:7379 -addr 127.0.0.1:7380

# Test replication
redis-cli -p 7379 SET key value   # Write to master
redis-cli -p 7380 GET key         # Read from replica
```

### Sentinel Failover
```bash
# Terminal 1: Master
./mole -role master -addr 127.0.0.1:7379

# Terminal 2: Replica 1
./mole -role replica -master-addr 127.0.0.1:7379 -addr 127.0.0.1:7380

# Terminal 3: Replica 2
./mole -role replica -master-addr 127.0.0.1:7379 -addr 127.0.0.1:7381

# Terminal 4: Sentinel 1
./sentinel -id s1 -master 127.0.0.1:7379 \
  -replicas 127.0.0.1:7380,127.0.0.1:7381 \
  -peers 127.0.0.1:9002 -addr 127.0.0.1:9001

# Terminal 5: Sentinel 2
./sentinel -id s2 -master 127.0.0.1:7379 \
  -replicas 127.0.0.1:7380,127.0.0.1:7381 \
  -peers 127.0.0.1:9001 -addr 127.0.0.1:9002

# Test failover: Kill master (Ctrl+C in Terminal 1)
# Sentinels will detect failure and promote a replica
```

## Project Structure

```
mole_db/
├── cmd/
│   ├── mole/          # Main server entry point
│   └── sentinel/      # Sentinel entry point
├── internal/
│   ├── commands/      # Command handlers
│   ├── config/        # Configuration
│   ├── core/          # Storage engine
│   ├── persistence/   # AOF implementation
│   │   └── aof/
│   ├── protocol/      # RESP protocol
│   │   └── resp/
│   ├── replication/   # Master-replica logic
│   ├── sentinel/      # Sentinel failover
│   └── server/        # TCP server
└── README.md
```

## Code Style

- One comment per function (above declaration)
- No inline comments unless critical
- Use `gofmt` for formatting
- Follow Go naming conventions

### Example
```go
// HSet sets a hash field value.
func (s *MemoryStore) HSet(ctx context.Context, key, field string, value []byte) (bool, error) {
    // Implementation
}
```

## Development Workflow

### Adding a New Command

1. **Update Store interface** (`internal/core/store.go`)
```go
// MyCommand does something.
MyCommand(ctx context.Context, key string) error
```

2. **Implement in MemoryStore** (`internal/core/memory_store.go`)
```go
func (s *MemoryStore) MyCommand(ctx context.Context, key string) error {
    // Implementation
}
```

3. **Add AOF wrapper** (`internal/persistence/aof/store.go`)
```go
func (s *LoggingStore) MyCommand(ctx context.Context, key string) error {
    return s.underlying.MyCommand(ctx, key)
}
```

4. **Add command handler** (`internal/commands/handler.go`)
```go
case "MYCOMMAND":
    if len(args) != 2 {
        return resp.Error("ERR wrong number of arguments")
    }
    // Handle command
```

5. **Test**
```bash
go build -o mole ./cmd/mole
./mole
redis-cli -p 7379 MYCOMMAND key
```

## Testing

### Unit Tests
```bash
# Run all tests
go test ./...

# Run specific package
go test ./internal/core

# With coverage
go test -cover ./...
```

### Manual Testing
```bash
# Start server in one terminal
./mole

# Test in another terminal
redis-cli -p 7379
> SET test value
> GET test
> HSET user:1 name alice
> HGETALL user:1
```

## Common Issues

### Port already in use
```bash
# Find process using port 7379
lsof -i :7379

# Kill process
kill -9 <PID>
```

### Permission denied
```bash
# Make binary executable
chmod +x mole sentinel
```

### Module issues
```bash
# Tidy dependencies
go mod tidy
```

## Performance Testing

### Benchmark with redis-benchmark
```bash
# Install redis-benchmark (comes with redis-tools)
redis-benchmark -p 7379 -t set,get -n 100000 -q
```

## Debugging

### Enable verbose output
```bash
# Add logging in code
fmt.Printf("Debug: key=%s, value=%v\n", key, value)
```

### Check memory usage
```bash
# View process memory
ps aux | grep mole

# macOS specific
top -pid <PID>
```

## Pull Request Guidelines

1. **Branch naming**: `feature/command-name` or `fix/issue-description`
2. **Commit messages**: Clear, concise (e.g., "Add HGETALL command")
3. **Testing**: Test your changes manually
4. **Code style**: Run `gofmt -w .` before committing
5. **Keep it simple**: Follow existing patterns

## Questions?

Open an issue or discussion on GitHub.
