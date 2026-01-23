# Mole

Mole is a fast, in-memory key-value server written in Go.

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

Supported commands today: `PING`, `SET`, `GET`, `DEL` (TTL is the next milestone).

## TTL (cache semantics)

Mole is a cache-first server. In the current TTL milestone:
- `SET key value` applies a **default TTL** (20 days).
- `SET key value EX <seconds>` / `PX <milliseconds>` sets a custom TTL, but it is **capped** at 20 days.
- Expired keys are deleted via:
  - **Passive expiry** (on `GET`, `TTL`, etc.)
  - **Active expiry** (a background janitor loop)

Supported TTL commands: `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`.

## Project Structure

- `cmd/mole/`: Mole server binary entrypoint.
- `internal/config/`: Runtime configuration and defaults.
- `internal/protocol/resp/`: RESP reader/writer + reply types.
- `internal/core/`: Storage interfaces and implementations (in-memory today).
- `internal/commands/`: Command routing/execution.
- `internal/server/`: TCP listener and per-connection loops.
