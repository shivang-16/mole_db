package commands

import (
	"context"
	"strconv"
	"strings"
	"time"

	"mole/internal/core"
	"mole/internal/persistence/aof"
	"mole/internal/protocol/resp"
	"mole/internal/replication"
)

type Handler struct {
	store    core.Store
	readOnly bool
	master   *replication.Master // nil if not master
}

func NewHandler(store core.Store) *Handler {
	return &Handler{store: store}
}

// SetReadOnly marks the handler as read-only (for replicas).
func (h *Handler) SetReadOnly(readOnly bool) {
	h.readOnly = readOnly
}

// SetMaster sets the master for broadcasting writes (for masters).
func (h *Handler) SetMaster(master *replication.Master) {
	h.master = master
}

// Handle executes a parsed command and returns a RESP reply.
// Args are binary-safe byte slices.
func (h *Handler) Handle(ctx context.Context, args [][]byte) resp.Reply {
	if len(args) == 0 {
		return resp.Error("ERR empty command")
	}

	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case "PING":
		// PING or PING <message>
		if len(args) == 1 {
			return resp.SimpleString("PONG")
		}
		if len(args) == 2 {
			return resp.BulkString(args[1])
		}
		return resp.Error("ERR wrong number of arguments for 'PING'")

	case "SET":
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		// Supported:
		// - SET key value
		// - SET key value EX <seconds>
		// - SET key value PX <milliseconds>
		if len(args) != 3 && len(args) != 5 {
			return resp.Error("ERR wrong number of arguments for 'SET'")
		}
		key := string(args[1])
		val := args[2]

		var expireAtMs int64
		if len(args) == 3 {
			// Use default TTL (we'll need to get it from store or config).
			// For now, use a reasonable default (20 days).
			defaultTTL := 20 * 24 * time.Hour
			if err := h.store.Set(ctx, key, val); err != nil {
				return resp.Error("ERR " + err.Error())
			}
			expireAtMs = time.Now().Add(defaultTTL).UnixMilli()
		} else {
			unit := strings.ToUpper(string(args[3]))
			n, err := parsePositiveInt64Bytes(args[4])
			if err != nil {
				return resp.Error("ERR invalid expire time in SET")
			}
			var ttl time.Duration
			switch unit {
			case "EX":
				ttl = time.Duration(n) * time.Second
			case "PX":
				ttl = time.Duration(n) * time.Millisecond
			default:
				return resp.Error("ERR syntax error")
			}

			if err := h.store.SetWithTTL(ctx, key, val, ttl); err != nil {
				return resp.Error("ERR " + err.Error())
			}
			expireAtMs = time.Now().Add(ttl).UnixMilli()
		}

		// Broadcast to replicas.
		if h.master != nil {
			op := aof.RecordSetAt(key, val, expireAtMs)
			h.master.BroadcastOp(ctx, op)
		}

		return resp.SimpleString("OK")

	case "GET":
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'GET'")
		}
		v, ok, err := h.store.Get(ctx, string(args[1]))
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if !ok {
			return resp.NullBulkString{}
		}
		return resp.BulkString(v)

	case "DEL":
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'DEL'")
		}
		key := string(args[1])
		deleted, err := h.store.Del(ctx, key)
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if deleted {
			// Broadcast to replicas.
			if h.master != nil {
				op := aof.RecordDel(key)
				h.master.BroadcastOp(ctx, op)
			}
			return resp.Integer(1)
		}
		return resp.Integer(0)

	case "EXPIRE":
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) != 3 {
			return resp.Error("ERR wrong number of arguments for 'EXPIRE'")
		}
		key := string(args[1])
		secs, err := parsePositiveInt64Bytes(args[2])
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range")
		}
		ttl := time.Duration(secs) * time.Second
		updated, err := h.store.Expire(ctx, key, ttl)
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if updated {
			// Broadcast to replicas.
			if h.master != nil {
				expireAtMs := time.Now().Add(ttl).UnixMilli()
				op := aof.RecordExpireAt(key, expireAtMs)
				h.master.BroadcastOp(ctx, op)
			}
			return resp.Integer(1)
		}
		return resp.Integer(0)

	case "PEXPIRE":
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) != 3 {
			return resp.Error("ERR wrong number of arguments for 'PEXPIRE'")
		}
		key := string(args[1])
		ms, err := parsePositiveInt64Bytes(args[2])
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range")
		}
		ttl := time.Duration(ms) * time.Millisecond
		updated, err := h.store.Expire(ctx, key, ttl)
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if updated {
			// Broadcast to replicas.
			if h.master != nil {
				expireAtMs := time.Now().Add(ttl).UnixMilli()
				op := aof.RecordExpireAt(key, expireAtMs)
				h.master.BroadcastOp(ctx, op)
			}
			return resp.Integer(1)
		}
		return resp.Integer(0)

	case "TTL":
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'TTL'")
		}
		ttl, ok, err := h.store.TTL(ctx, string(args[1]))
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if !ok {
			return resp.Integer(-2)
		}
		// Redis TTL truncates toward zero seconds.
		return resp.Integer(int64(ttl / time.Second))

	case "PTTL":
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'PTTL'")
		}
		ttl, ok, err := h.store.TTL(ctx, string(args[1]))
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if !ok {
			return resp.Integer(-2)
		}
		return resp.Integer(int64(ttl / time.Millisecond))

	default:
		return resp.Error("ERR unknown command '" + cmd + "'")
	}
}

func parsePositiveInt64Bytes(b []byte) (int64, error) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil || n <= 0 {
		return 0, strconv.ErrRange
	}
	return n, nil
}
