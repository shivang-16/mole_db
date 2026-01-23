package commands

import (
	"context"
	"strconv"
	"strings"
	"time"

	"mole/internal/core"
	"mole/internal/protocol/resp"
)

type Handler struct {
	store core.Store
}

func NewHandler(store core.Store) *Handler {
	return &Handler{store: store}
}

// Handle executes a parsed command and returns a RESP reply.
func (h *Handler) Handle(ctx context.Context, args []string) resp.Reply {
	if len(args) == 0 {
		return resp.Error("ERR empty command")
	}

	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "PING":
		// PING or PING <message>
		if len(args) == 1 {
			return resp.SimpleString("PONG")
		}
		if len(args) == 2 {
			return resp.BulkString([]byte(args[1]))
		}
		return resp.Error("ERR wrong number of arguments for 'PING'")

	case "SET":
		// Supported:
		// - SET key value
		// - SET key value EX <seconds>
		// - SET key value PX <milliseconds>
		if len(args) != 3 && len(args) != 5 {
			return resp.Error("ERR wrong number of arguments for 'SET'")
		}
		key := args[1]
		val := []byte(args[2])

		if len(args) == 3 {
			if err := h.store.Set(ctx, key, val); err != nil {
				return resp.Error("ERR " + err.Error())
			}
			return resp.SimpleString("OK")
		}

		unit := strings.ToUpper(args[3])
		n, err := parsePositiveInt64(args[4])
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
		return resp.SimpleString("OK")

	case "GET":
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'GET'")
		}
		v, ok, err := h.store.Get(ctx, args[1])
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if !ok {
			return resp.NullBulkString{}
		}
		return resp.BulkString(v)

	case "DEL":
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'DEL'")
		}
		deleted, err := h.store.Del(ctx, args[1])
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if deleted {
			return resp.Integer(1)
		}
		return resp.Integer(0)

	case "EXPIRE":
		if len(args) != 3 {
			return resp.Error("ERR wrong number of arguments for 'EXPIRE'")
		}
		secs, err := parsePositiveInt64(args[2])
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range")
		}
		updated, err := h.store.Expire(ctx, args[1], time.Duration(secs)*time.Second)
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if updated {
			return resp.Integer(1)
		}
		return resp.Integer(0)

	case "PEXPIRE":
		if len(args) != 3 {
			return resp.Error("ERR wrong number of arguments for 'PEXPIRE'")
		}
		ms, err := parsePositiveInt64(args[2])
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range")
		}
		updated, err := h.store.Expire(ctx, args[1], time.Duration(ms)*time.Millisecond)
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if updated {
			return resp.Integer(1)
		}
		return resp.Integer(0)

	case "TTL":
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'TTL'")
		}
		ttl, ok, err := h.store.TTL(ctx, args[1])
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
		ttl, ok, err := h.store.TTL(ctx, args[1])
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

func parsePositiveInt64(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0, strconv.ErrRange
	}
	return n, nil
}
