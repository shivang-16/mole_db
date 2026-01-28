package commands

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"mole/internal/core"
	"mole/internal/persistence/aof"
	"mole/internal/protocol/resp"
	"mole/internal/pubsub"
	"mole/internal/replication"
)

type Handler struct {
	store     core.Store
	mu        sync.RWMutex // protects readOnly and master fields
	readOnly  bool
	master    *replication.Master // nil if not master
	pubsubMgr *pubsub.Manager
}

func NewHandler(store core.Store) *Handler {
	return &Handler{
		store:     store,
		pubsubMgr: pubsub.NewManager(),
	}
}

// GetPubSubManager returns the pub/sub manager.
func (h *Handler) GetPubSubManager() *pubsub.Manager {
	return h.pubsubMgr
}

// SetReadOnly marks the handler as read-only (for replicas).
func (h *Handler) SetReadOnly(readOnly bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.readOnly = readOnly
}

// SetMaster sets the master for broadcasting writes (for masters).
func (h *Handler) SetMaster(master *replication.Master) {
	h.mu.Lock()
	defer h.mu.Unlock()
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
		// Hold read lock for entire operation to ensure readOnly and master remain consistent.
		h.mu.RLock()
		defer h.mu.RUnlock()
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

		// Broadcast to replicas (best-effort, async replication).
		if h.master != nil {
			op := aof.RecordSetAt(key, val, expireAtMs)
			h.master.BroadcastOp(ctx, op)
			// Note: BroadcastOp doesn't return errors by design (async replication).
			// Failed replicas are automatically removed by the master.
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
		// Hold read lock for entire operation to ensure readOnly and master remain consistent.
		h.mu.RLock()
		defer h.mu.RUnlock()
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
			// Broadcast to replicas (best-effort, async replication).
			if h.master != nil {
				op := aof.RecordDel(key)
				h.master.BroadcastOp(ctx, op)
			}
			return resp.Integer(1)
		}
		return resp.Integer(0)

	case "EXPIRE":
		// Hold read lock for entire operation to ensure readOnly and master remain consistent.
		h.mu.RLock()
		defer h.mu.RUnlock()
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
			// Broadcast to replicas (best-effort, async replication).
			if h.master != nil {
				expireAtMs := time.Now().Add(ttl).UnixMilli()
				op := aof.RecordExpireAt(key, expireAtMs)
				h.master.BroadcastOp(ctx, op)
			}
			return resp.Integer(1)
		}
		return resp.Integer(0)

	case "PEXPIRE":
		// Hold read lock for entire operation to ensure readOnly and master remain consistent.
		h.mu.RLock()
		defer h.mu.RUnlock()
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
			// Broadcast to replicas (best-effort, async replication).
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
		// TTL truncates toward zero seconds.
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

	case "EXISTS":
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'EXISTS'")
		}
		key := string(args[1])
		exists, err := h.store.Exists(ctx, key)
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if exists {
			return resp.Integer(1)
		}
		return resp.Integer(0)

	case "INCR":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'INCR'")
		}
		key := string(args[1])
		newValue, err := h.store.Incr(ctx, key)
		if err != nil {
			return resp.Error(err.Error())
		}
		// Broadcast to replicas.
		if h.master != nil {
			// Send INCRBY command for replication (absolute value).
			op := [][]byte{[]byte("MOLE.SET"), []byte(key), []byte(strconv.FormatInt(newValue, 10))}
			h.master.BroadcastOp(ctx, op)
		}
		return resp.Integer(newValue)

	case "DECR":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'DECR'")
		}
		key := string(args[1])
		newValue, err := h.store.Decr(ctx, key)
		if err != nil {
			return resp.Error(err.Error())
		}
		// Broadcast to replicas.
		if h.master != nil {
			op := [][]byte{[]byte("MOLE.SET"), []byte(key), []byte(strconv.FormatInt(newValue, 10))}
			h.master.BroadcastOp(ctx, op)
		}
		return resp.Integer(newValue)

	case "INCRBY":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) != 3 {
			return resp.Error("ERR wrong number of arguments for 'INCRBY'")
		}
		key := string(args[1])
		delta, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range")
		}
		newValue, err := h.store.IncrBy(ctx, key, delta)
		if err != nil {
			return resp.Error(err.Error())
		}
		// Broadcast to replicas.
		if h.master != nil {
			op := [][]byte{[]byte("MOLE.SET"), []byte(key), []byte(strconv.FormatInt(newValue, 10))}
			h.master.BroadcastOp(ctx, op)
		}
		return resp.Integer(newValue)

	case "DECRBY":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) != 3 {
			return resp.Error("ERR wrong number of arguments for 'DECRBY'")
		}
		key := string(args[1])
		delta, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return resp.Error("ERR value is not an integer or out of range")
		}
		newValue, err := h.store.DecrBy(ctx, key, delta)
		if err != nil {
			return resp.Error(err.Error())
		}
		// Broadcast to replicas.
		if h.master != nil {
			op := [][]byte{[]byte("MOLE.SET"), []byte(key), []byte(strconv.FormatInt(newValue, 10))}
			h.master.BroadcastOp(ctx, op)
		}
		return resp.Integer(newValue)

	case "MSET":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		// MSET key1 value1 key2 value2 ...
		if len(args) < 3 || len(args)%2 == 0 {
			return resp.Error("ERR wrong number of arguments for 'MSET'")
		}
		pairs := make(map[string][]byte)
		for i := 1; i < len(args); i += 2 {
			key := string(args[i])
			value := args[i+1]
			pairs[key] = value
		}
		if err := h.store.MSet(ctx, pairs); err != nil {
			return resp.Error("ERR " + err.Error())
		}
		// Broadcast to replicas.
		if h.master != nil {
			for key, value := range pairs {
				expireAtMs := time.Now().Add(20 * 24 * time.Hour).UnixMilli()
				op := aof.RecordSetAt(key, value, expireAtMs)
				h.master.BroadcastOp(ctx, op)
			}
		}
		return resp.SimpleString("OK")

	case "MGET":
		// MGET key1 key2 key3 ...
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'MGET'")
		}
		keys := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			keys[i-1] = string(args[i])
		}
		result, err := h.store.MGet(ctx, keys)
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		// Return array of values (in order of requested keys).
		// If key doesn't exist, return null bulk string.
		values := make([]resp.Reply, len(keys))
		for i, key := range keys {
			if val, ok := result[key]; ok {
				values[i] = resp.BulkString(val)
			} else {
				values[i] = resp.NullBulkString{}
			}
		}
		return resp.Array(values)

	case "SETNX":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) != 3 {
			return resp.Error("ERR wrong number of arguments for 'SETNX'")
		}
		key := string(args[1])
		value := args[2]
		set, err := h.store.SetNX(ctx, key, value)
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if set && h.master != nil {
			expireAtMs := time.Now().Add(20 * 24 * time.Hour).UnixMilli()
			op := aof.RecordSetAt(key, value, expireAtMs)
			h.master.BroadcastOp(ctx, op)
		}
		if set {
			return resp.Integer(1)
		}
		return resp.Integer(0)

	case "SETEX":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) != 4 {
			return resp.Error("ERR wrong number of arguments for 'SETEX'")
		}
		key := string(args[1])
		seconds, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil || seconds <= 0 {
			return resp.Error("ERR invalid expire time in SETEX")
		}
		value := args[3]
		if err := h.store.SetEX(ctx, key, value, seconds); err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if h.master != nil {
			expireAtMs := time.Now().Add(time.Duration(seconds) * time.Second).UnixMilli()
			op := aof.RecordSetAt(key, value, expireAtMs)
			h.master.BroadcastOp(ctx, op)
		}
		return resp.SimpleString("OK")

	case "APPEND":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) != 3 {
			return resp.Error("ERR wrong number of arguments for 'APPEND'")
		}
		key := string(args[1])
		value := args[2]
		newLen, err := h.store.Append(ctx, key, value)
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		if h.master != nil {
			val, ok, err := h.store.Get(ctx, key)
			if err != nil {
				return resp.Error("ERR " + err.Error())
			}
			if ok {
				expireAtMs := time.Now().Add(20 * 24 * time.Hour).UnixMilli()
				op := aof.RecordSetAt(key, val, expireAtMs)
				h.master.BroadcastOp(ctx, op)
			}
		}
		return resp.Integer(newLen)

	case "STRLEN":
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'STRLEN'")
		}
		key := string(args[1])
		length, err := h.store.StrLen(ctx, key)
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		return resp.Integer(length)

	case "SCAN":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'SCAN'")
		}
		cursor, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return resp.Error("ERR invalid cursor")
		}
		pattern := "*"
		count := 10
		for i := 2; i < len(args); i += 2 {
			if i+1 >= len(args) {
				break
			}
			opt := strings.ToUpper(string(args[i]))
			if opt == "MATCH" {
				pattern = string(args[i+1])
			} else if opt == "COUNT" {
				c, err := strconv.Atoi(string(args[i+1]))
				if err != nil || c <= 0 {
					return resp.Error("ERR invalid COUNT value")
				}
				count = c
			}
		}
		nextCursor, keys, err := h.store.Scan(ctx, cursor, pattern, count)
		if err != nil {
			return resp.Error("ERR " + err.Error())
		}
		result := make([]resp.Reply, 2)
		result[0] = resp.BulkString([]byte(strconv.Itoa(nextCursor)))
		keyReplies := make([]resp.Reply, len(keys))
		for i, k := range keys {
			keyReplies[i] = resp.BulkString([]byte(k))
		}
		result[1] = resp.Array(keyReplies)
		return resp.Array(result)

	case "HSET":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) != 4 {
			return resp.Error("ERR wrong number of arguments for 'HSET'")
		}
		key := string(args[1])
		field := string(args[2])
		value := args[3]
		isNew, err := h.store.HSet(ctx, key, field, value)
		if err != nil {
			return resp.Error(err.Error())
		}
		if isNew {
			return resp.Integer(1)
		}
		return resp.Integer(0)

	case "HGET":
		if len(args) != 3 {
			return resp.Error("ERR wrong number of arguments for 'HGET'")
		}
		key := string(args[1])
		field := string(args[2])
		value, ok, err := h.store.HGet(ctx, key, field)
		if err != nil {
			return resp.Error(err.Error())
		}
		if !ok {
			return resp.NullBulkString{}
		}
		return resp.BulkString(value)

	case "HGETALL":
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'HGETALL'")
		}
		key := string(args[1])
		fields, err := h.store.HGetAll(ctx, key)
		if err != nil {
			return resp.Error(err.Error())
		}
		result := make([]resp.Reply, 0, len(fields)*2)
		for k, v := range fields {
			result = append(result, resp.BulkString([]byte(k)))
			result = append(result, resp.BulkString(v))
		}
		return resp.Array(result)

	case "HDEL":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) < 3 {
			return resp.Error("ERR wrong number of arguments for 'HDEL'")
		}
		key := string(args[1])
		fields := make([]string, len(args)-2)
		for i := 2; i < len(args); i++ {
			fields[i-2] = string(args[i])
		}
		count, err := h.store.HDel(ctx, key, fields)
		if err != nil {
			return resp.Error(err.Error())
		}
		return resp.Integer(count)

	case "HEXISTS":
		if len(args) != 3 {
			return resp.Error("ERR wrong number of arguments for 'HEXISTS'")
		}
		key := string(args[1])
		field := string(args[2])
		exists, err := h.store.HExists(ctx, key, field)
		if err != nil {
			return resp.Error(err.Error())
		}
		if exists {
			return resp.Integer(1)
		}
		return resp.Integer(0)

	case "HLEN":
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'HLEN'")
		}
		key := string(args[1])
		length, err := h.store.HLen(ctx, key)
		if err != nil {
			return resp.Error(err.Error())
		}
		return resp.Integer(length)

	case "LPUSH":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) < 3 {
			return resp.Error("ERR wrong number of arguments for 'LPUSH'")
		}
		key := string(args[1])
		values := args[2:]
		length, err := h.store.LPush(ctx, key, values)
		if err != nil {
			return resp.Error(err.Error())
		}
		return resp.Integer(length)

	case "RPUSH":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) < 3 {
			return resp.Error("ERR wrong number of arguments for 'RPUSH'")
		}
		key := string(args[1])
		values := args[2:]
		length, err := h.store.RPush(ctx, key, values)
		if err != nil {
			return resp.Error(err.Error())
		}
		return resp.Integer(length)

	case "LPOP":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'LPOP'")
		}
		key := string(args[1])
		value, ok, err := h.store.LPop(ctx, key)
		if err != nil {
			return resp.Error(err.Error())
		}
		if !ok {
			return resp.NullBulkString{}
		}
		return resp.BulkString(value)

	case "RPOP":
		h.mu.RLock()
		defer h.mu.RUnlock()
		if h.readOnly {
			return resp.Error("READONLY You can't write against a read only replica.")
		}
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'RPOP'")
		}
		key := string(args[1])
		value, ok, err := h.store.RPop(ctx, key)
		if err != nil {
			return resp.Error(err.Error())
		}
		if !ok {
			return resp.NullBulkString{}
		}
		return resp.BulkString(value)

	case "LLEN":
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'LLEN'")
		}
		key := string(args[1])
		length, err := h.store.LLen(ctx, key)
		if err != nil {
			return resp.Error(err.Error())
		}
		return resp.Integer(length)

	case "LRANGE":
		if len(args) != 4 {
			return resp.Error("ERR wrong number of arguments for 'LRANGE'")
		}
		key := string(args[1])
		start, err1 := strconv.ParseInt(string(args[2]), 10, 64)
		stop, err2 := strconv.ParseInt(string(args[3]), 10, 64)
		if err1 != nil || err2 != nil {
			return resp.Error("ERR value is not an integer or out of range")
		}
		values, err := h.store.LRange(ctx, key, start, stop)
		if err != nil {
			return resp.Error(err.Error())
		}
		result := make([]resp.Reply, len(values))
		for i, v := range values {
			result[i] = resp.BulkString(v)
		}
		return resp.Array(result)

	case "PUBLISH", "PUB", "MOLE.PUB", "MOLE.PUBLISH":
		if len(args) != 3 {
			return resp.Error("ERR wrong number of arguments for 'PUB'")
		}
		channel := string(args[1])
		message := args[2]
		count := h.pubsubMgr.Publish(ctx, channel, message)
		return resp.Integer(count)

	case "PUBSUB", "MOLE.PUBSUB":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'PUBSUB'")
		}
		subCmd := strings.ToUpper(string(args[1]))
		switch subCmd {
		case "CHANNELS":
			pattern := "*"
			if len(args) >= 3 {
				pattern = string(args[2])
			}
			channels := h.pubsubMgr.GetChannels(pattern)
			result := make([]resp.Reply, len(channels))
			for i, ch := range channels {
				result[i] = resp.BulkString([]byte(ch))
			}
			return resp.Array(result)

		case "NUMSUB":
			if len(args) < 3 {
				return resp.Array([]resp.Reply{})
			}
			channels := make([]string, len(args)-2)
			for i := 2; i < len(args); i++ {
				channels[i-2] = string(args[i])
			}
			counts := h.pubsubMgr.GetNumSub(channels...)
			result := make([]resp.Reply, 0, len(channels)*2)
			for _, ch := range channels {
				result = append(result, resp.BulkString([]byte(ch)))
				result = append(result, resp.Integer(counts[ch]))
			}
			return resp.Array(result)

		case "NUMPAT":
			count := h.pubsubMgr.GetNumPat()
			return resp.Integer(count)

		default:
			return resp.Error("ERR unknown PUBSUB subcommand '" + subCmd + "'")
		}

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
