package core

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryStore is a concurrency-safe in-memory hash map store with eviction.
//
// Implementation notes:
// - Data is sharded across N maps to reduce lock contention under concurrent load.
// - Each entry stores an absolute expireAt timestamp (unix millis) and lastAccessedAt.
// - Expired keys are removed via passive expiry (on access) and an active janitor.
// - When maxmemory is reached, keys are evicted using approximated LRU (sample-based).
type MemoryStore struct {
	shards []shard

	defaultTTL time.Duration
	maxTTL     time.Duration
	now        func() time.Time

	// Memory tracking + eviction
	usedMemory int64  // atomic counter (approximate bytes)
	maxMemory  int64  // 0 = no limit
	policy     string // eviction policy (only "allkeys-lru" supported for now)
}

type MemoryStoreOptions struct {
	DefaultTTL time.Duration
	MaxTTL     time.Duration
	Shards     int // number of shards for concurrent access (defaults to 64)

	MaxMemory int64  // max memory in bytes (0 = no limit)
	Policy    string // eviction policy: "noeviction", "allkeys-lru" (defaults to "allkeys-lru")
}

func NewMemoryStore(opts MemoryStoreOptions) *MemoryStore {
	if opts.DefaultTTL <= 0 {
		opts.DefaultTTL = 20 * 24 * time.Hour
	}
	if opts.MaxTTL <= 0 {
		opts.MaxTTL = 20 * 24 * time.Hour
	}
	if opts.DefaultTTL > opts.MaxTTL {
		opts.DefaultTTL = opts.MaxTTL
	}
	if opts.Shards <= 0 {
		opts.Shards = 64
	}
	if opts.Policy == "" {
		opts.Policy = "allkeys-lru"
	}

	s := &MemoryStore{
		shards:     make([]shard, opts.Shards),
		defaultTTL: opts.DefaultTTL,
		maxTTL:     opts.MaxTTL,
		now:        time.Now,
		maxMemory:  opts.MaxMemory,
		policy:     opts.Policy,
	}
	for i := range s.shards {
		s.shards[i].kv = make(map[string]entry)
	}
	return s
}

type entry struct {
	value          []byte
	expireAtMs     int64 // unix millis; 0 means no expiry
	lastAccessedAt int64 // unix seconds (for LRU)
	typ            string // "string", "hash", "list"
	hash           map[string][]byte
	list           [][]byte
}

type shard struct {
	mu sync.RWMutex
	kv map[string]entry
}

func (s *MemoryStore) shardFor(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	idx := int(h.Sum32()) % len(s.shards)
	return &s.shards[idx]
}

func (s *MemoryStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	nowMs := s.now().UnixMilli()
	nowSec := nowMs / 1000
	sh := s.shardFor(key)

	// Start with read lock for the common case (key exists, not expired).
	sh.mu.RLock()
	e, ok := sh.kv[key]
	if !ok {
		sh.mu.RUnlock()
		return nil, false, nil
	}

	// Check if expired.
	if e.expireAtMs > 0 && nowMs >= e.expireAtMs {
		// Need to delete - upgrade to write lock.
		sh.mu.RUnlock()
		sh.mu.Lock()
		// Recalculate time after acquiring write lock (TOCTOU protection).
		nowMs = s.now().UnixMilli()
		nowSec = nowMs / 1000
		// Re-check under write lock (key might have been deleted/modified).
		e2, ok2 := sh.kv[key]
		if !ok2 {
			sh.mu.Unlock()
			return nil, false, nil
		}
		if e2.expireAtMs > 0 && nowMs >= e2.expireAtMs {
			// Still expired - delete it.
			size := entrySize(key, e2.value)
			delete(sh.kv, key)
			sh.mu.Unlock()
			atomic.AddInt64(&s.usedMemory, -size)
			return nil, false, nil
		}
		// Key was updated (expiry extended) - update LRU with current entry.
		e = e2
	} else {
		// Not expired - upgrade to write lock to update LRU.
		sh.mu.RUnlock()
		sh.mu.Lock()
		// Recalculate time after acquiring write lock (TOCTOU protection).
		nowMs = s.now().UnixMilli()
		nowSec = nowMs / 1000
		// Re-check key still exists (might have been deleted).
		e2, ok2 := sh.kv[key]
		if !ok2 {
			sh.mu.Unlock()
			return nil, false, nil
		}
		e = e2
	}

	// Update LRU access time.
	e.lastAccessedAt = nowSec
	sh.kv[key] = e
	sh.mu.Unlock()

	// Defensive copy to prevent callers from mutating internal state.
	out := make([]byte, len(e.value))
	copy(out, e.value)
	return out, true, nil
}

func (s *MemoryStore) Set(ctx context.Context, key string, value []byte) error {
	return s.SetWithTTL(ctx, key, value, s.defaultTTL)
}

func (s *MemoryStore) SetWithTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = s.defaultTTL
	}
	if ttl > s.maxTTL {
		ttl = s.maxTTL
	}
	expireAt := s.now().Add(ttl).UnixMilli()
	nowSec := s.now().Unix()

	// Defensive copy to prevent callers from mutating internal state.
	v := make([]byte, len(value))
	copy(v, value)

	newSize := entrySize(key, v)

	// If maxmemory is set, evict keys if needed.
	if s.maxMemory > 0 {
		if err := s.evictIfNeeded(ctx, newSize); err != nil {
			return err
		}
	}

	sh := s.shardFor(key)
	sh.mu.Lock()
	oldEntry, existed := sh.kv[key]

	// Calculate oldSize while lock is held to avoid race condition.
	var oldSize int64
	if existed {
		oldSize = entrySize(key, oldEntry.value)
	}

	sh.kv[key] = entry{
		value:          v,
		expireAtMs:     expireAt,
		lastAccessedAt: nowSec,
	}
	sh.mu.Unlock()

	// Update memory counter using pre-calculated values.
	if existed {
		atomic.AddInt64(&s.usedMemory, newSize-oldSize)
	} else {
		atomic.AddInt64(&s.usedMemory, newSize)
	}

	return nil
}

func (s *MemoryStore) Expire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, nil
	}
	if ttl > s.maxTTL {
		ttl = s.maxTTL
	}
	nowMs := s.now().UnixMilli()
	expireAt := s.now().Add(ttl).UnixMilli()

	sh := s.shardFor(key)
	sh.mu.Lock()
	e, ok := sh.kv[key]
	if !ok {
		sh.mu.Unlock()
		return false, nil
	}
	// If already expired, delete and treat as missing.
	if e.expireAtMs > 0 && nowMs >= e.expireAtMs {
		size := entrySize(key, e.value)
		delete(sh.kv, key)
		sh.mu.Unlock()
		atomic.AddInt64(&s.usedMemory, -size)
		return false, nil
	}
	e.expireAtMs = expireAt
	sh.kv[key] = e
	sh.mu.Unlock()
	return true, nil
}

func (s *MemoryStore) TTL(_ context.Context, key string) (time.Duration, bool, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.RLock()
	e, ok := sh.kv[key]
	sh.mu.RUnlock()
	if !ok {
		return 0, false, nil
	}
	if e.expireAtMs > 0 && nowMs >= e.expireAtMs {
		// Passive expiry: delete on TTL query.
		sh.mu.Lock()
		e2, ok2 := sh.kv[key]
		if ok2 && e2.expireAtMs > 0 && nowMs >= e2.expireAtMs {
			size := entrySize(key, e2.value)
			delete(sh.kv, key)
			sh.mu.Unlock()
			atomic.AddInt64(&s.usedMemory, -size)
		} else {
			sh.mu.Unlock()
		}
		return 0, false, nil
	}
	if e.expireAtMs == 0 {
		return 0, true, nil
	}
	remMs := e.expireAtMs - nowMs
	if remMs < 0 {
		remMs = 0
	}
	return time.Duration(remMs) * time.Millisecond, true, nil
}

func (s *MemoryStore) Del(_ context.Context, key string) (bool, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	e, ok := sh.kv[key]
	if ok {
		size := entrySize(key, e.value)
		delete(sh.kv, key)
		sh.mu.Unlock()
		atomic.AddInt64(&s.usedMemory, -size)
		return true, nil
	}
	sh.mu.Unlock()
	return false, nil
}

// Exists checks if key exists.
func (s *MemoryStore) Exists(_ context.Context, key string) (bool, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.RLock()
	e, ok := sh.kv[key]
	sh.mu.RUnlock()

	if !ok {
		return false, nil
	}

	// Check if expired.
	if e.expireAtMs > 0 && nowMs >= e.expireAtMs {
		return false, nil
	}

	return true, nil
}

// Incr increments the integer value of key by 1.
func (s *MemoryStore) Incr(ctx context.Context, key string) (int64, error) {
	return s.IncrBy(ctx, key, 1)
}

// Decr decrements the integer value of key by 1.
func (s *MemoryStore) Decr(ctx context.Context, key string) (int64, error) {
	return s.DecrBy(ctx, key, -1)
}

// IncrBy increments the integer value of key by delta.
func (s *MemoryStore) IncrBy(_ context.Context, key string, delta int64) (int64, error) {
	nowMs := s.now().UnixMilli()
	nowSec := nowMs / 1000
	sh := s.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	var currentValue int64
	e, ok := sh.kv[key]

	if ok {
		// Check if expired.
		if e.expireAtMs > 0 && nowMs >= e.expireAtMs {
			// Treat as non-existent (expired).
			ok = false
		} else {
			// Parse current value as integer.
			val, err := strconv.ParseInt(string(e.value), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("ERR value is not an integer or out of range")
			}
			currentValue = val
		}
	}

	// Calculate new value.
	newValue := currentValue + delta
	newValueBytes := []byte(strconv.FormatInt(newValue, 10))

	// Calculate memory delta.
	oldSize := int64(0)
	if ok {
		oldSize = entrySize(key, e.value)
	}
	newSize := entrySize(key, newValueBytes)

	// Calculate TTL (keep existing TTL or use default).
	var expireAtMs int64
	if ok && e.expireAtMs > 0 {
		expireAtMs = e.expireAtMs
	} else {
		expireAtMs = s.now().Add(s.defaultTTL).UnixMilli()
	}

	// Store new value.
	sh.kv[key] = entry{
		value:          newValueBytes,
		expireAtMs:     expireAtMs,
		lastAccessedAt: nowSec,
	}

	// Update memory counter.
	if ok {
		atomic.AddInt64(&s.usedMemory, newSize-oldSize)
	} else {
		atomic.AddInt64(&s.usedMemory, newSize)
	}

	return newValue, nil
}

// DecrBy decrements the integer value of key by delta.
func (s *MemoryStore) DecrBy(ctx context.Context, key string, delta int64) (int64, error) {
	return s.IncrBy(ctx, key, -delta)
}

// MSet sets multiple key-value pairs (all with default TTL).
func (s *MemoryStore) MSet(ctx context.Context, pairs map[string][]byte) error {
	for key, value := range pairs {
		if err := s.Set(ctx, key, value); err != nil {
			return err
		}
	}
	return nil
}

// MGet retrieves multiple keys.
func (s *MemoryStore) MGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte, len(keys))
	for _, key := range keys {
		value, ok, err := s.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if ok {
			result[key] = value
		}
	}
	return result, nil
}

// SetNX sets key only if it doesn't exist.
func (s *MemoryStore) SetNX(ctx context.Context, key string, value []byte) (bool, error) {
	nowMs := s.now().UnixMilli()
	nowSec := nowMs / 1000
	sh := s.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, exists := sh.kv[key]
	if exists {
		if e.expireAtMs > 0 && nowMs >= e.expireAtMs {
			exists = false
		} else {
			return false, nil
		}
	}

	expireAtMs := s.now().Add(s.defaultTTL).UnixMilli()
	newSize := entrySize(key, value)
	sh.kv[key] = entry{
		value:          value,
		expireAtMs:     expireAtMs,
		lastAccessedAt: nowSec,
		typ:            "string",
	}
	atomic.AddInt64(&s.usedMemory, newSize)
	return true, nil
}

// SetEX sets key with expiry in seconds.
func (s *MemoryStore) SetEX(ctx context.Context, key string, value []byte, seconds int64) error {
	ttl := time.Duration(seconds) * time.Second
	if ttl > s.maxTTL {
		ttl = s.maxTTL
	}
	return s.SetWithTTL(ctx, key, value, ttl)
}

// Append appends value to existing key.
func (s *MemoryStore) Append(ctx context.Context, key string, value []byte) (int64, error) {
	nowMs := s.now().UnixMilli()
	nowSec := nowMs / 1000
	sh := s.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, exists := sh.kv[key]
	var newValue []byte
	var oldSize int64

	if exists && !(e.expireAtMs > 0 && nowMs >= e.expireAtMs) {
		oldSize = entrySize(key, e.value)
		newValue = append(e.value, value...)
	} else {
		newValue = value
		exists = false
	}

	newSize := entrySize(key, newValue)
	expireAtMs := s.now().Add(s.defaultTTL).UnixMilli()
	if exists && e.expireAtMs > 0 {
		expireAtMs = e.expireAtMs
	}

	sh.kv[key] = entry{
		value:          newValue,
		expireAtMs:     expireAtMs,
		lastAccessedAt: nowSec,
	}

	if exists {
		atomic.AddInt64(&s.usedMemory, newSize-oldSize)
	} else {
		atomic.AddInt64(&s.usedMemory, newSize)
	}

	return int64(len(newValue)), nil
}

// StrLen returns length of string value.
func (s *MemoryStore) StrLen(ctx context.Context, key string) (int64, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.RLock()
	e, ok := sh.kv[key]
	sh.mu.RUnlock()

	if !ok || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) {
		return 0, nil
	}
	return int64(len(e.value)), nil
}

// Scan iterates keys matching pattern.
func (s *MemoryStore) Scan(ctx context.Context, cursor int, pattern string, count int) (int, []string, error) {
	if count <= 0 {
		count = 10
	}

	nowMs := s.now().UnixMilli()
	var keys []string
	totalShards := len(s.shards)
	startShard := cursor % totalShards
	keysPerShard := count / totalShards
	if keysPerShard == 0 {
		keysPerShard = 1
	}

	for i := 0; i < totalShards && len(keys) < count; i++ {
		shardIdx := (startShard + i) % totalShards
		sh := &s.shards[shardIdx]

		sh.mu.RLock()
		for k, e := range sh.kv {
			if len(keys) >= count {
				sh.mu.RUnlock()
				goto done
			}
			if e.expireAtMs > 0 && nowMs >= e.expireAtMs {
				continue
			}
			if pattern == "*" || matchPattern(pattern, k) {
				keys = append(keys, k)
			}
		}
		sh.mu.RUnlock()
	}

done:
	nextCursor := (cursor + totalShards) % (totalShards * 100)
	if len(keys) < count {
		nextCursor = 0
	}
	return nextCursor, keys, nil
}

func matchPattern(pattern, key string) bool {
	if pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return pattern == key
	}
	parts := strings.Split(pattern, "*")
	if len(parts) == 2 {
		prefix, suffix := parts[0], parts[1]
		return strings.HasPrefix(key, prefix) && strings.HasSuffix(key, suffix)
	}
	return false
}

// HSet sets hash field.
func (s *MemoryStore) HSet(ctx context.Context, key, field string, value []byte) (bool, error) {
	nowMs := s.now().UnixMilli()
	nowSec := nowMs / 1000
	sh := s.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, exists := sh.kv[key]
	isNew := false
	var sizeDelta int64
	
	if !exists || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) {
		e = entry{
			typ:            "hash",
			hash:           make(map[string][]byte),
			expireAtMs:     s.now().Add(s.defaultTTL).UnixMilli(),
			lastAccessedAt: nowSec,
		}
		exists = false
		sizeDelta += int64(len(key)) + 48
	}

	if e.typ != "hash" {
		return false, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	oldValue, fieldExists := e.hash[field]
	if fieldExists {
		sizeDelta -= int64(len(oldValue))
	} else {
		sizeDelta += int64(len(field))
	}
	sizeDelta += int64(len(value))
	
	e.hash[field] = value
	e.lastAccessedAt = nowSec
	sh.kv[key] = e

	atomic.AddInt64(&s.usedMemory, sizeDelta)

	if !fieldExists {
		isNew = true
	}
	return isNew, nil
}

// HGet gets hash field.
func (s *MemoryStore) HGet(ctx context.Context, key, field string) ([]byte, bool, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.RLock()
	e, ok := sh.kv[key]
	sh.mu.RUnlock()

	if !ok || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) || e.typ != "hash" {
		return nil, false, nil
	}

	val, exists := e.hash[field]
	if !exists {
		return nil, false, nil
	}
	
	result := make([]byte, len(val))
	copy(result, val)
	return result, true, nil
}

// HGetAll gets all hash fields.
func (s *MemoryStore) HGetAll(ctx context.Context, key string) (map[string][]byte, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.RLock()
	e, ok := sh.kv[key]
	sh.mu.RUnlock()

	if !ok || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) || e.typ != "hash" {
		return make(map[string][]byte), nil
	}

	result := make(map[string][]byte, len(e.hash))
	for k, v := range e.hash {
		valCopy := make([]byte, len(v))
		copy(valCopy, v)
		result[k] = valCopy
	}
	return result, nil
}

// HDel deletes hash fields.
func (s *MemoryStore) HDel(ctx context.Context, key string, fields []string) (int64, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.kv[key]
	if !ok || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) || e.typ != "hash" {
		return 0, nil
	}

	count := int64(0)
	var freedMemory int64
	for _, field := range fields {
		if value, exists := e.hash[field]; exists {
			freedMemory += int64(len(field)) + int64(len(value))
			delete(e.hash, field)
			count++
		}
	}

	if len(e.hash) == 0 {
		freedMemory += int64(len(key)) + 48
		delete(sh.kv, key)
	} else {
		sh.kv[key] = e
	}
	
	atomic.AddInt64(&s.usedMemory, -freedMemory)
	return count, nil
}

// HExists checks if hash field exists.
func (s *MemoryStore) HExists(ctx context.Context, key, field string) (bool, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.RLock()
	e, ok := sh.kv[key]
	sh.mu.RUnlock()

	if !ok || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) || e.typ != "hash" {
		return false, nil
	}

	_, exists := e.hash[field]
	return exists, nil
}

// HLen returns hash field count.
func (s *MemoryStore) HLen(ctx context.Context, key string) (int64, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.RLock()
	e, ok := sh.kv[key]
	sh.mu.RUnlock()

	if !ok || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) || e.typ != "hash" {
		return 0, nil
	}
	return int64(len(e.hash)), nil
}

// HKeys returns all hash field names.
func (s *MemoryStore) HKeys(ctx context.Context, key string) ([]string, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.RLock()
	e, ok := sh.kv[key]
	sh.mu.RUnlock()

	if !ok || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) || e.typ != "hash" {
		return []string{}, nil
	}

	keys := make([]string, 0, len(e.hash))
	for k := range e.hash {
		keys = append(keys, k)
	}
	return keys, nil
}

// HVals returns all hash values.
func (s *MemoryStore) HVals(ctx context.Context, key string) ([][]byte, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.RLock()
	e, ok := sh.kv[key]
	sh.mu.RUnlock()

	if !ok || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) || e.typ != "hash" {
		return [][]byte{}, nil
	}

	vals := make([][]byte, 0, len(e.hash))
	for _, v := range e.hash {
		valCopy := make([]byte, len(v))
		copy(valCopy, v)
		vals = append(vals, valCopy)
	}
	return vals, nil
}

// HMSet sets multiple hash fields.
func (s *MemoryStore) HMSet(ctx context.Context, key string, fields map[string][]byte) error {
	for field, value := range fields {
		if _, err := s.HSet(ctx, key, field, value); err != nil {
			return err
		}
	}
	return nil
}

// HMGet gets multiple hash fields.
func (s *MemoryStore) HMGet(ctx context.Context, key string, fields []string) ([][]byte, error) {
	result := make([][]byte, len(fields))
	for i, field := range fields {
		val, ok, err := s.HGet(ctx, key, field)
		if err != nil {
			return nil, err
		}
		if ok {
			result[i] = val
		}
	}
	return result, nil
}

// LPush pushes to list head.
func (s *MemoryStore) LPush(ctx context.Context, key string, values [][]byte) (int64, error) {
	nowMs := s.now().UnixMilli()
	nowSec := nowMs / 1000
	sh := s.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, exists := sh.kv[key]
	var sizeDelta int64
	if !exists || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) {
		e = entry{
			typ:            "list",
			list:           [][]byte{},
			expireAtMs:     s.now().Add(s.defaultTTL).UnixMilli(),
			lastAccessedAt: nowSec,
		}
		sizeDelta += int64(len(key)) + 48
	}

	if e.typ != "list" {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	for _, value := range values {
		sizeDelta += int64(len(value))
	}

	e.list = append(values, e.list...)
	e.lastAccessedAt = nowSec
	sh.kv[key] = e
	
	atomic.AddInt64(&s.usedMemory, sizeDelta)
	return int64(len(e.list)), nil
}

// RPush pushes to list tail.
func (s *MemoryStore) RPush(ctx context.Context, key string, values [][]byte) (int64, error) {
	nowMs := s.now().UnixMilli()
	nowSec := nowMs / 1000
	sh := s.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, exists := sh.kv[key]
	var sizeDelta int64
	if !exists || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) {
		e = entry{
			typ:            "list",
			list:           [][]byte{},
			expireAtMs:     s.now().Add(s.defaultTTL).UnixMilli(),
			lastAccessedAt: nowSec,
		}
		sizeDelta += int64(len(key)) + 48
	}

	if e.typ != "list" {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	for _, value := range values {
		sizeDelta += int64(len(value))
	}

	e.list = append(e.list, values...)
	e.lastAccessedAt = nowSec
	sh.kv[key] = e
	
	atomic.AddInt64(&s.usedMemory, sizeDelta)
	return int64(len(e.list)), nil
}

// LPop pops from list head.
func (s *MemoryStore) LPop(ctx context.Context, key string) ([]byte, bool, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.kv[key]
	if !ok || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) || e.typ != "list" || len(e.list) == 0 {
		return nil, false, nil
	}

	val := e.list[0]
	sizeDelta := -int64(len(val))
	e.list = e.list[1:]
	
	if len(e.list) == 0 {
		sizeDelta -= int64(len(key)) + 48
		delete(sh.kv, key)
	} else {
		sh.kv[key] = e
	}
	
	atomic.AddInt64(&s.usedMemory, sizeDelta)
	
	valCopy := make([]byte, len(val))
	copy(valCopy, val)
	return valCopy, true, nil
}

// RPop pops from list tail.
func (s *MemoryStore) RPop(ctx context.Context, key string) ([]byte, bool, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.kv[key]
	if !ok || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) || e.typ != "list" || len(e.list) == 0 {
		return nil, false, nil
	}

	val := e.list[len(e.list)-1]
	sizeDelta := -int64(len(val))
	e.list = e.list[:len(e.list)-1]
	
	if len(e.list) == 0 {
		sizeDelta -= int64(len(key)) + 48
		delete(sh.kv, key)
	} else {
		sh.kv[key] = e
	}
	
	atomic.AddInt64(&s.usedMemory, sizeDelta)
	
	valCopy := make([]byte, len(val))
	copy(valCopy, val)
	return valCopy, true, nil
}

// LLen returns list length.
func (s *MemoryStore) LLen(ctx context.Context, key string) (int64, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.RLock()
	e, ok := sh.kv[key]
	sh.mu.RUnlock()

	if !ok || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) || e.typ != "list" {
		return 0, nil
	}
	return int64(len(e.list)), nil
}

// LRange returns list range.
func (s *MemoryStore) LRange(ctx context.Context, key string, start, stop int64) ([][]byte, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.RLock()
	e, ok := sh.kv[key]
	sh.mu.RUnlock()

	if !ok || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) || e.typ != "list" {
		return [][]byte{}, nil
	}

	length := int64(len(e.list))
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		return [][]byte{}, nil
	}

	result := make([][]byte, stop-start+1)
	for i := start; i <= stop; i++ {
		valCopy := make([]byte, len(e.list[i]))
		copy(valCopy, e.list[i])
		result[i-start] = valCopy
	}
	return result, nil
}

// LIndex returns list element at index.
func (s *MemoryStore) LIndex(ctx context.Context, key string, index int64) ([]byte, bool, error) {
	nowMs := s.now().UnixMilli()
	sh := s.shardFor(key)

	sh.mu.RLock()
	e, ok := sh.kv[key]
	sh.mu.RUnlock()

	if !ok || (e.expireAtMs > 0 && nowMs >= e.expireAtMs) || e.typ != "list" {
		return nil, false, nil
	}

	length := int64(len(e.list))
	if index < 0 {
		index = length + index
	}
	if index < 0 || index >= length {
		return nil, false, nil
	}

	valCopy := make([]byte, len(e.list[index]))
	copy(valCopy, e.list[index])
	return valCopy, true, nil
}

// evictIfNeeded runs the eviction loop if usedMemory + newSize would exceed maxMemory.
func (s *MemoryStore) evictIfNeeded(ctx context.Context, newSize int64) error {
	used := atomic.LoadInt64(&s.usedMemory)
	if used+newSize <= s.maxMemory {
		return nil
	}

	switch s.policy {
	case "noeviction":
		return errors.New("OOM command not allowed when used memory > 'maxmemory'")
	case "allkeys-lru":
		return s.evictAllKeysLRU(ctx, newSize)
	default:
		return errors.New("unsupported eviction policy: " + s.policy)
	}
}

// evictAllKeysLRU implements approximated LRU eviction.
// Sample N random keys and evict the one with the oldest lastAccessedAt.
func (s *MemoryStore) evictAllKeysLRU(ctx context.Context, neededSpace int64) error {
	const sampleSize = 5
	const maxAttempts = 100

	for attempt := 0; attempt < maxAttempts; attempt++ {
		used := atomic.LoadInt64(&s.usedMemory)
		if used+neededSpace <= s.maxMemory {
			return nil
		}

		// Sample random keys across shards.
		candidates := s.sampleKeys(sampleSize)
		if len(candidates) == 0 {
			// No keys to evict.
			return errors.New("OOM: unable to evict enough keys")
		}

		// Find the one with the oldest access time.
		oldestKey := ""
		oldestTime := int64(1<<63 - 1)
		for key, accessTime := range candidates {
			if accessTime < oldestTime {
				oldestTime = accessTime
				oldestKey = key
			}
		}

		// Evict it.
		if oldestKey != "" {
			_, _ = s.Del(ctx, oldestKey)
		}
	}

	return errors.New("OOM: unable to evict enough keys after max attempts")
}

// sampleKeys returns a random sample of up to N keys with their lastAccessedAt times.
func (s *MemoryStore) sampleKeys(n int) map[string]int64 {
	sample := make(map[string]int64, n)
	collected := 0

	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for k, e := range sh.kv {
			if collected >= n {
				sh.mu.RUnlock()
				return sample
			}
			if rand.Float64() < 0.1 { // probabilistic sampling
				sample[k] = e.lastAccessedAt
				collected++
			}
		}
		sh.mu.RUnlock()
	}

	return sample
}

// StartJanitor starts a background loop that actively deletes expired keys.
// It is safe to call at most once per store instance.
func (s *MemoryStore) StartJanitor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	t := time.NewTicker(interval)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.deleteExpiredOnce()
			}
		}
	}()
}

func (s *MemoryStore) deleteExpiredOnce() {
	nowMs := s.now().UnixMilli()

	// Limit work per tick to avoid long pauses on large datasets.
	const maxDeletesPerShard = 64

	for i := range s.shards {
		sh := &s.shards[i]
		deletes := 0

		sh.mu.Lock()
		for k, e := range sh.kv {
			if e.expireAtMs > 0 && nowMs >= e.expireAtMs {
				size := entrySize(k, e.value)
				delete(sh.kv, k)
				atomic.AddInt64(&s.usedMemory, -size)
				deletes++
				if deletes >= maxDeletesPerShard {
					break
				}
			}
		}
		sh.mu.Unlock()
	}
}

// entrySize estimates the memory footprint of one entry.
// This is approximate: real overhead includes map bucket overhead, struct alignment, etc.
func entrySize(key string, value []byte) int64 {
	const overhead = 64 // rough estimate per entry (struct + map overhead)
	return int64(len(key) + len(value) + overhead)
}

// IterateAllKeys calls fn for every key/value/expireAt in the store.
// Used for replication snapshots.
func (s *MemoryStore) IterateAllKeys(ctx context.Context, fn func(key string, value []byte, expireAtMs int64) error) error {
	type snapshotEntry struct {
		key        string
		value      []byte
		expireAtMs int64
	}

	for i := range s.shards {
		sh := &s.shards[i]

		// Snapshot all entries while holding the lock.
		var snapshot []snapshotEntry
		sh.mu.RLock()
		for k, e := range sh.kv {
			select {
			case <-ctx.Done():
				sh.mu.RUnlock()
				return ctx.Err()
			default:
			}
			// Defensive copy of value.
			valCopy := make([]byte, len(e.value))
			copy(valCopy, e.value)
			snapshot = append(snapshot, snapshotEntry{
				key:        k,
				value:      valCopy,
				expireAtMs: e.expireAtMs,
			})
		}
		sh.mu.RUnlock()

		// Call callbacks without holding the lock.
		for _, entry := range snapshot {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := fn(entry.key, entry.value, entry.expireAtMs); err != nil {
				return err
			}
		}
	}
	return nil
}
