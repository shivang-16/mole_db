package core

import (
	"context"
	"errors"
	"hash/fnv"
	"math/rand"
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
	expireAtMs     int64 // unix millis; 0 means no expiry (unused in current policy)
	lastAccessedAt int64 // unix seconds (for LRU approximation)
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

	sh.mu.Lock()
	e, ok := sh.kv[key]
	if !ok {
		sh.mu.Unlock()
		return nil, false, nil
	}

	if e.expireAtMs > 0 && nowMs >= e.expireAtMs {
		// Passive expiry: delete on read.
		size := entrySize(key, e.value)
		delete(sh.kv, key)
		sh.mu.Unlock()
		atomic.AddInt64(&s.usedMemory, -size)
		return nil, false, nil
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
	sh.kv[key] = entry{
		value:          v,
		expireAtMs:     expireAt,
		lastAccessedAt: nowSec,
	}
	sh.mu.Unlock()

	// Update memory counter.
	if existed {
		oldSize := entrySize(key, oldEntry.value)
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
	for i := range s.shards {
		sh := &s.shards[i]
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
			if err := fn(k, valCopy, e.expireAtMs); err != nil {
				sh.mu.RUnlock()
				return err
			}
		}
		sh.mu.RUnlock()
	}
	return nil
}
