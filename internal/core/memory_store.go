package core

import (
	"context"
	"sync"
	"time"
)

// MemoryStore is a concurrency-safe in-memory map store.
//
// This is the simplest backend; later we can evolve it to support TTL metadata,
// sharding, and memory policies while keeping the Store interface stable.
type MemoryStore struct {
	mu sync.RWMutex

	kv map[string]entry

	defaultTTL time.Duration
	maxTTL     time.Duration
	now        func() time.Time
}

type MemoryStoreOptions struct {
	DefaultTTL time.Duration
	MaxTTL     time.Duration
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
	return &MemoryStore{
		kv:         make(map[string]entry),
		defaultTTL: opts.DefaultTTL,
		maxTTL:     opts.MaxTTL,
		now:        time.Now,
	}
}

type entry struct {
	value      []byte
	expireAtMs int64 // unix millis; 0 means no expiry (unused in current policy)
}

func (s *MemoryStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	nowMs := s.now().UnixMilli()

	s.mu.RLock()
	e, ok := s.kv[key]
	s.mu.RUnlock()
	if !ok {
		return nil, false, nil
	}

	if e.expireAtMs > 0 && nowMs >= e.expireAtMs {
		// Passive expiry: delete on read.
		s.mu.Lock()
		// Re-check under write lock to avoid races.
		e2, ok2 := s.kv[key]
		if ok2 && e2.expireAtMs > 0 && nowMs >= e2.expireAtMs {
			delete(s.kv, key)
		}
		s.mu.Unlock()
		return nil, false, nil
	}

	// Defensive copy to prevent callers from mutating internal state.
	out := make([]byte, len(e.value))
	copy(out, e.value)
	return out, true, nil
}

func (s *MemoryStore) Set(ctx context.Context, key string, value []byte) error {
	return s.SetWithTTL(ctx, key, value, s.defaultTTL)
}

func (s *MemoryStore) SetWithTTL(_ context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = s.defaultTTL
	}
	if ttl > s.maxTTL {
		ttl = s.maxTTL
	}
	expireAt := s.now().Add(ttl).UnixMilli()

	// Defensive copy to prevent callers from mutating internal state.
	v := make([]byte, len(value))
	copy(v, value)

	s.mu.Lock()
	s.kv[key] = entry{value: v, expireAtMs: expireAt}
	s.mu.Unlock()
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

	s.mu.Lock()
	e, ok := s.kv[key]
	if !ok {
		s.mu.Unlock()
		return false, nil
	}
	// If already expired, delete and treat as missing.
	if e.expireAtMs > 0 && nowMs >= e.expireAtMs {
		delete(s.kv, key)
		s.mu.Unlock()
		return false, nil
	}
	e.expireAtMs = expireAt
	s.kv[key] = e
	s.mu.Unlock()
	return true, nil
}

func (s *MemoryStore) TTL(_ context.Context, key string) (time.Duration, bool, error) {
	nowMs := s.now().UnixMilli()

	s.mu.RLock()
	e, ok := s.kv[key]
	s.mu.RUnlock()
	if !ok {
		return 0, false, nil
	}
	if e.expireAtMs > 0 && nowMs >= e.expireAtMs {
		// Passive expiry: delete on TTL query.
		s.mu.Lock()
		e2, ok2 := s.kv[key]
		if ok2 && e2.expireAtMs > 0 && nowMs >= e2.expireAtMs {
			delete(s.kv, key)
		}
		s.mu.Unlock()
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
	s.mu.Lock()
	_, ok := s.kv[key]
	if ok {
		delete(s.kv, key)
	}
	s.mu.Unlock()
	return ok, nil
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
	s.mu.Lock()
	for k, e := range s.kv {
		if e.expireAtMs > 0 && nowMs >= e.expireAtMs {
			delete(s.kv, k)
		}
	}
	s.mu.Unlock()
}
