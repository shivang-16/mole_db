package core

import (
	"context"
	"time"
)

// Store is Mole's storage contract for the single-node cache.
//
// Values are opaque bytes; higher layers define command semantics.
type Store interface {
	Get(ctx context.Context, key string) (value []byte, ok bool, err error)
	// Set stores the value using the store's default TTL policy.
	Set(ctx context.Context, key string, value []byte) error
	// SetWithTTL stores the value with a specific TTL (which may be capped by policy).
	SetWithTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// Expire sets/updates the TTL for an existing key.
	Expire(ctx context.Context, key string, ttl time.Duration) (updated bool, err error)
	// TTL returns the remaining TTL. If ok=false, the key does not exist (or is expired).
	TTL(ctx context.Context, key string) (ttl time.Duration, ok bool, err error)
	Del(ctx context.Context, key string) (deleted bool, err error)
}
