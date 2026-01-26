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

	// Exists checks if key exists.
	Exists(ctx context.Context, key string) (bool, error)

	// Incr increments the integer value of key by 1. Returns new value.
	// If key doesn't exist, it's set to 0 before incrementing.
	Incr(ctx context.Context, key string) (int64, error)
	// Decr decrements the integer value of key by 1. Returns new value.
	Decr(ctx context.Context, key string) (int64, error)
	// IncrBy increments the integer value of key by delta. Returns new value.
	IncrBy(ctx context.Context, key string, delta int64) (int64, error)
	// DecrBy decrements the integer value of key by delta. Returns new value.
	DecrBy(ctx context.Context, key string, delta int64) (int64, error)

	// MSet sets multiple key-value pairs (all with default TTL).
	MSet(ctx context.Context, pairs map[string][]byte) error
	// MGet retrieves multiple keys. Returns map of key -> value.
	// Keys that don't exist are omitted from the result map.
	MGet(ctx context.Context, keys []string) (map[string][]byte, error)

	// SetNX sets key only if it doesn't exist. Returns true if set.
	SetNX(ctx context.Context, key string, value []byte) (bool, error)
	// SetEX sets key with expiry in seconds.
	SetEX(ctx context.Context, key string, value []byte, seconds int64) error
	// Append appends value to existing key. Returns new length.
	Append(ctx context.Context, key string, value []byte) (int64, error)
	// StrLen returns length of string value.
	StrLen(ctx context.Context, key string) (int64, error)
	// Scan iterates keys matching pattern. Returns cursor and keys.
	Scan(ctx context.Context, cursor int, pattern string, count int) (int, []string, error)

	// Hash operations
	HSet(ctx context.Context, key, field string, value []byte) (bool, error)
	HGet(ctx context.Context, key, field string) ([]byte, bool, error)
	HGetAll(ctx context.Context, key string) (map[string][]byte, error)
	HDel(ctx context.Context, key string, fields []string) (int64, error)
	HExists(ctx context.Context, key, field string) (bool, error)
	HLen(ctx context.Context, key string) (int64, error)
	HKeys(ctx context.Context, key string) ([]string, error)
	HVals(ctx context.Context, key string) ([][]byte, error)
	HMSet(ctx context.Context, key string, fields map[string][]byte) error
	HMGet(ctx context.Context, key string, fields []string) ([][]byte, error)

	// List operations
	LPush(ctx context.Context, key string, values [][]byte) (int64, error)
	RPush(ctx context.Context, key string, values [][]byte) (int64, error)
	LPop(ctx context.Context, key string) ([]byte, bool, error)
	RPop(ctx context.Context, key string) ([]byte, bool, error)
	LLen(ctx context.Context, key string) (int64, error)
	LRange(ctx context.Context, key string, start, stop int64) ([][]byte, error)
	LIndex(ctx context.Context, key string, index int64) ([]byte, bool, error)
}
