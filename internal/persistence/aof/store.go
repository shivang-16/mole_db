package aof

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"mole/internal/core"
)

// LoggingStore wraps a core.Store and appends successful writes to an AOF Writer.
//
// It computes the effective TTL using the same policy values used by MemoryStore,
// then records absolute expireAt timestamps so replay does not extend key lifetimes.
//
// If AOF append fails, the write is already committed to memory.
// This is acceptable for async replication - AOF failures are logged but don't
// rollback memory operations (which would be incorrect for async replication).
type LoggingStore struct {
	underlying core.Store
	w          *Writer

	defaultTTL time.Duration
	maxTTL     time.Duration
	now        func() time.Time
}

type LoggingStoreOptions struct {
	DefaultTTL time.Duration
	MaxTTL     time.Duration
	Now        func() time.Time
}

func Wrap(underlying core.Store, w *Writer, opts LoggingStoreOptions) (*LoggingStore, error) {
	if underlying == nil {
		return nil, errors.New("mole: underlying store is required")
	}
	if w == nil {
		return nil, errors.New("mole: aof writer is required")
	}
	if opts.DefaultTTL <= 0 {
		opts.DefaultTTL = 20 * 24 * time.Hour
	}
	if opts.MaxTTL <= 0 {
		opts.MaxTTL = 20 * 24 * time.Hour
	}
	if opts.DefaultTTL > opts.MaxTTL {
		opts.DefaultTTL = opts.MaxTTL
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	return &LoggingStore{
		underlying: underlying,
		w:          w,
		defaultTTL: opts.DefaultTTL,
		maxTTL:     opts.MaxTTL,
		now:        opts.Now,
	}, nil
}

func (s *LoggingStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return s.underlying.Get(ctx, key)
}

func (s *LoggingStore) TTL(ctx context.Context, key string) (time.Duration, bool, error) {
	return s.underlying.TTL(ctx, key)
}

func (s *LoggingStore) Set(ctx context.Context, key string, value []byte) error {
	ttl := s.defaultTTL
	// Normalize TTL for consistency with SetWithTTL() and Expire() methods.
	eff := normalizeTTL(ttl, s.defaultTTL, s.maxTTL)
	expireAtMs := s.now().Add(eff).UnixMilli()

	// Use SetWithTTL to ensure underlying store and AOF log use consistent TTL.
	if err := s.underlying.SetWithTTL(ctx, key, value, eff); err != nil {
		return err
	}
	return s.w.AppendRecord(RecordSetAt(key, value, expireAtMs))
}

func (s *LoggingStore) SetWithTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	eff := normalizeTTL(ttl, s.defaultTTL, s.maxTTL)
	expireAtMs := s.now().Add(eff).UnixMilli()

	// Pass normalized TTL to underlying store to ensure consistency with AOF log.
	if err := s.underlying.SetWithTTL(ctx, key, value, eff); err != nil {
		return err
	}
	return s.w.AppendRecord(RecordSetAt(key, value, expireAtMs))
}

func (s *LoggingStore) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	eff := normalizeTTL(ttl, s.defaultTTL, s.maxTTL)
	if eff <= 0 {
		return s.underlying.Expire(ctx, key, eff)
	}
	expireAtMs := s.now().Add(eff).UnixMilli()

	// Pass normalized TTL to underlying store to ensure consistency with AOF log.
	updated, err := s.underlying.Expire(ctx, key, eff)
	if err != nil || !updated {
		return updated, err
	}
	return updated, s.w.AppendRecord(RecordExpireAt(key, expireAtMs))
}

func (s *LoggingStore) Del(ctx context.Context, key string) (bool, error) {
	deleted, err := s.underlying.Del(ctx, key)
	if err != nil || !deleted {
		return deleted, err
	}
	return deleted, s.w.AppendRecord(RecordDel(key))
}

// Exists checks if key exists (read-only).
func (s *LoggingStore) Exists(ctx context.Context, key string) (bool, error) {
	return s.underlying.Exists(ctx, key)
}

// Incr increments and logs the operation.
func (s *LoggingStore) Incr(ctx context.Context, key string) (int64, error) {
	newValue, err := s.underlying.Incr(ctx, key)
	if err != nil {
		return 0, err
	}
	// Log the final value as MOLE.SET
	expireAtMs := s.now().Add(s.defaultTTL).UnixMilli()
	_ = s.w.AppendRecord(RecordSetAt(key, []byte(strconv.FormatInt(newValue, 10)), expireAtMs))
	return newValue, nil
}

// Decr decrements and logs the operation.
func (s *LoggingStore) Decr(ctx context.Context, key string) (int64, error) {
	newValue, err := s.underlying.Decr(ctx, key)
	if err != nil {
		return 0, err
	}
	expireAtMs := s.now().Add(s.defaultTTL).UnixMilli()
	_ = s.w.AppendRecord(RecordSetAt(key, []byte(strconv.FormatInt(newValue, 10)), expireAtMs))
	return newValue, nil
}

// IncrBy increments by delta and logs the operation.
func (s *LoggingStore) IncrBy(ctx context.Context, key string, delta int64) (int64, error) {
	newValue, err := s.underlying.IncrBy(ctx, key, delta)
	if err != nil {
		return 0, err
	}
	expireAtMs := s.now().Add(s.defaultTTL).UnixMilli()
	_ = s.w.AppendRecord(RecordSetAt(key, []byte(strconv.FormatInt(newValue, 10)), expireAtMs))
	return newValue, nil
}

// DecrBy decrements by delta and logs the operation.
func (s *LoggingStore) DecrBy(ctx context.Context, key string, delta int64) (int64, error) {
	newValue, err := s.underlying.DecrBy(ctx, key, delta)
	if err != nil {
		return 0, err
	}
	expireAtMs := s.now().Add(s.defaultTTL).UnixMilli()
	_ = s.w.AppendRecord(RecordSetAt(key, []byte(strconv.FormatInt(newValue, 10)), expireAtMs))
	return newValue, nil
}

// MSet sets multiple keys and logs each operation.
func (s *LoggingStore) MSet(ctx context.Context, pairs map[string][]byte) error {
	if err := s.underlying.MSet(ctx, pairs); err != nil {
		return err
	}
	// Log each key-value pair
	expireAtMs := s.now().Add(s.defaultTTL).UnixMilli()
	for key, value := range pairs {
		_ = s.w.AppendRecord(RecordSetAt(key, value, expireAtMs))
	}
	return nil
}

// MGet retrieves multiple keys (read-only).
func (s *LoggingStore) MGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	return s.underlying.MGet(ctx, keys)
}

// SetNX sets if not exists.
func (s *LoggingStore) SetNX(ctx context.Context, key string, value []byte) (bool, error) {
	set, err := s.underlying.SetNX(ctx, key, value)
	if err != nil || !set {
		return set, err
	}
	expireAtMs := s.now().Add(s.defaultTTL).UnixMilli()
	_ = s.w.AppendRecord(RecordSetAt(key, value, expireAtMs))
	return true, nil
}

// SetEX sets with expiry.
func (s *LoggingStore) SetEX(ctx context.Context, key string, value []byte, seconds int64) error {
	if err := s.underlying.SetEX(ctx, key, value, seconds); err != nil {
		return err
	}
	expireAtMs := s.now().Add(time.Duration(seconds) * time.Second).UnixMilli()
	return s.w.AppendRecord(RecordSetAt(key, value, expireAtMs))
}

// Append appends to key.
func (s *LoggingStore) Append(ctx context.Context, key string, value []byte) (int64, error) {
	newLen, err := s.underlying.Append(ctx, key, value)
	if err != nil {
		return 0, err
	}
	fullValue, ok, err := s.underlying.Get(ctx, key)
	if err != nil {
		return newLen, err
	}
	if ok {
		expireAtMs := s.now().Add(s.defaultTTL).UnixMilli()
		_ = s.w.AppendRecord(RecordSetAt(key, fullValue, expireAtMs))
	}
	return newLen, nil
}

// StrLen returns string length (read-only).
func (s *LoggingStore) StrLen(ctx context.Context, key string) (int64, error) {
	return s.underlying.StrLen(ctx, key)
}

// Scan iterates keys (read-only).
func (s *LoggingStore) Scan(ctx context.Context, cursor int, pattern string, count int) (int, []string, error) {
	return s.underlying.Scan(ctx, cursor, pattern, count)
}

// HSet sets hash field.
func (s *LoggingStore) HSet(ctx context.Context, key, field string, value []byte) (bool, error) {
	isNew, err := s.underlying.HSet(ctx, key, field, value)
	if err != nil {
		return false, err
	}
	_ = s.w.AppendRecord([][]byte{[]byte("MOLE.HSET"), []byte(key), []byte(field), value})
	return isNew, nil
}

func (s *LoggingStore) HGet(ctx context.Context, key, field string) ([]byte, bool, error) {
	return s.underlying.HGet(ctx, key, field)
}

func (s *LoggingStore) HGetAll(ctx context.Context, key string) (map[string][]byte, error) {
	return s.underlying.HGetAll(ctx, key)
}

func (s *LoggingStore) HDel(ctx context.Context, key string, fields []string) (int64, error) {
	count, err := s.underlying.HDel(ctx, key, fields)
	if err != nil || count == 0 {
		return count, err
	}
	for _, field := range fields {
		_ = s.w.AppendRecord([][]byte{[]byte("MOLE.HDEL"), []byte(key), []byte(field)})
	}
	return count, nil
}

func (s *LoggingStore) HExists(ctx context.Context, key, field string) (bool, error) {
	return s.underlying.HExists(ctx, key, field)
}

func (s *LoggingStore) HLen(ctx context.Context, key string) (int64, error) {
	return s.underlying.HLen(ctx, key)
}

func (s *LoggingStore) HKeys(ctx context.Context, key string) ([]string, error) {
	return s.underlying.HKeys(ctx, key)
}

func (s *LoggingStore) HVals(ctx context.Context, key string) ([][]byte, error) {
	return s.underlying.HVals(ctx, key)
}

func (s *LoggingStore) HMSet(ctx context.Context, key string, fields map[string][]byte) error {
	if err := s.underlying.HMSet(ctx, key, fields); err != nil {
		return err
	}
	for field, value := range fields {
		_ = s.w.AppendRecord([][]byte{[]byte("MOLE.HSET"), []byte(key), []byte(field), value})
	}
	return nil
}

func (s *LoggingStore) HMGet(ctx context.Context, key string, fields []string) ([][]byte, error) {
	return s.underlying.HMGet(ctx, key, fields)
}

func (s *LoggingStore) LPush(ctx context.Context, key string, values [][]byte) (int64, error) {
	length, err := s.underlying.LPush(ctx, key, values)
	if err != nil {
		return 0, err
	}
	for _, value := range values {
		_ = s.w.AppendRecord([][]byte{[]byte("MOLE.LPUSH"), []byte(key), value})
	}
	return length, nil
}

func (s *LoggingStore) RPush(ctx context.Context, key string, values [][]byte) (int64, error) {
	length, err := s.underlying.RPush(ctx, key, values)
	if err != nil {
		return 0, err
	}
	for _, value := range values {
		_ = s.w.AppendRecord([][]byte{[]byte("MOLE.RPUSH"), []byte(key), value})
	}
	return length, nil
}

func (s *LoggingStore) LPop(ctx context.Context, key string) ([]byte, bool, error) {
	value, ok, err := s.underlying.LPop(ctx, key)
	if err != nil || !ok {
		return value, ok, err
	}
	_ = s.w.AppendRecord([][]byte{[]byte("MOLE.LPOP"), []byte(key)})
	return value, ok, nil
}

func (s *LoggingStore) RPop(ctx context.Context, key string) ([]byte, bool, error) {
	value, ok, err := s.underlying.RPop(ctx, key)
	if err != nil || !ok {
		return value, ok, err
	}
	_ = s.w.AppendRecord([][]byte{[]byte("MOLE.RPOP"), []byte(key)})
	return value, ok, nil
}

func (s *LoggingStore) LLen(ctx context.Context, key string) (int64, error) {
	return s.underlying.LLen(ctx, key)
}

func (s *LoggingStore) LRange(ctx context.Context, key string, start, stop int64) ([][]byte, error) {
	return s.underlying.LRange(ctx, key, start, stop)
}

func (s *LoggingStore) LIndex(ctx context.Context, key string, index int64) ([]byte, bool, error) {
	return s.underlying.LIndex(ctx, key, index)
}

func normalizeTTL(ttl, defaultTTL, maxTTL time.Duration) time.Duration {
	if ttl <= 0 {
		ttl = defaultTTL
	}
	if ttl > maxTTL {
		ttl = maxTTL
	}
	return ttl
}

// ApplyRecordToStore applies one AOF record (MOLE.*) to the provided store.
// It uses absolute expireAt timestamps stored in the log.
func ApplyRecordToStore(ctx context.Context, store core.Store, args [][]byte) error {
	if len(args) == 0 {
		return nil
	}
	switch strings.ToUpper(string(args[0])) {
	case "MOLE.SETAT":
		if len(args) != 4 {
			return errors.New("mole: bad aof record MOLE.SETAT")
		}
		expireAtMs, err := strconv.ParseInt(string(args[3]), 10, 64)
		if err != nil {
			return err
		}
		ttl := time.Until(time.UnixMilli(expireAtMs))
		if ttl <= 0 {
			_, _ = store.Del(ctx, string(args[1]))
			return nil
		}
		return store.SetWithTTL(ctx, string(args[1]), args[2], ttl)

	case "MOLE.EXPIREAT":
		if len(args) != 3 {
			return errors.New("mole: bad aof record MOLE.EXPIREAT")
		}
		expireAtMs, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return err
		}
		ttl := time.Until(time.UnixMilli(expireAtMs))
		if ttl <= 0 {
			_, _ = store.Del(ctx, string(args[1]))
			return nil
		}
		_, err = store.Expire(ctx, string(args[1]), ttl)
		return err

	case "MOLE.DEL":
		if len(args) != 2 {
			return errors.New("mole: bad aof record MOLE.DEL")
		}
		_, err := store.Del(ctx, string(args[1]))
		return err

	case "MOLE.HSET":
		if len(args) != 4 {
			return errors.New("mole: bad aof record MOLE.HSET")
		}
		_, err := store.HSet(ctx, string(args[1]), string(args[2]), args[3])
		return err

	case "MOLE.HDEL":
		if len(args) != 3 {
			return errors.New("mole: bad aof record MOLE.HDEL")
		}
		_, err := store.HDel(ctx, string(args[1]), []string{string(args[2])})
		return err

	case "MOLE.LPUSH":
		if len(args) != 3 {
			return errors.New("mole: bad aof record MOLE.LPUSH")
		}
		_, err := store.LPush(ctx, string(args[1]), [][]byte{args[2]})
		return err

	case "MOLE.RPUSH":
		if len(args) != 3 {
			return errors.New("mole: bad aof record MOLE.RPUSH")
		}
		_, err := store.RPush(ctx, string(args[1]), [][]byte{args[2]})
		return err

	case "MOLE.LPOP":
		if len(args) != 2 {
			return errors.New("mole: bad aof record MOLE.LPOP")
		}
		_, _, err := store.LPop(ctx, string(args[1]))
		return err

	case "MOLE.RPOP":
		if len(args) != 2 {
			return errors.New("mole: bad aof record MOLE.RPOP")
		}
		_, _, err := store.RPop(ctx, string(args[1]))
		return err

	default:
		// Ignore unknown records for forward compatibility.
		return nil
	}
}
