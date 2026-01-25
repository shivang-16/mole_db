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
	expireAtMs := s.now().Add(ttl).UnixMilli()

	if err := s.underlying.Set(ctx, key, value); err != nil {
		return err
	}
	return s.w.AppendRecord(RecordSetAt(key, value, expireAtMs))
}

func (s *LoggingStore) SetWithTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	eff := normalizeTTL(ttl, s.defaultTTL, s.maxTTL)
	expireAtMs := s.now().Add(eff).UnixMilli()

	if err := s.underlying.SetWithTTL(ctx, key, value, ttl); err != nil {
		return err
	}
	return s.w.AppendRecord(RecordSetAt(key, value, expireAtMs))
}

func (s *LoggingStore) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	eff := normalizeTTL(ttl, s.defaultTTL, s.maxTTL)
	if eff <= 0 {
		return s.underlying.Expire(ctx, key, ttl)
	}
	expireAtMs := s.now().Add(eff).UnixMilli()

	updated, err := s.underlying.Expire(ctx, key, ttl)
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

	default:
		// Ignore unknown records for forward compatibility.
		return nil
	}
}
