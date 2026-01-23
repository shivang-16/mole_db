package config

import "time"

type Config struct {
	Addr            string
	IdleTimeout     time.Duration
	DefaultTTL      time.Duration
	MaxTTL          time.Duration
	JanitorInterval time.Duration
}

func Default() Config {
	return Config{
		// Use a Redis-adjacent but non-default port to avoid collisions
		// with a local Redis instance (which commonly uses 7379).
		Addr:        "127.0.0.1:7379",
		IdleTimeout: 5 * time.Minute,
		// Cache policy (initial milestone):
		// - Keys without explicit TTL get DefaultTTL.
		// - Any TTL requested above MaxTTL is capped.
		DefaultTTL:      20 * 24 * time.Hour,
		MaxTTL:          20 * 24 * time.Hour,
		JanitorInterval: 500 * time.Millisecond,
	}
}
