package config

import "time"

type Config struct {
	Addr            string
	IdleTimeout     time.Duration
	DefaultTTL      time.Duration
	MaxTTL          time.Duration
	JanitorInterval time.Duration

	// AOF persistence (optional).
	AOFEnabled    bool
	AOFPath       string
	AOFSyncPolicy string // "always" | "everysec" | "no"

	// Memory eviction
	MaxMemory       int64  // max memory in bytes (0 = no limit)
	MaxMemoryPolicy string // "noeviction" | "allkeys-lru"

	// Replication
	Role       string // "master" | "replica"
	MasterAddr string // for replicas: master address to connect to
}

func Default() Config {
	return Config{
		Addr:        "127.0.0.1:7379",
		IdleTimeout: 5 * time.Minute,
		// Cache policy (initial milestone):
		// - Keys without explicit TTL get DefaultTTL.
		// - Any TTL requested above MaxTTL is capped.
		DefaultTTL:      20 * 24 * time.Hour,
		MaxTTL:          20 * 24 * time.Hour,
		JanitorInterval: 500 * time.Millisecond,

		AOFEnabled:    false,
		AOFPath:       "mole.aof",
		AOFSyncPolicy: "everysec",

		MaxMemory:       0,             // 0 = no limit (disable eviction)
		MaxMemoryPolicy: "allkeys-lru", // default eviction policy

		Role:       "master", // default to master
		MasterAddr: "",       // empty = not a replica
	}
}
