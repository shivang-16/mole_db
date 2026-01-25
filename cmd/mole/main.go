package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"mole/internal/commands"
	"mole/internal/config"
	"mole/internal/core"
	"mole/internal/persistence/aof"
	"mole/internal/replication"
	"mole/internal/server"
)

func main() {
	cfg := config.Default()
	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "TCP address to listen on")
	flag.DurationVar(&cfg.IdleTimeout, "idle-timeout", cfg.IdleTimeout, "idle timeout for client connections")
	flag.DurationVar(&cfg.DefaultTTL, "default-ttl", cfg.DefaultTTL, "default TTL applied to SET without EX/PX")
	flag.DurationVar(&cfg.MaxTTL, "max-ttl", cfg.MaxTTL, "hard cap for any TTL (EX/PX/EXPIRE)")
	flag.DurationVar(&cfg.JanitorInterval, "janitor-interval", cfg.JanitorInterval, "how often to actively delete expired keys")
	flag.BoolVar(&cfg.AOFEnabled, "aof", cfg.AOFEnabled, "enable AOF persistence")
	flag.StringVar(&cfg.AOFPath, "aof-path", cfg.AOFPath, "AOF file path")
	flag.StringVar(&cfg.AOFSyncPolicy, "aof-fsync", cfg.AOFSyncPolicy, "AOF fsync policy: always|everysec|no")
	flag.Int64Var(&cfg.MaxMemory, "maxmemory", cfg.MaxMemory, "max memory in bytes (0 = no limit)")
	flag.StringVar(&cfg.MaxMemoryPolicy, "maxmemory-policy", cfg.MaxMemoryPolicy, "eviction policy: noeviction|allkeys-lru")
	flag.StringVar(&cfg.Role, "role", cfg.Role, "server role: master|replica")
	flag.StringVar(&cfg.MasterAddr, "master-addr", cfg.MasterAddr, "master address (required for replica)")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	memStore := core.NewMemoryStore(core.MemoryStoreOptions{
		DefaultTTL: cfg.DefaultTTL,
		MaxTTL:     cfg.MaxTTL,
		MaxMemory:  cfg.MaxMemory,
		Policy:     cfg.MaxMemoryPolicy,
	})
	memStore.StartJanitor(ctx, cfg.JanitorInterval)

	var store core.Store = memStore

	if cfg.AOFEnabled {
		// Replay existing AOF into memory before serving clients.
		if err := aof.Replay(ctx, cfg.AOFPath, func(ctx context.Context, args [][]byte) error {
			return aof.ApplyRecordToStore(ctx, memStore, args)
		}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		w, err := aof.OpenWriter(aof.WriterOptions{
			Path:   cfg.AOFPath,
			Policy: aof.SyncPolicy(cfg.AOFSyncPolicy),
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		go func() {
			<-ctx.Done()
			_ = w.Close()
		}()

		loggingStore, err := aof.Wrap(store, w, aof.LoggingStoreOptions{
			DefaultTTL: cfg.DefaultTTL,
			MaxTTL:     cfg.MaxTTL,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		store = loggingStore
	}

	handler := commands.NewHandler(store)

	// Set up replication based on role.
	var master *replication.Master
	if cfg.Role == "master" {
		master = replication.NewMaster(memStore)
		handler.SetMaster(master)
	} else if cfg.Role == "replica" {
		if cfg.MasterAddr == "" {
			fmt.Fprintln(os.Stderr, "mole: -master-addr is required when -role=replica")
			os.Exit(1)
		}
		handler.SetReadOnly(true)
		replica := replication.NewReplica(cfg.MasterAddr, memStore)
		go func() {
			if err := replica.Start(ctx); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "mole: replica error: %v\n", err)
			}
		}()
	}

	srv := server.New(server.Config{
		Addr:        cfg.Addr,
		IdleTimeout: cfg.IdleTimeout,
	}, handler)
	if master != nil {
		srv.SetMaster(master)
	}

	if err := srv.ListenAndServe(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
