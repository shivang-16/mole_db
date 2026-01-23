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
	"mole/internal/server"
)

func main() {
	cfg := config.Default()
	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "TCP address to listen on")
	flag.DurationVar(&cfg.IdleTimeout, "idle-timeout", cfg.IdleTimeout, "idle timeout for client connections")
	flag.DurationVar(&cfg.DefaultTTL, "default-ttl", cfg.DefaultTTL, "default TTL applied to SET without EX/PX")
	flag.DurationVar(&cfg.MaxTTL, "max-ttl", cfg.MaxTTL, "hard cap for any TTL (EX/PX/EXPIRE)")
	flag.DurationVar(&cfg.JanitorInterval, "janitor-interval", cfg.JanitorInterval, "how often to actively delete expired keys")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store := core.NewMemoryStore(core.MemoryStoreOptions{
		DefaultTTL: cfg.DefaultTTL,
		MaxTTL:     cfg.MaxTTL,
	})
	store.StartJanitor(ctx, cfg.JanitorInterval)
	handler := commands.NewHandler(store)

	srv := server.New(server.Config{
		Addr:        cfg.Addr,
		IdleTimeout: cfg.IdleTimeout,
	}, handler)

	if err := srv.ListenAndServe(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
