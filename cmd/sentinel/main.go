package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mole/internal/sentinel"
)

func main() {
	var (
		id         = flag.String("id", "sentinel-1", "sentinel ID")
		masterAddr = flag.String("master", "127.0.0.1:7379", "master address to monitor")
		replicas   = flag.String("replicas", "", "comma-separated replica addresses")
		peers      = flag.String("peers", "", "comma-separated peer sentinel addresses")
		heartbeat  = flag.Duration("heartbeat", 1*time.Second, "heartbeat interval")
		downAfter  = flag.Duration("down-after", 5*time.Second, "time before considering master down")
	)
	flag.Parse()

	var replicaAddrs []string
	if *replicas != "" {
		replicaAddrs = strings.Split(*replicas, ",")
		for i := range replicaAddrs {
			replicaAddrs[i] = strings.TrimSpace(replicaAddrs[i])
		}
	}

	var peerAddrs []string
	if *peers != "" {
		peerAddrs = strings.Split(*peers, ",")
		for i := range peerAddrs {
			peerAddrs[i] = strings.TrimSpace(peerAddrs[i])
		}
	}

	cfg := sentinel.SentinelConfig{
		ID:                *id,
		MasterAddr:        *masterAddr,
		ReplicaAddrs:      replicaAddrs,
		Peers:             peerAddrs,
		HeartbeatInterval: *heartbeat,
		DownAfter:         *downAfter,
	}

	s := sentinel.NewSentinel(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("sentinel %s monitoring master %s\n", *id, *masterAddr)
	if err := s.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
