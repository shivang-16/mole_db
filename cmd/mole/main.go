package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mole/internal/commands"
	"mole/internal/config"
	"mole/internal/core"
	"mole/internal/persistence/aof"
	"mole/internal/replication"
	"mole/internal/server"
)

func main() {
	cfg := config.Default()
	interactive := flag.Bool("i", false, "start server and enter interactive mode")
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

	if *interactive {
		runInteractive(&cfg)
		return
	}

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

func runInteractive(cfg *config.Config) {
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

	go func() {
		if err := srv.ListenAndServe(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", cfg.Addr, 2*time.Second)
	if err != nil {
		fmt.Printf("Could not connect to Mole DB at %s: %v\n", cfg.Addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("mole-cli connected to %s\n", cfg.Addr)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("%s> ", cfg.Addr)
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}
		if strings.ToLower(input) == "exit" || strings.ToLower(input) == "quit" {
			break
		}

		args := strings.Fields(input)
		if err := sendCommand(conn, args); err != nil {
			fmt.Printf("Error sending command: %v\n", err)
			continue
		}
		readResponse(conn)
	}

	cancel()
}

func sendCommand(conn net.Conn, args []string) error {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	_, err := conn.Write([]byte(sb.String()))
	return err
}

func readResponse(conn net.Conn) {
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Error reading response: %v\n", err)
		return
	}
	line = strings.TrimSuffix(line, "\r\n")
	if len(line) == 0 {
		return
	}

	dataType := line[0]
	content := line[1:]

	switch dataType {
	case '+':
		fmt.Println(content)
	case '-':
		fmt.Printf("(error) %s\n", content)
	case ':':
		fmt.Printf("(integer) %s\n", content)
	case '$':
		var length int
		fmt.Sscanf(content, "%d", &length)
		if length == -1 {
			fmt.Println("(nil)")
			return
		}
		data := make([]byte, length+2)
		_, err := reader.Read(data)
		if err != nil {
			fmt.Printf("Error reading bulk string: %v\n", err)
			return
		}
		fmt.Printf("\"%s\"\n", string(data[:length]))
	case '*':
		var count int
		fmt.Sscanf(content, "%d", &count)
		if count == 0 {
			fmt.Println("(empty array)")
			return
		}
		for i := 0; i < count; i++ {
			fmt.Printf("%d) ", i+1)
			readResponse(conn)
		}
	default:
		fmt.Printf("Unknown response type: %s\n", line)
	}
}
