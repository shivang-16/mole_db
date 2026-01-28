package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
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
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "server":
			runServerCommand()
			return
		case "connect":
			runConnectCommand()
			return
		case "help", "--help", "-h":
			printHelp()
			return
		case "version", "--version", "-v":
			fmt.Println("Mole DB v0.1.4")
			return
		}
	}

	// Default behavior: interactive mode (server + client)
	runDefaultCommand()
}

func runServerCommand() {
	cfg := config.Default()
	daemon := flag.Bool("d", false, "run server in daemon mode")
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

	// Skip "server" argument for flag parsing
	os.Args = os.Args[1:]
	flag.Parse()

	if *daemon {
		runDaemon(&cfg)
		return
	}

	// Default server behavior: interactive mode (server + client)
	runInteractiveServer(&cfg)
}

func runConnectCommand() {
	host := flag.String("h", "127.0.0.1", "Mole DB Server Host")
	port := flag.Int("p", 7379, "Mole DB Server Port")

	// Skip "connect" argument for flag parsing
	os.Args = os.Args[1:]
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *host, *port)

	// Try to connect to existing server
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		fmt.Printf("No server found at %s, starting interactive mode...\n\n", addr)
		// Fallback to interactive mode
		cfg := config.Default()
		cfg.Addr = addr
		runInteractiveServer(&cfg)
		return
	}
	conn.Close()

	// Server exists, connect as client
	// If args provided (non-interactive mode)
	if len(flag.Args()) > 0 {
		runSingleCommand(addr, flag.Args())
		return
	}

	// Interactive client mode
	runInteractiveClient(addr)
}

func runDefaultCommand() {
	cfg := config.Default()
	daemon := flag.Bool("d", false, "run server in daemon mode (no interactive shell)")
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

	if *daemon {
		runDaemon(&cfg)
		return
	}

	// Default: interactive mode (server + client)
	runInteractiveServer(&cfg)
}

func printHelp() {
	fmt.Println("Mole DB - Fast, lightweight in-memory database")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  mole                    Start interactive mode (server + client)")
	fmt.Println("  mole server [options]   Start server (interactive by default)")
	fmt.Println("  mole connect [options]  Connect to existing server (fallback to interactive)")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  mole                    # Interactive mode (easiest)")
	fmt.Println("  mole server             # Same as above")
	fmt.Println("  mole -d                 # Daemon server")
	fmt.Println("  mole server -d          # Daemon server")
	fmt.Println("  mole connect            # Connect to localhost:7379 or start interactive")
	fmt.Println("  mole connect -h remote -p 8080  # Connect to remote server")
	fmt.Println()
	fmt.Println("Server Options:")
	fmt.Println("  -addr string            TCP address (default \"127.0.0.1:7379\")")
	fmt.Println("  -d                      Daemon mode (no interactive shell)")
	fmt.Println("  -aof                    Enable AOF persistence")
	fmt.Println("  -maxmemory int          Max memory in bytes (0 = no limit)")
	fmt.Println("  -role string            Server role: master|replica")
	fmt.Println()
	fmt.Println("Connect Options:")
	fmt.Println("  -h string               Server host (default \"127.0.0.1\")")
	fmt.Println("  -p int                  Server port (default 7379)")
	fmt.Println()
	fmt.Println("For full options: mole server -help or mole connect -help")
}

func runDaemon(cfg *config.Config) {
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

func runInteractiveServer(cfg *config.Config) {
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

	fmt.Println()
	fmt.Println("Current Mole Log ID:", generateLogID())
	fmt.Printf("Connecting to:       \x1b[32mmole://%s\x1b[0m\n", cfg.Addr)
	fmt.Println("Using Mole DB:       0.1.4")
	fmt.Println()
	fmt.Println("------")
	fmt.Println("   For Mole DB documentation see: https://github.com/shivang-16/mole_db")
	fmt.Println("------")
	fmt.Println()

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

		args := parseInput(input)
		if len(args) == 0 {
			continue
		}

		// Check if entering subscription mode
		cmd := strings.ToUpper(args[0])
		if cmd == "SUB" || cmd == "PSUB" || cmd == "MOLE.SUB" || cmd == "MOLE.PSUB" {
			if err := sendCommand(conn, args); err != nil {
				fmt.Printf("Error sending command: %v\n", err)
				continue
			}
			fmt.Println("Entering subscription mode. Press Ctrl+C to exit.")
			handleSubscriptionMode(conn)
			break
		}

		if err := sendCommand(conn, args); err != nil {
			fmt.Printf("Error sending command: %v\n", err)
			continue
		}
		readResponse(conn)
	}

	cancel()
}

func runInteractiveClient(addr string) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		fmt.Printf("Could not connect to Mole DB at %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Connected to %s\n", addr)
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Printf("%s> ", addr)
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

		// Parse input respecting quoted strings.
		args := parseInput(input)
		if len(args) == 0 {
			continue
		}

		// Check if entering subscription mode
		cmd := strings.ToUpper(args[0])
		if cmd == "SUB" || cmd == "PSUB" || cmd == "MOLE.SUB" || cmd == "MOLE.PSUB" {
			if err := sendCommand(conn, args); err != nil {
				fmt.Printf("Error sending command: %v\n", err)
				continue
			}
			fmt.Println("Entering subscription mode. Press Ctrl+C to exit.")
			handleSubscriptionMode(conn)
			break
		}

		if err := sendCommand(conn, args); err != nil {
			fmt.Printf("Error sending command: %v\n", err)
			continue
		}
		readResponse(conn)
	}
}

func runSingleCommand(addr string, args []string) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		fmt.Printf("Could not connect to Mole DB at %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	if err := sendCommand(conn, args); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	readResponse(conn)
}

// parseInput tokenizes user input respecting quoted strings.
func parseInput(input string) []string {
	var args []string
	i := 0
	n := len(input)

	for i < n {
		// Skip whitespace.
		for i < n && (input[i] == ' ' || input[i] == '\t') {
			i++
		}
		if i >= n {
			break
		}

		var token string
		switch input[i] {
		case '"':
			token, i = parseDoubleQuoted(input, i)
		case '\'':
			token, i = parseSingleQuoted(input, i)
		default:
			token, i = parseUnquoted(input, i)
		}
		args = append(args, token)
	}
	return args
}

// parseDoubleQuoted parses a double-quoted string with escape sequences.
func parseDoubleQuoted(input string, start int) (string, int) {
	n := len(input)
	i := start + 1
	var result strings.Builder

	for i < n {
		ch := input[i]
		if ch == '"' {
			return result.String(), i + 1
		}
		if ch == '\\' && i+1 < n {
			i++
			switch input[i] {
			case 'n':
				result.WriteByte('\n')
			case 'r':
				result.WriteByte('\r')
			case 't':
				result.WriteByte('\t')
			case '\\':
				result.WriteByte('\\')
			case '"':
				result.WriteByte('"')
			default:
				result.WriteByte(input[i])
			}
		} else {
			result.WriteByte(ch)
		}
		i++
	}
	return result.String(), i
}

// parseSingleQuoted parses a single-quoted string (no escape processing).
func parseSingleQuoted(input string, start int) (string, int) {
	n := len(input)
	i := start + 1
	var result strings.Builder

	for i < n {
		if input[i] == '\'' {
			return result.String(), i + 1
		}
		result.WriteByte(input[i])
		i++
	}
	return result.String(), i
}

// parseUnquoted parses an unquoted token until whitespace.
func parseUnquoted(input string, start int) (string, int) {
	n := len(input)
	i := start
	for i < n && input[i] != ' ' && input[i] != '\t' {
		i++
	}
	return input[start:i], i
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

func generateLogID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// handleSubscriptionMode handles receiving messages in subscription mode.
func handleSubscriptionMode(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		// Read array response
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("\nConnection closed: %v\n", err)
			return
		}

		line = strings.TrimSuffix(line, "\r\n")
		if len(line) == 0 || line[0] != '*' {
			continue
		}

		var count int
		fmt.Sscanf(line[1:], "%d", &count)

		if count < 2 {
			continue
		}

		// Read elements
		elements := make([]string, count)
		for i := 0; i < count; i++ {
			elem, err := readBulkString(reader)
			if err != nil {
				fmt.Printf("Error reading element: %v\n", err)
				return
			}
			elements[i] = elem
		}

		// Format output based on message type
		if len(elements) >= 2 {
			msgType := elements[0]
			switch msgType {
			case "subscribe", "psubscribe":
				channel := elements[1]
				subCount := "(unknown)"
				if len(elements) >= 3 {
					subCount = elements[2]
				}
				fmt.Printf("Subscribed to %s (total: %s)\n", channel, subCount)

			case "unsubscribe", "punsubscribe":
				channel := elements[1]
				subCount := "(unknown)"
				if len(elements) >= 3 {
					subCount = elements[2]
				}
				fmt.Printf("Unsubscribed from %s (total: %s)\n", channel, subCount)

			case "message":
				if len(elements) >= 3 {
					channel := elements[1]
					payload := elements[2]
					timestamp := time.Now().Format("15:04:05")
					fmt.Printf("[%s] %s: %s\n", timestamp, channel, payload)
				}

			case "pmessage":
				if len(elements) >= 4 {
					pattern := elements[1]
					channel := elements[2]
					payload := elements[3]
					timestamp := time.Now().Format("15:04:05")
					fmt.Printf("[%s] %s (pattern: %s): %s\n", timestamp, channel, pattern, payload)
				}

			case "pong":
				fmt.Println("PONG")

			default:
				fmt.Printf("Unknown message type: %s\n", msgType)
			}
		}
	}
}

// readBulkString reads a RESP bulk string from the reader.
func readBulkString(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	line = strings.TrimSuffix(line, "\r\n")
	if len(line) == 0 || line[0] != '$' {
		return "", fmt.Errorf("expected bulk string, got: %s", line)
	}

	var length int
	fmt.Sscanf(line[1:], "%d", &length)

	if length == -1 {
		return "(nil)", nil
	}

	// Read exact bytes + CRLF
	data := make([]byte, length+2)
	_, err = reader.Read(data)
	if err != nil {
		return "", err
	}

	return string(data[:length]), nil
}
