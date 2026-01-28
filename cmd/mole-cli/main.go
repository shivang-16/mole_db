package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// RESP Constants
const (
	SimpleString = '+'
	Error        = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

func main() {
	host := flag.String("h", "127.0.0.1", "Mole DB Server Host")
	port := flag.Int("p", 7379, "Mole DB Server Port")
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *host, *port)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		fmt.Printf("Could not connect to Mole DB at %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	// If args provided (non-interactive mode)
	if len(flag.Args()) > 0 {
		cmd := flag.Args()
		if err := sendCommand(conn, cmd); err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		readResponse(conn)
		return
	}

	// Interactive Mode
	fmt.Printf("mole-cli connected to %s\n", addr)
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
		if err := sendCommand(conn, args); err != nil {
			fmt.Printf("Error sending command: %v\n", err)
			continue
		}
		readResponse(conn)
	}
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
	// Unclosed quote - return what we have.
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
	// Construct RESP Array
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
	// Trim CRLF
	line = strings.TrimSuffix(line, "\r\n")
	if len(line) == 0 {
		return
	}

	dataType := line[0]
	content := line[1:]

	switch dataType {
	case SimpleString:
		fmt.Println(content)
	case Error:
		fmt.Printf("(error) %s\n", content)
	case Integer:
		fmt.Printf("(integer) %s\n", content)
	case BulkString:
		// Format: $5\r\nhello\r\n
		// The 'line' variable currently holds "$5"
		// We need to parse length, then read the content
		// Ideally we would reuse a robust RESP parser, but for a simple CLI this suffices
		var length int
		fmt.Sscanf(content, "%d", &length)
		if length == -1 {
			fmt.Println("(nil)")
			return
		}
		// Read exact bytes + CRLF
		data := make([]byte, length+2)
		_, err := reader.Read(data)
		if err != nil {
			fmt.Printf("Error reading bulk string: %v\n", err)
			return
		}
		fmt.Printf("\"%s\"\n", string(data[:length])) // trim CRLF for display
	case Array:
		// Recurse or print raw for now (handling arrays in a simple CLI reader is complex without a full parser)
		// For MVP, if it returns an array (like KEYS *), we just print headers or basic items
		// Implementing full array recursion here would duplicate the server's protocol code.
		// For now, let's just print "Array support limited in basic CLI" or try to read N lines
		var count int
		fmt.Sscanf(content, "%d", &count)
		if count == 0 {
			fmt.Println("(empty array)")
			return
		}
		for i := 0; i < count; i++ {
			fmt.Printf("%d) ", i+1)
			readResponse(conn) // Recursive-ish call for elements
		}
	default:
		fmt.Printf("Unknown response type: %s\n", line)
	}
}
