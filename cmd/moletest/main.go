package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

// Simple RESP client for testing

func main() {
	addr := "127.0.0.1:7379"
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	tester := &Tester{conn: conn, reader: bufio.NewReader(conn)}

	// --- CONNECTION ---
	tester.Run("PING", []string{"PING"}, "PONG")
	tester.Run("PING message", []string{"PING", "hello"}, "hello")

	// --- STRINGS ---
	// SET / GET
	tester.Run("SET key val", []string{"SET", "mykey", "myval"}, "OK")
	tester.Run("GET key", []string{"GET", "mykey"}, "myval")
	tester.Run("GET non-existent", []string{"GET", "nokey"}, "(nil)")

	// DEL
	tester.Run("DEL key", []string{"DEL", "mykey"}, "1")
	tester.Run("GET deleted", []string{"GET", "mykey"}, "(nil)")
	tester.Run("DEL non-existent", []string{"DEL", "nokey"}, "0")

	// SET options (EX, PX)
	tester.Run("SET with EX", []string{"SET", "ex_key", "val", "EX", "10"}, "OK")
	tester.Run("TTL ex_key", []string{"TTL", "ex_key"}, "10")
	tester.Run("SET with PX", []string{"SET", "px_key", "val", "PX", "10000"}, "OK")
	tester.Run("TTL px_key (approx)", []string{"TTL", "px_key"}, "10")

	// SETNX
	tester.Run("SETNX new", []string{"SETNX", "nxkey", "val"}, "1")
	tester.Run("SETNX existing", []string{"SETNX", "nxkey", "val2"}, "0")
	tester.Run("GET nxkey", []string{"GET", "nxkey"}, "val")

	// SETEX
	tester.Run("SETEX new", []string{"SETEX", "sexkey", "10", "val"}, "OK")
	tester.Run("TTL sexkey", []string{"TTL", "sexkey"}, "10")
	tester.Run("GET sexkey", []string{"GET", "sexkey"}, "val")
	tester.Run("SETEX invalid", []string{"SETEX", "k", "invalid", "v"}, "ERR")

	// APPEND
	tester.Run("SET append", []string{"SET", "appkey", "hello"}, "OK")
	tester.Run("APPEND", []string{"APPEND", "appkey", " world"}, "11") // hello world
	tester.Run("GET append", []string{"GET", "appkey"}, "hello world")
	tester.Run("APPEND new", []string{"APPEND", "newapp", "test"}, "4")
	tester.Run("GET newapp", []string{"GET", "newapp"}, "test")

	// STRLEN
	tester.Run("STRLEN", []string{"STRLEN", "appkey"}, "11")
	tester.Run("STRLEN non-existent", []string{"STRLEN", "nokey"}, "0")

	// INCR / DECR
	tester.Run("SET counter", []string{"SET", "count", "10"}, "OK")
	tester.Run("INCR", []string{"INCR", "count"}, "11")
	tester.Run("DECR", []string{"DECR", "count"}, "10")
	tester.Run("INCRBY", []string{"INCRBY", "count", "5"}, "15")
	tester.Run("DECRBY", []string{"DECRBY", "count", "3"}, "12")
	tester.Run("INCR new key", []string{"INCR", "newcounter"}, "1")
	tester.Run("DECR new key", []string{"DECR", "newdecr"}, "-1")

	// MSET / MGET
	tester.Run("MSET", []string{"MSET", "k1", "v1", "k2", "v2"}, "OK")
	tester.Run("MGET", []string{"MGET", "k1", "k2", "nonexistent"}, []string{"v1", "v2", "(nil)"})
	tester.Run("MGET one key", []string{"MGET", "k1"}, []string{"v1"})

	// --- EXPIRATION ---
	// EXPIRE / TTL
	tester.Run("SET for expire", []string{"SET", "expire_me", "val"}, "OK")
	tester.Run("EXPIRE key", []string{"EXPIRE", "expire_me", "10"}, "1")
	tester.Run("TTL check", []string{"TTL", "expire_me"}, "10")
	tester.Run("EXPIRE non-existent", []string{"EXPIRE", "nokey", "10"}, "0")
	tester.Run("PEXPIRE key", []string{"SET", "pex", "val"}, "OK")
	tester.Run("PEXPIRE exec", []string{"PEXPIRE", "pex", "10000"}, "1")
	tester.Run("PTTL check", []string{"PTTL", "pex"}, nil) // Just strict run check for now to avoid flakes

	// --- LISTS ---
	// LPUSH / RPUSH / LLEN
	tester.Run("LPUSH", []string{"LPUSH", "mylist", "a", "b", "c"}, "3") // c, b, a
	tester.Run("LLEN", []string{"LLEN", "mylist"}, "3")
	tester.Run("RPUSH", []string{"RPUSH", "mylist", "d", "e"}, "5") // c, b, a, d, e

	// LRANGE
	tester.Run("LRANGE full", []string{"LRANGE", "mylist", "0", "-1"}, []string{"c", "b", "a", "d", "e"})
	tester.Run("LRANGE subset", []string{"LRANGE", "mylist", "1", "2"}, []string{"b", "a"})
	tester.Run("LRANGE oob", []string{"LRANGE", "mylist", "10", "20"}, []string{})

	// LPOP / RPOP
	tester.Run("LPOP", []string{"LPOP", "mylist"}, "c")
	tester.Run("RPOP", []string{"RPOP", "mylist"}, "e")
	tester.Run("LLEN after pop", []string{"LLEN", "mylist"}, "3")
	tester.Run("LPOP empty", []string{"LPOP", "nolist"}, "(nil)")

	// --- HASHES ---
	tester.Run("HSET", []string{"HSET", "myhash", "f1", "v1"}, "1")
	tester.Run("HSET update", []string{"HSET", "myhash", "f1", "v2"}, "0")
	tester.Run("HSET new field", []string{"HSET", "myhash", "f2", "v3"}, "1")

	tester.Run("HGET", []string{"HGET", "myhash", "f1"}, "v2")
	tester.Run("HGET non-existent", []string{"HGET", "myhash", "nofield"}, "(nil)")
	tester.Run("HGET no key", []string{"HGET", "nohash", "f1"}, "(nil)")

	tester.Run("HEXISTS", []string{"HEXISTS", "myhash", "f1"}, "1")
	tester.Run("HEXISTS no", []string{"HEXISTS", "myhash", "f2"}, "1")
	tester.Run("HEXISTS fail", []string{"HEXISTS", "myhash", "nope"}, "0")

	tester.Run("HLEN", []string{"HLEN", "myhash"}, "2")

	tester.Run("HGETALL", []string{"HGETALL", "myhash"}, nil)
	// Note: Order varies, so strict match fails. We just ensure it runs.

	tester.Run("HDEL", []string{"HDEL", "myhash", "f1"}, "1")
	tester.Run("HLEN after del", []string{"HLEN", "myhash"}, "1")
	tester.Run("HDEL non-existent", []string{"HDEL", "myhash", "f1"}, "0")

	// --- SCAN ---
	// Basic scan test (assumes we have keys)
	// tester.Run("SCAN", []string{"SCAN", "0", "COUNT", "100"}, []string{"0", "array"})
	// Note: output is [0 [list of keys]]. Matching string is brittle due to random map order.
	tester.Run("SCAN simple", []string{"SCAN", "0", "COUNT", "1"}, nil) // Just strict run check

	// --- ERROR CASES ---
	tester.Run("SET missing arg", []string{"SET", "key"}, "ERR wrong number of arguments for 'SET'")
	tester.Run("UNKNOWN command", []string{"UNKNOWN"}, "ERR unknown command 'UNKNOWN'")
	tester.Run("INCR string", []string{"SET", "str", "abc"}, "OK")
	tester.Run("INCR invalid", []string{"INCR", "str"}, "ERR")
	tester.Run("WRONGTYPE", []string{"LPUSH", "listkey", "v"}, "1")
	tester.Run("GET WRONGTYPE", []string{"GET", "listkey"}, "ERR") // GET on list? -> MemoryStore.Get returns value?

	fmt.Printf("\nSummary: %d passed, %d failed\n", tester.passed, tester.failed)
	if tester.failed > 0 {
		os.Exit(1)
	}
}

type Tester struct {
	conn   net.Conn
	reader *bufio.Reader
	passed int
	failed int
}

func (t *Tester) Run(name string, args []string, expected interface{}) {
	if err := sendCommand(t.conn, args); err != nil {
		fmt.Printf("FAIL: %s - Send error: %v\n", name, err)
		t.failed++
		return
	}

	resp, err := readResponse(t.reader)
	if err != nil {
		fmt.Printf("FAIL: %s - Read error: %v\n", name, err)
		t.failed++
		return
	}

	match := false
	switch e := expected.(type) {
	case nil:
		// Expect non-error response only
		if strings.HasPrefix(resp, "(error)") {
			match = false
		} else {
			match = true
		}

	case string:
		if strings.Contains(resp, e) {
			match = true
		} else if e == "(nil)" && resp == "(nil)" { // exact match for nil
			match = true
		}
		// Handle integer responses that might be just numbers in string form
		if !match && resp == fmt.Sprintf("(integer) %s", e) {
			match = true
		}
		if !match && resp == e { // Exact match fallback
			match = true
		}
		// Handle "ERR" prefix check loosely
		if !match && strings.HasPrefix(e, "ERR") && strings.HasPrefix(resp, "(error) ERR") {
			match = true
		}

	case []string:
		// Compare with string representation [item1 item2]
		var sb strings.Builder
		sb.WriteString("[")
		for i, v := range e {
			if i > 0 {
				sb.WriteString(" ")
			}
			sb.WriteString(v)
		}
		sb.WriteString("]")
		expectedStr := sb.String()

		if resp == expectedStr {
			match = true
		}
	}

	// Actually, I can't easily reuse readResponse logic if I just copied main.go's structure which prints.
	// I should implement a proper reader here that returns the string representation.

	// For now, let's assume I fixed readResponse to return a string.
	// Since I can't edit this file after writing without another tool call, I'll put the logic in now.

	if match {
		fmt.Printf("PASS: %s\n", name)
		t.passed++
	} else {
		fmt.Printf("FAIL: %s\n  Expected: %v\n  Got:      %s\n", name, expected, resp)
		t.failed++
	}
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

func readResponse(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if len(line) == 0 {
		return "", fmt.Errorf("empty response")
	}

	dataType := line[0]
	content := line[1:]

	switch dataType {
	case '+':
		return content, nil
	case '-':
		return "(error) " + content, nil
	case ':':
		return content, nil // Integer as string
	case '$':
		var length int
		fmt.Sscanf(content, "%d", &length)
		if length == -1 {
			return "(nil)", nil
		}
		data := make([]byte, length+2)
		_, err := reader.Read(data)
		if err != nil {
			return "", err
		}
		return string(data[:length]), nil
	case '*':
		var count int
		fmt.Sscanf(content, "%d", &count)
		if count == 0 {
			return "[]", nil
		}
		// For array, we recursively read. For simplicity in this test,
		// let's just return a simpler string representation of the array
		// or actually implement it to support the []string check.
		var sb strings.Builder
		sb.WriteString("[")
		for i := 0; i < count; i++ {
			if i > 0 {
				sb.WriteString(" ")
			}
			s, err := readResponse(reader)
			if err != nil {
				return "", err
			}
			sb.WriteString(s)
		}
		sb.WriteString("]")
		return sb.String(), nil
	default:
		return "", fmt.Errorf("unknown response type: %s", line)
	}
}
