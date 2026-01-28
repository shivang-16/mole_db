package server

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"mole/internal/protocol/resp"
	"mole/internal/pubsub"
)

func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(s.cfg.IdleTimeout))

	rr := resp.NewReader(conn)
	wr := resp.NewWriter(conn)

	// Set deadline immediately to protect first command read from slowloris attacks.
	_ = conn.SetDeadline(time.Now().Add(s.cfg.IdleTimeout))

	// Check if this is a replica connection (first command is REPLICAOF).
	args, err := rr.ReadCommand()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return
		}
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		_ = wr.WriteError("ERR " + err.Error())
		_ = wr.Flush()
		return
	}

	// Check for REPLICAOF handshake.
	if len(args) > 0 && string(args[0]) == "REPLICAOF" {
		if s.master != nil {
			// This is a replica connection - hand it to master.
			_ = wr.WriteSimpleString("OK")
			_ = wr.Flush()
			if err := s.master.AddReplica(conn); err != nil {
				_ = wr.WriteError("ERR failed to add replica: " + err.Error())
				_ = wr.Flush()
				_ = conn.Close()
				return
			}
			return
		}
		_ = wr.WriteError("ERR not a master")
		_ = wr.Flush()
		return
	}

	// Check for subscription commands.
	if len(args) > 0 {
		cmd := strings.ToUpper(string(args[0]))
		if cmd == "SUB" || cmd == "PSUB" || cmd == "MOLE.SUB" || cmd == "MOLE.PSUB" {
			// Enter subscription mode
			s.handleSubscriptionMode(ctx, conn, rr, wr, args)
			return
		}
	}

	// Normal client connection - handle commands.
	reply := s.handler.Handle(ctx, args)
	if err := reply.WriteTo(wr); err != nil {
		return
	}
	if err := wr.Flush(); err != nil {
		return
	}

	_ = conn.SetDeadline(time.Now().Add(s.cfg.IdleTimeout))

	// Continue handling more commands from this client.
	for {
		select {
		case <-ctx.Done():
			_ = wr.WriteError("ERR server shutting down")
			_ = wr.Flush()
			return
		default:
		}

		args, err := rr.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			_ = wr.WriteError("ERR " + err.Error())
			_ = wr.Flush()
			return
		}

		// Check if entering subscription mode
		if len(args) > 0 {
			cmd := strings.ToUpper(string(args[0]))
			if cmd == "SUB" || cmd == "PSUB" || cmd == "MOLE.SUB" || cmd == "MOLE.PSUB" {
				s.handleSubscriptionMode(ctx, conn, rr, wr, args)
				return
			}
		}

		reply := s.handler.Handle(ctx, args)
		if err := reply.WriteTo(wr); err != nil {
			return
		}
		if err := wr.Flush(); err != nil {
			return
		}

		_ = conn.SetDeadline(time.Now().Add(s.cfg.IdleTimeout))
	}
}

// handleSubscriptionMode enters pub/sub mode for this connection.
func (s *Server) handleSubscriptionMode(ctx context.Context, conn net.Conn, rr *resp.Reader, wr *resp.Writer, firstCmd [][]byte) {
	pubsubMgr := s.handler.GetPubSubManager()
	sub := pubsub.NewSubscriber(1000)
	defer pubsubMgr.RemoveSubscriber(sub)

	// Handle first subscription command
	if !s.handleSubCommand(ctx, sub, pubsubMgr, wr, firstCmd) {
		return
	}

	// Start message delivery goroutine
	msgCtx, cancelMsg := context.WithCancel(ctx)
	defer cancelMsg()

	go func() {
		for {
			select {
			case <-msgCtx.Done():
				return
			case msg, ok := <-sub.Messages():
				if !ok {
					return
				}
				// Send message in pub/sub format: ["message", channel, payload]
				reply := resp.Array([]resp.Reply{
					resp.BulkString([]byte("message")),
					resp.BulkString([]byte(msg.Channel)),
					resp.BulkString(msg.Payload),
				})
				if err := reply.WriteTo(wr); err != nil {
					return
				}
				_ = wr.Flush()
			}
		}
	}()

	// Handle subscription commands in a loop
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_ = conn.SetDeadline(time.Now().Add(s.cfg.IdleTimeout))

		args, err := rr.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			_ = wr.WriteError("ERR " + err.Error())
			_ = wr.Flush()
			return
		}

		if !s.handleSubCommand(ctx, sub, pubsubMgr, wr, args) {
			return
		}
	}
}

// handleSubCommand processes a single subscription command.
// Returns false if connection should close.
func (s *Server) handleSubCommand(ctx context.Context, sub *pubsub.Subscriber, mgr *pubsub.Manager, wr *resp.Writer, args [][]byte) bool {
	if len(args) == 0 {
		_ = wr.WriteError("ERR empty command")
		_ = wr.Flush()
		return false
	}

	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case "SUB", "MOLE.SUB":
		if len(args) < 2 {
			_ = wr.WriteError("ERR wrong number of arguments for 'SUB'")
			_ = wr.Flush()
			return false
		}
		channels := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			channels[i-1] = string(args[i])
		}
		mgr.Subscribe(sub, channels...)
		// Send subscription confirmation for each channel
		for _, ch := range channels {
			reply := resp.Array([]resp.Reply{
				resp.BulkString([]byte("subscribe")),
				resp.BulkString([]byte(ch)),
				resp.Integer(int64(sub.CountTotal())),
			})
			_ = reply.WriteTo(wr)
		}
		_ = wr.Flush()
		return true

	case "PSUB", "MOLE.PSUB":
		if len(args) < 2 {
			_ = wr.WriteError("ERR wrong number of arguments for 'PSUB'")
			_ = wr.Flush()
			return false
		}
		patterns := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			patterns[i-1] = string(args[i])
		}
		mgr.PSubscribe(sub, patterns...)
		// Send subscription confirmation for each pattern
		for _, pat := range patterns {
			reply := resp.Array([]resp.Reply{
				resp.BulkString([]byte("psubscribe")),
				resp.BulkString([]byte(pat)),
				resp.Integer(int64(sub.CountTotal())),
			})
			_ = reply.WriteTo(wr)
		}
		_ = wr.Flush()
		return true

	case "UNSUB", "MOLE.UNSUB":
		channels := make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			channels = append(channels, string(args[i]))
		}
		remaining := mgr.Unsubscribe(sub, channels...)
		if len(channels) == 0 {
			// Unsubscribed from all
			reply := resp.Array([]resp.Reply{
				resp.BulkString([]byte("unsubscribe")),
				resp.NullBulkString{},
				resp.Integer(int64(sub.CountTotal())),
			})
			_ = reply.WriteTo(wr)
		} else {
			for _, ch := range channels {
				reply := resp.Array([]resp.Reply{
					resp.BulkString([]byte("unsubscribe")),
					resp.BulkString([]byte(ch)),
					resp.Integer(int64(sub.CountTotal())),
				})
				_ = reply.WriteTo(wr)
			}
		}
		_ = wr.Flush()
		// If no subscriptions left, exit subscription mode
		if len(remaining) == 0 && len(sub.GetPatterns()) == 0 {
			return false
		}
		return true

	case "PUNSUB", "MOLE.PUNSUB":
		patterns := make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			patterns = append(patterns, string(args[i]))
		}
		remaining := mgr.PUnsubscribe(sub, patterns...)
		if len(patterns) == 0 {
			// Unsubscribed from all
			reply := resp.Array([]resp.Reply{
				resp.BulkString([]byte("punsubscribe")),
				resp.NullBulkString{},
				resp.Integer(int64(sub.CountTotal())),
			})
			_ = reply.WriteTo(wr)
		} else {
			for _, pat := range patterns {
				reply := resp.Array([]resp.Reply{
					resp.BulkString([]byte("punsubscribe")),
					resp.BulkString([]byte(pat)),
					resp.Integer(int64(sub.CountTotal())),
				})
				_ = reply.WriteTo(wr)
			}
		}
		_ = wr.Flush()
		// If no subscriptions left, exit subscription mode
		if len(remaining) == 0 && len(sub.GetChannels()) == 0 {
			return false
		}
		return true

	case "PING":
		// PING is allowed in subscription mode
		if len(args) == 1 {
			reply := resp.Array([]resp.Reply{
				resp.BulkString([]byte("pong")),
				resp.BulkString([]byte("")),
			})
			_ = reply.WriteTo(wr)
		} else {
			reply := resp.Array([]resp.Reply{
				resp.BulkString([]byte("pong")),
				resp.BulkString(args[1]),
			})
			_ = reply.WriteTo(wr)
		}
		_ = wr.Flush()
		return true

	case "QUIT":
		_ = wr.WriteSimpleString("OK")
		_ = wr.Flush()
		return false

	default:
		_ = wr.WriteError("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context")
		_ = wr.Flush()
		return true
	}
}
