package server

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"mole/internal/protocol/resp"
)

func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(s.cfg.IdleTimeout))

	rr := resp.NewReader(conn)
	wr := resp.NewWriter(conn)

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
			_ = s.master.AddReplica(conn)
			return
		}
		_ = wr.WriteError("ERR not a master")
		_ = wr.Flush()
		return
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
