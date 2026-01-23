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
