package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"mole/internal/commands"
)

type Config struct {
	Addr        string
	IdleTimeout time.Duration
}

type Server struct {
	cfg     Config
	handler *commands.Handler
}

func New(cfg Config, handler *commands.Handler) *Server {
	return &Server{cfg: cfg, handler: handler}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	if s.cfg.Addr == "" {
		s.cfg.Addr = "127.0.0.1:7379"
	}
	if s.cfg.IdleTimeout <= 0 {
		s.cfg.IdleTimeout = 5 * time.Minute
	}
	if s.handler == nil {
		return errors.New("mole: server handler is required")
	}

	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	fmt.Printf("mole listening on %s\n", s.cfg.Addr)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Temporary() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}

		go s.serveConn(ctx, conn)
	}
}
