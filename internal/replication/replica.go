package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"mole/internal/core"
	"mole/internal/persistence/aof"
	"mole/internal/protocol/resp"
)

// Replica manages connection to master and applies replication stream.
type Replica struct {
	masterAddr string
	store      core.Store
}

func NewReplica(masterAddr string, store core.Store) *Replica {
	return &Replica{
		masterAddr: masterAddr,
		store:      store,
	}
}

// Start connects to master, performs full sync, then streams live updates.
func (r *Replica) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := net.DialTimeout("tcp", r.masterAddr, 5*time.Second)
		if err != nil {
			// Retry after delay.
			time.Sleep(1 * time.Second)
			continue
		}

		// Send handshake: REPLICAOF command.
		wr := resp.NewWriter(conn)
		handshake := [][]byte{[]byte("REPLICAOF")}
		if err := wr.WriteArray(handshake); err != nil {
			_ = conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		if err := wr.Flush(); err != nil {
			_ = conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		// Perform full sync, then stream.
		if err := r.syncFromMaster(ctx, conn); err != nil {
			_ = conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		// Stream live updates.
		if err := r.streamFromMaster(ctx, conn); err != nil {
			_ = conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}
	}
}

// syncFromMaster performs full synchronization (snapshot).
func (r *Replica) syncFromMaster(ctx context.Context, conn net.Conn) error {
	rr := resp.NewReader(conn)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		args, err := rr.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("mole: master closed connection during sync")
			}
			return err
		}

		if len(args) == 0 {
			continue
		}

		// Check for SYNC_DONE marker.
		if len(args) == 1 && strings.ToUpper(string(args[0])) == "SYNC_DONE" {
			return nil
		}

		// Apply operation to local store.
		if err := aof.ApplyRecordToStore(ctx, r.store, args); err != nil {
			return fmt.Errorf("mole: failed to apply sync op: %w", err)
		}
	}
}

// streamFromMaster streams live updates from master.
func (r *Replica) streamFromMaster(ctx context.Context, conn net.Conn) error {
	rr := resp.NewReader(conn)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		args, err := rr.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("mole: master closed connection")
			}
			return err
		}

		if len(args) == 0 {
			continue
		}

		// Apply operation to local store.
		// Note: args are already defensive copies from resp.Reader (fixed in reader.go).
		if err := aof.ApplyRecordToStore(ctx, r.store, args); err != nil {
			// Log error but continue streaming to maintain connection.
			// This provides visibility into replication failures.
			log.Printf("replication: failed to apply operation from master: %v", err)
			continue
		}
	}
}
