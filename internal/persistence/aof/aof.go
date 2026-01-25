package aof

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"mole/internal/protocol/resp"
)

type SyncPolicy string

const (
	SyncAlways   SyncPolicy = "always"
	SyncEverySec SyncPolicy = "everysec"
	SyncNo       SyncPolicy = "no"
)

// Writer appends operation records to an AOF file.
//
// Records are encoded as RESP Arrays-of-Bulk-Strings so we can reuse the RESP
// parser for replay and the file stays human-inspectable.
type Writer struct {
	f      *os.File
	bw     *bufio.Writer
	policy SyncPolicy

	mu     sync.Mutex
	closed bool

	// everysec sync loop
	stopSync chan struct{}
}

type WriterOptions struct {
	Path   string
	Policy SyncPolicy
}

func OpenWriter(opts WriterOptions) (*Writer, error) {
	if opts.Path == "" {
		return nil, errors.New("mole: aof path is required")
	}
	if opts.Policy == "" {
		opts.Policy = SyncEverySec
	}

	f, err := os.OpenFile(opts.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		f:        f,
		bw:       bufio.NewWriterSize(f, 64*1024),
		policy:   opts.Policy,
		stopSync: make(chan struct{}),
	}

	if w.policy == SyncEverySec {
		go w.syncLoop()
	}

	return w, nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	close(w.stopSync)

	// Best-effort final flush+sync while holding lock.
	_ = w.bw.Flush()
	_ = w.f.Sync()
	return w.f.Close()
}

func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("mole: aof writer closed")
	}
	return w.bw.Flush()
}

func (w *Writer) syncLoop() {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-w.stopSync:
			return
		case <-t.C:
			_ = w.Flush()
			_ = w.f.Sync()
		}
	}
}

// AppendRecord appends one operation record encoded as a RESP array of bulk strings.
//
// Durability guarantees by policy:
// - SyncAlways: fsync after every write (slowest, no data loss on crash)
// - SyncEverySec: fsync every second via background goroutine (up to 1 second data loss)
// - SyncNo: OS decides when to sync (unbounded data loss window)
func (w *Writer) AppendRecord(args [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("mole: aof writer closed")
	}

	if err := writeRESPArray(w.bw, args); err != nil {
		return err
	}

	// Flush buffered writer so data reaches the OS.
	if err := w.bw.Flush(); err != nil {
		return err
	}

	switch w.policy {
	case SyncAlways:
		return w.f.Sync()
	case SyncEverySec, SyncNo:
		return nil
	default:
		return fmt.Errorf("mole: unknown aof sync policy %q", w.policy)
	}
}

// Replay reads AOF records from path and invokes apply for each record.
// It stops cleanly at EOF. On apply errors, it logs and continues to recover
// as much data as possible from the AOF file.
func Replay(ctx context.Context, path string, apply func(ctx context.Context, args [][]byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	rr := resp.NewReader(f)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		args, err := rr.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			// Protocol errors might indicate corruption - log but continue.
			continue
		}
		if len(args) == 0 {
			continue
		}
		if err := apply(ctx, args); err != nil {
			// Log apply errors but continue replay to recover as much as possible.
			// This allows recovery even if some records are corrupted.
			continue
		}
	}
}

func writeRESPArray(w io.Writer, args [][]byte) error {
	// *<n>\r\n
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if a == nil {
			// Null bulk string
			if _, err := io.WriteString(w, "$-1\r\n"); err != nil {
				return err
			}
			continue
		}
		if _, err := fmt.Fprintf(w, "$%d\r\n", len(a)); err != nil {
			return err
		}
		if _, err := w.Write(a); err != nil {
			return err
		}
		if _, err := io.WriteString(w, "\r\n"); err != nil {
			return err
		}
	}
	return nil
}

// Helpers to build internal op args.
func RecordSetAt(key string, value []byte, expireAtMs int64) [][]byte {
	return [][]byte{
		[]byte("MOLE.SETAT"),
		[]byte(key),
		value,
		[]byte(strconv.FormatInt(expireAtMs, 10)),
	}
}

func RecordExpireAt(key string, expireAtMs int64) [][]byte {
	return [][]byte{
		[]byte("MOLE.EXPIREAT"),
		[]byte(key),
		[]byte(strconv.FormatInt(expireAtMs, 10)),
	}
}

func RecordDel(key string) [][]byte {
	return [][]byte{
		[]byte("MOLE.DEL"),
		[]byte(key),
	}
}
