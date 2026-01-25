package replication

import (
	"context"
	"net"
	"sync"

	"mole/internal/core"
	"mole/internal/persistence/aof"
	"mole/internal/protocol/resp"
)

// Master manages replication to connected replicas.
type Master struct {
	mu       sync.RWMutex
	replicas map[net.Conn]*replicaConn
	store    core.Store
}

type replicaConn struct {
	conn   net.Conn
	wr     *resp.Writer
	cancel context.CancelFunc
}

func NewMaster(store core.Store) *Master {
	return &Master{
		replicas: make(map[net.Conn]*replicaConn),
		store:    store,
	}
}

// AddReplica registers a new replica connection.
func (m *Master) AddReplica(conn net.Conn) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create cancellable context for this replica connection.
	ctx, cancel := context.WithCancel(context.Background())

	rc := &replicaConn{
		conn:   conn,
		wr:     resp.NewWriter(conn),
		cancel: cancel,
	}
	m.replicas[conn] = rc

	// Send full snapshot to new replica.
	go func() {
		defer m.RemoveReplica(conn)
		if err := m.sendSnapshot(ctx, rc); err != nil {
			return
		}
		// After snapshot, stream live updates.
		m.streamToReplica(ctx, rc)
	}()

	return nil
}

// RemoveReplica removes a replica connection.
func (m *Master) RemoveReplica(conn net.Conn) {
	m.mu.Lock()
	rc, ok := m.replicas[conn]
	if ok {
		delete(m.replicas, conn)
	}
	m.mu.Unlock()

	if ok && rc.cancel != nil {
		// Cancel context to stop goroutines before closing connection.
		rc.cancel()
	}
	_ = conn.Close()
}

// BroadcastOp sends an operation to all connected replicas.
func (m *Master) BroadcastOp(ctx context.Context, op [][]byte) {
	m.mu.RLock()
	replicas := make([]*replicaConn, 0, len(m.replicas))
	for _, rc := range m.replicas {
		replicas = append(replicas, rc)
	}
	m.mu.RUnlock()

	// Collect failed replicas to remove after iteration to avoid lock contention.
	var failedReplicas []net.Conn
	for _, rc := range replicas {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Write RESP array.
		if err := writeOp(rc.wr, op); err != nil {
			failedReplicas = append(failedReplicas, rc.conn)
			continue
		}
		if err := rc.wr.Flush(); err != nil {
			failedReplicas = append(failedReplicas, rc.conn)
			continue
		}
	}

	// Remove failed replicas after iteration completes.
	for _, conn := range failedReplicas {
		m.RemoveReplica(conn)
	}
}

// sendSnapshot sends the full state of the store to a replica.
func (m *Master) sendSnapshot(ctx context.Context, rc *replicaConn) error {
	// Collect snapshot data first (without network I/O) to avoid blocking locks.
	type snapshotEntry struct {
		key        string
		value      []byte
		expireAtMs int64
	}
	var snapshot []snapshotEntry

	// Try to get snapshot iterator from MemoryStore.
	if memStore, ok := m.store.(*core.MemoryStore); ok {
		err := memStore.IterateAllKeys(ctx, func(key string, value []byte, expireAtMs int64) error {
			// Defensive copy of value.
			valCopy := make([]byte, len(value))
			copy(valCopy, value)
			snapshot = append(snapshot, snapshotEntry{
				key:        key,
				value:      valCopy,
				expireAtMs: expireAtMs,
			})
			return nil
		})
		if err != nil {
			return err
		}
	}

	// Now perform network I/O without holding any locks.
	for _, entry := range snapshot {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		op := aof.RecordSetAt(entry.key, entry.value, entry.expireAtMs)
		if err := writeOp(rc.wr, op); err != nil {
			return err
		}
		if err := rc.wr.Flush(); err != nil {
			return err
		}
	}

	// Send "SYNC_DONE" marker.
	_ = rc.wr.WriteSimpleString("SYNC_DONE")
	_ = rc.wr.Flush()
	return nil
}

// streamToReplica keeps the connection alive and handles any errors.
func (m *Master) streamToReplica(ctx context.Context, rc *replicaConn) {
	// Connection is kept alive; ops are sent via BroadcastOp.
	// If connection closes, RemoveReplica will clean it up.
	<-ctx.Done()
}

// writeOp writes a MOLE.* operation as a RESP array.
func writeOp(wr *resp.Writer, op [][]byte) error {
	return wr.WriteArray(op)
}
