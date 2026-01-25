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
	conn net.Conn
	wr   *resp.Writer
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

	rc := &replicaConn{
		conn: conn,
		wr:   resp.NewWriter(conn),
	}
	m.replicas[conn] = rc

	// Send full snapshot to new replica.
	go func() {
		defer m.RemoveReplica(conn)
		if err := m.sendSnapshot(context.Background(), rc); err != nil {
			return
		}
		// After snapshot, stream live updates.
		m.streamToReplica(context.Background(), rc)
	}()

	return nil
}

// RemoveReplica removes a replica connection.
func (m *Master) RemoveReplica(conn net.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.replicas, conn)
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

	for _, rc := range replicas {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Write RESP array.
		if err := writeOp(rc.wr, op); err != nil {
			m.RemoveReplica(rc.conn)
			continue
		}
		if err := rc.wr.Flush(); err != nil {
			m.RemoveReplica(rc.conn)
			continue
		}
	}
}

// sendSnapshot sends the full state of the store to a replica.
func (m *Master) sendSnapshot(ctx context.Context, rc *replicaConn) error {
	// Try to get snapshot iterator from MemoryStore.
	if memStore, ok := m.store.(*core.MemoryStore); ok {
		err := memStore.IterateAllKeys(ctx, func(key string, value []byte, expireAtMs int64) error {
			op := aof.RecordSetAt(key, value, expireAtMs)
			if err := writeOp(rc.wr, op); err != nil {
				return err
			}
			return rc.wr.Flush()
		})
		if err != nil {
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
