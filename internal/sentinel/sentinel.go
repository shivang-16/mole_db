package sentinel

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// Sentinel monitors master and replicas, coordinates failover.
type Sentinel struct {
	id           string
	masterAddr   string
	replicaAddrs []string
	peers        []string // other sentinel addresses

	mu            sync.RWMutex
	masterDown    bool
	currentMaster string
	epoch         int64 // incremented on each failover

	heartbeatInterval time.Duration
	downAfter         time.Duration
}

type SentinelConfig struct {
	ID                string
	MasterAddr        string
	ReplicaAddrs      []string
	Peers             []string // other sentinel addresses
	HeartbeatInterval time.Duration
	DownAfter         time.Duration
}

func NewSentinel(cfg SentinelConfig) *Sentinel {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 1 * time.Second
	}
	if cfg.DownAfter <= 0 {
		cfg.DownAfter = 5 * time.Second
	}
	return &Sentinel{
		id:                cfg.ID,
		masterAddr:        cfg.MasterAddr,
		replicaAddrs:      cfg.ReplicaAddrs,
		peers:             cfg.Peers,
		heartbeatInterval: cfg.HeartbeatInterval,
		downAfter:         cfg.DownAfter,
		currentMaster:     cfg.MasterAddr,
	}
}

// Start begins monitoring and failover coordination.
func (s *Sentinel) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.checkMaster(ctx)
		}
	}
}

// checkMaster checks if master is alive.
func (s *Sentinel) checkMaster(ctx context.Context) {
	alive := s.pingMaster(s.currentMaster)

	s.mu.Lock()
	wasDown := s.masterDown
	s.masterDown = !alive
	s.mu.Unlock()

	if !alive && !wasDown {
		// Master just went down - start failover process.
		go s.initiateFailover(ctx)
	}
}

// pingMaster checks if master is reachable.
func (s *Sentinel) pingMaster(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// initiateFailover coordinates failover with other sentinels.
func (s *Sentinel) initiateFailover(ctx context.Context) {
	// Wait for downAfter duration to confirm master is down.
	time.Sleep(s.downAfter)

	// Check again.
	s.mu.RLock()
	masterDown := s.masterDown
	currentMaster := s.currentMaster
	s.mu.RUnlock()

	if !masterDown {
		return // Master recovered
	}

	// Ask peers if they also see master down (quorum check).
	agreed, err := s.quorumVote(ctx, "master-down", currentMaster)
	if err != nil || !agreed {
		return // No quorum
	}

	// Elect best replica to promote.
	bestReplica := s.electBestReplica(ctx)
	if bestReplica == "" {
		return // No suitable replica
	}

	// Promote replica.
	if err := s.promoteReplica(ctx, bestReplica); err != nil {
		return
	}

	// Update our view.
	s.mu.Lock()
	s.currentMaster = bestReplica
	s.epoch++
	s.mu.Unlock()
}

// quorumVote asks peers and requires majority agreement.
func (s *Sentinel) quorumVote(ctx context.Context, proposal, arg string) (bool, error) {
	if len(s.peers) == 0 {
		// No peers - single sentinel, proceed.
		return true, nil
	}

	agreed := 1 // count ourselves
	for _, peer := range s.peers {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		if s.askPeer(peer, proposal, arg) {
			agreed++
		}
	}

	// Need majority: (total sentinels + 1) / 2 + 1
	total := len(s.peers) + 1
	required := total/2 + 1
	return agreed >= required, nil
}

// askPeer asks a peer sentinel for their vote.
func (s *Sentinel) askPeer(peerAddr, proposal, arg string) bool {
	// For now, simple implementation: try to connect and ask.
	// In a full implementation, this would use a proper protocol.
	conn, err := net.DialTimeout("tcp", peerAddr, 1*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	// Send query (simplified - would use proper protocol).
	_, _ = conn.Write([]byte(fmt.Sprintf("QUERY %s %s\n", proposal, arg)))

	// Read response (simplified).
	buf := make([]byte, 10)
	n, _ := conn.Read(buf)
	return n > 0 && buf[0] == 'Y'
}

// electBestReplica picks the best replica to promote.
func (s *Sentinel) electBestReplica(ctx context.Context) string {
	// For now, pick first reachable replica.
	// In a full implementation, we'd check replication lag, etc.
	for _, addr := range s.replicaAddrs {
		if s.pingMaster(addr) {
			return addr
		}
	}
	return ""
}

// promoteReplica tells a replica to become master.
func (s *Sentinel) promoteReplica(ctx context.Context, replicaAddr string) error {
	conn, err := net.DialTimeout("tcp", replicaAddr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send PROMOTE command (simplified - would use proper protocol).
	_, err = conn.Write([]byte(fmt.Sprintf("PROMOTE %d\n", s.epoch)))
	return err
}

// GetCurrentMaster returns the current master address.
func (s *Sentinel) GetCurrentMaster() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentMaster
}
