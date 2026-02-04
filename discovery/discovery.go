package discovery

import (
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/mohitkumar/mlog/config"
	"go.uber.org/zap"
)

const leaveRetryInterval = 10 * time.Second

type Membership struct {
	config.Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
	// pendingJoin stores nodes we need to Join() once we become leader again.
	pendingJoin map[string]struct {
		RaftAddr string
		RpcAddr  string
	}
	pendingLeave map[string]struct{}
	pendingMu    sync.Mutex
	stopCh       chan struct{}
	leaveOnce    sync.Once
}

func New(handler Handler, config config.Config) (*Membership, error) {
	c := &Membership{
		Config:       config,
		handler:      handler,
		logger:       zap.L().Named("discovery"),
		pendingJoin:  make(map[string]struct{ RaftAddr, RpcAddr string }),
		pendingLeave: make(map[string]struct{}),
		pendingMu:    sync.Mutex{},
		stopCh:       make(chan struct{}),
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	go c.retryPendingEvents()
	return c, nil
}

func (m *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events
	rpcAddr, err := m.Config.RPCAddr()
	if err != nil {
		return err
	}
	config.Tags = map[string]string{
		"rpc_addr":  rpcAddr,
		"raft_addr": m.RaftConfig.Address,
	}
	config.NodeName = m.NodeConfig.ID
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}
	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			m.logger.Error("serf join failed", zap.Error(err), zap.Strings("addrs", m.StartJoinAddrs))
		}
	}
	return nil
}

type Handler interface {
	Join(id, raftAddr, rpcAddr string) error
	Leave(name string) error
}

func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *Membership) handleJoin(member serf.Member) {
	raftAddr := member.Tags["raft_addr"]
	rpcAddr := member.Tags["rpc_addr"]
	if raftAddr == "" {
		raftAddr = rpcAddr
	}
	if rpcAddr == "" {
		rpcAddr = raftAddr
	}
	if err := m.handler.Join(member.Name, raftAddr, rpcAddr); err != nil {
		if err == raft.ErrNotLeader {
			// Leader not ready / election in progress; queue join until we become leader.
			m.pendingMu.Lock()
			m.pendingJoin[member.Name] = struct {
				RaftAddr string
				RpcAddr  string
			}{RaftAddr: raftAddr, RpcAddr: rpcAddr}
			m.pendingMu.Unlock()
			m.logger.Debug("join deferred (not leader), will retry", zap.String("name", member.Name), zap.String("raft_addr", raftAddr), zap.String("rpc_addr", rpcAddr))
			return
		}
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		if err == raft.ErrNotLeader {
			// Leader left or election in progress; queue for retry when we become leader.
			m.pendingMu.Lock()
			m.pendingLeave[member.Name] = struct{}{}
			m.pendingMu.Unlock()
			m.logger.Debug("leave deferred (not leader), will retry", zap.String("name", member.Name))
			return
		}
		m.logError(err, "failed to leave", member)
	}
}

// retryPendingEvents runs periodically so the new leader eventually processes queued joins/leaves.
func (m *Membership) retryPendingEvents() {
	ticker := time.NewTicker(leaveRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			// Snapshot pending work under lock, then process without holding it.
			m.pendingMu.Lock()
			joinSnap := make(map[string]struct {
				RaftAddr string
				RpcAddr  string
			}, len(m.pendingJoin))
			for name, info := range m.pendingJoin {
				joinSnap[name] = info
			}
			leaveNames := make([]string, 0, len(m.pendingLeave))
			for name := range m.pendingLeave {
				leaveNames = append(leaveNames, name)
			}
			m.pendingMu.Unlock()

			// Retry joins first so the leader brings new nodes in.
			for name, info := range joinSnap {
				if err := m.handler.Join(name, info.RaftAddr, info.RpcAddr); err != nil {
					if err == raft.ErrNotLeader {
						continue
					}
					m.logger.Error("retry join failed", zap.Error(err), zap.String("name", name))
					continue
				}
				m.pendingMu.Lock()
				delete(m.pendingJoin, name)
				m.pendingMu.Unlock()
				m.logger.Info("join completed on retry", zap.String("name", name), zap.String("raft_addr", info.RaftAddr), zap.String("rpc_addr", info.RpcAddr))
			}

			// Retry leaves.
			for _, name := range leaveNames {
				if err := m.handler.Leave(name); err != nil {
					if err == raft.ErrNotLeader {
						continue
					}
					m.logger.Error("retry leave failed", zap.Error(err), zap.String("name", name))
					continue
				}
				m.pendingMu.Lock()
				delete(m.pendingLeave, name)
				m.pendingMu.Unlock()
				m.logger.Info("leave completed on retry", zap.String("name", name))
			}
		}
	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

func (m *Membership) Leave() error {
	m.leaveOnce.Do(func() { close(m.stopCh) })
	return m.serf.Leave()
}

func (m *Membership) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error
	if err == raft.ErrNotLeader {
		log = m.logger.Debug
	}
	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
