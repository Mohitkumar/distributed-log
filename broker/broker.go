package broker

import (
	"sync"

	"github.com/mohitkumar/mlog/transport"
)

type Broker struct {
	NodeID string
	Addr   string
	mu     sync.Mutex
	conn   *transport.Conn
	tr     *transport.Transport
}

func NewBroker(nodeID string, addr string) *Broker {
	return &Broker{
		NodeID: nodeID,
		Addr:   addr,
		tr:     transport.NewTransport(),
	}
}

// GetConn returns the TCP transport connection, creating it if necessary.
// Replication (and producer/consumer) use this connection for frame-based RPC.
// This method is thread-safe and will lazily establish the connection on first call.
func (b *Broker) GetConn() (*transport.Conn, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.conn != nil {
		return b.conn, nil
	}

	conn, err := b.tr.Connect(b.Addr)
	if err != nil {
		return nil, err
	}
	b.conn = conn
	return b.conn, nil
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.conn != nil {
		err := b.conn.Close()
		b.conn = nil
		return err
	}
	return nil
}

func (b *Broker) GetAddr() string {
	return b.Addr
}

type BrokerManager struct {
	brokers map[string]*Broker
}

func NewBrokerManager() *BrokerManager {
	return &BrokerManager{
		brokers: make(map[string]*Broker),
	}
}

func (bm *BrokerManager) AddBroker(broker *Broker) {
	bm.brokers[broker.NodeID] = broker
}

func (bm *BrokerManager) GetBroker(nodeID string) *Broker {
	return bm.brokers[nodeID]
}

func (bm *BrokerManager) RemoveBroker(nodeID string) {
	delete(bm.brokers, nodeID)
}

func (bm *BrokerManager) GetAllBrokers() []*Broker {
	brokers := make([]*Broker, 0, len(bm.brokers))
	for _, broker := range bm.brokers {
		brokers = append(brokers, broker)
	}
	return brokers
}

// GetBrokerByAddr finds a broker by its address
func (bm *BrokerManager) GetBrokerByAddr(addr string) *Broker {
	for _, b := range bm.brokers {
		if b.Addr == addr {
			return b
		}
	}
	return nil
}
