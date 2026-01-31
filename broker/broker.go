package broker

import (
	"sync"

	"github.com/mohitkumar/mlog/transport"
)

type Broker struct {
	NodeID string
	Addr   string
	mu     sync.Mutex
	client *transport.TransportClient
}

func NewBroker(nodeID string, addr string) *Broker {
	return &Broker{
		NodeID: nodeID,
		Addr:   addr,
	}
}

// GetClient returns the transport client for this broker, dialing once and caching the connection.
func (b *Broker) GetClient() (*transport.TransportClient, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.client == nil {
		client, err := transport.Dial(b.Addr)
		if err != nil {
			return nil, err
		}
		b.client = client
	}
	return b.client, nil
}

func (b *Broker) GetAddr() string {
	return b.Addr
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.client != nil {
		err := b.client.Close()
		b.client = nil
		return err
	}
	return nil
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
