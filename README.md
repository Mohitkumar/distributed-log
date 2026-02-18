# mlog — A Distributed Log System

A production-inspired distributed log implementation in Go, featuring Raft consensus, pull-based replication, Serf cluster discovery, and a comprehensive producer/consumer API. Built for learning the architecture of Apache Kafka and similar event-streaming platforms.

## Features

- **Append-only storage** — Segment-based log with sparse indexes and memory-mapped I/O
- **Wire protocol** — Length-prefixed frames, JSON codec, binary replication batches
- **TCP transport** — Connection pooling, idle timeout, multiplexed RPC
- **Cluster discovery** — Serf gossip protocol for automatic node discovery
- **Raft consensus** — Metadata replication (topics, leaders, ISR) across the cluster
- **Topic management** — Create, delete, and list topics with leader assignment and automatic failover
- **Pull-based replication** — Followers fetch data from the leader; per-leader goroutine pools
- **In-Sync Replicas (ISR)** — Dynamic ISR tracking with configurable lag threshold
- **High watermark** — Consumers only read committed data (replicated to all ISR)
- **Producer API** — Configurable ack modes (none, leader, all) for durability levels
- **Consumer API** — Read by offset with committed offset tracking and auto-resume
- **Fault tolerance** — Automatic leader election and data recovery on node failure

## Project Structure

```
distributed-log/
├── segment/           # Segment files with sparse indexes and mmap
├── log/              # Log manager coordinating multiple segments
├── protocol/         # Wire protocol, codec, message types
├── transport/        # TCP server, client, connection pooling
├── rpc/              # RPC handler dispatch and marshaling
├── discovery/        # Serf membership integration
├── coordinator/      # Raft consensus for metadata
├── topic/            # Topic management and replication thread
├── consumer/         # Server-side offset tracking
├── client/           # Producer, consumer, and admin clients
├── cmd/              # CLI binaries (server, producer, consumer, topic)
├── tests/            # Integration and unit tests
├── infra/            # Docker and deployment configs
├── scripts/          # Cluster startup/shutdown scripts
└── Makefile          # Build and run targets
```

## Getting Started

### Prerequisites

- Go 1.25+
- Docker and Docker Compose (for containerized cluster)
- Make

### Building

Build all binaries:

```bash
make build
```

Individual binaries:

```bash
make build-server      # Broker server
make build-producer    # Producer CLI
make build-consumer    # Consumer CLI
make build-topic       # Topic management CLI
```

### Running a Cluster

#### Option 1: Docker Compose (Recommended)

Start a 3-node cluster:

```bash
make cluster-up
```

View logs:

```bash
make cluster-logs
```

Stop the cluster:

```bash
make cluster-down
```

Restart with fresh data:

```bash
make cluster-restart
```

#### Option 2: Local Cluster

Build the server first:

```bash
make build-server
```

Start the local cluster:

```bash
./scripts/start-local-cluster.sh
```

Stop the local cluster:

```bash
./scripts/stop-local-cluster.sh
```

### Quick Start: Create, Produce, and Consume

1. **Create a topic** with 3 replicas:

```bash
make create-topic topic=orders replicas=3
```

2. **Open a producer** (reads from stdin):

```bash
make producer-connect topic=orders
```

Type messages (one per line):

```
hello world
this is a test
^D  (press Ctrl-D to exit)
```

3. **Open a consumer** (in another terminal):

```bash
make consumer-connect topic=orders
```

4. **Verify replication** by consuming from different nodes:

```bash
./bin/consumer connect --addrs 127.0.0.1:9097 --topic orders --id test --from-beginning
```

## Configuration

### Server Flags

```
--bind-addr              Serf listen address (default 127.0.0.1:9092)
--advertise-addr         Address other nodes see (e.g., node1 in Docker)
--rpc-port               RPC listen port (default 9094)
--data-dir               Where to store log data (default /tmp/mlog)
--node-id                Unique node identifier (default node-1)
--raft-addr              Raft transport address (default 127.0.0.1:9093)
--bootstrap              Bootstrap a new Raft cluster (only on ONE node)
--peer                   Peer nodes for Serf join (repeatable)
--replication-batch-size Max records per replication fetch (default 5000)
```

All flags can also be set via environment variables with the `MLOG_` prefix (e.g., `MLOG_NODE_ID=node-1`).

### Environment Variables

```bash
export MLOG_NODE_ID=node-2
export MLOG_RPC_PORT=9094
export MLOG_DATA_DIR=/var/lib/mlog
```

## API Usage

### Producer API

```go
import "github.com/mohitkumar/mlog/client"

// Connect to a broker
pc, err := client.NewProducerClient("127.0.0.1:9094")
defer pc.Close()

// Produce a single message
resp, err := pc.Produce(ctx, &protocol.ProduceRequest{
    Topic: "orders",
    Value: []byte("order-123"),
    Acks:  protocol.AckAll,  // 0=none, 1=leader, 2=all
})
fmt.Printf("offset=%d\n", resp.Offset)

// Produce a batch
resp, err := pc.ProduceBatch(ctx, &protocol.ProduceBatchRequest{
    Topic:  "orders",
    Values: [][]byte{[]byte("val1"), []byte("val2")},
    Acks:   protocol.AckLeader,
})
fmt.Printf("base=%d last=%d\n", resp.BaseOffset, resp.LastOffset)
```

### Consumer API

```go
// Connect to a broker
cc, err := client.NewConsumerClient("127.0.0.1:9094")
defer cc.Close()

// Fetch a single record
resp, err := cc.Fetch(ctx, &protocol.FetchRequest{
    Topic:  "orders",
    Offset: 0,
    Id:     "my-consumer",
})
if resp.Entry != nil {
    fmt.Printf("offset=%d value=%s\n", resp.Entry.Offset, string(resp.Entry.Value))
}

// Commit offset for resuming later
cc.CommitOffset(ctx, &protocol.CommitOffsetRequest{
    Topic:  "orders",
    Offset: 1,
    Id:     "my-consumer",
})

// Fetch last committed offset
resp, err := cc.FetchOffset(ctx, &protocol.FetchOffsetRequest{
    Topic: "orders",
    Id:    "my-consumer",
})
fmt.Printf("last committed offset: %d\n", resp.Offset)
```

### Admin API

```go
// Create a topic
rc, err := client.NewRemoteClient("127.0.0.1:9094")
defer rc.Close()

resp, err := rc.CreateTopic(ctx, &protocol.CreateTopicRequest{
    Topic:        "orders",
    ReplicaCount: 3,
})

// List topics
resp, err := rc.ListTopics(ctx, &protocol.ListTopicsRequest{})
for _, topic := range resp.Topics {
    fmt.Printf("topic=%s leader=%s\n", topic.Name, topic.LeaderNodeID)
}

// Delete a topic
resp, err := rc.DeleteTopic(ctx, &protocol.DeleteTopicRequest{
    Topic: "orders",
})
```

## Testing

Run all tests:

```bash
make test
```

Run specific test file:

```bash
go test -v ./segment -run TestAppend
go test -v ./topic -run TestCreateTopic
```

Run with coverage:

```bash
go test -v ./... -cover
```

## Makefile Targets

```makefile
# Build
build              # Build all binaries
build-server       # Build server binary
build-producer     # Build producer CLI
build-consumer     # Build consumer CLI
build-topic        # Build topic CLI

# Docker cluster
cluster-up         # Start 3-node Docker cluster
cluster-down       # Stop Docker cluster
cluster-restart    # Restart with fresh data
cluster-logs       # Tail Docker logs

# Local cluster
local-cluster-start  # Start local 3-node cluster
local-cluster-stop   # Stop local cluster

# Operations
create-topic       # Create topic (topic=X replicas=N)
delete-topic       # Delete topic (topic=X)
list-topics        # List all topics
producer-connect   # Interactive producer (topic=X)
consumer-connect   # Streaming consumer (topic=X)

# Testing
test               # Run all tests
test-coverage      # Run tests with coverage
```

## Architecture Overview

### Storage Layer

- **Segment**: Fixed-size append-only files with a dense log and sparse index
- **Index**: Every Nth record has an index entry (offset, file position)
- **mmap**: Memory-mapped I/O for fast reading without syscalls
- **LogManager**: Coordinates multiple segments, handles log rotation

### Protocol Layer

- **Frame**: Length-prefixed protocol (4-byte big-endian length + payload)
- **Codec**: JSON for RPC, binary for replication batches
- **Message Types**: Produce, Fetch, CreateTopic, LeaderChange, IsrUpdate, etc.

### Transport Layer

- **TCP Server**: Listens on RPC port, dispatches requests to handlers
- **Connection Pool**: Reuses connections, idle timeout for cleanup
- **RPC Handler**: Type-switches on message type, calls topic manager

### Cluster Layer

- **Serf**: Gossip-based discovery; detects joins and failures
- **Raft**: Replicates metadata (topics, leaders, ISR) via consensus
- **Coordinator**: FSM that applies Raft events to topic metadata

### Replication Layer

- **Replication Thread**: Wakes every 1s, launches per-leader goroutines
- **Pull-Based**: Followers fetch from leader using `FetchBatch`
- **ISR Tracking**: Leader monitors replica LEO; demotes lagging replicas
- **High Watermark**: Advanced when all ISR replicas catch up

### API Layer

- **Producer Client**: Sends produce requests with configurable ack modes
- **Consumer Client**: Fetches by offset, tracks committed offsets
- **Topic Client**: Admin operations for topic lifecycle
- **Auto-Reconnect**: Clients rediscover leader on failure

## Ack Modes

| Mode | Latency | Durability | Use Case |
|------|---------|-----------|----------|
| **AckNone (0)** | Fastest | None | Metrics, logs, fire-and-forget |
| **AckLeader (1)** | Medium | Single node | General messaging (default) |
| **AckAll (2)** | Slowest | Replicated | Financial, critical data |

## Fault Tolerance

The system handles:

- **Leader crash** — Serf detects failure, Raft elects new metadata leader, topic leaders are reassigned from ISR
- **Follower crash** — Replication stalls for that replica; ISR tracking demotes it; HW may not advance if it's the only replica
- **Network partition** — Leader election resumes when partition heals; split-brain prevented by quorum

## Performance Tuning

### Key Constants

```go
DefaultReplicationBatchSize = 5000      // Records per fetch
DefaultISRLagThreshold      = 100       // Max lag to stay in ISR
replicationTickInterval     = 1 * time.Second
AckAll timeout              = 5 * time.Second
AckAll poll interval        = 10 * time.Millisecond
```

### Optimization Tips

- **Larger replication batch** for high-throughput topics: `--replication-batch-size 10000`
- **Smaller ISR threshold** for low-latency detection: modify `DefaultISRLagThreshold` in code
- **Memory-mapped logs** speed up segment reads (already enabled)
- **Consumer batch fetch** faster than single record fetch

## Debugging

### Enable detailed logging

```bash
export MLOG_LOG_LEVEL=debug
./bin/server --node-id node-1
```

### Inspect segment files

```bash
ls -lh /tmp/mlog/node1/orders/
```

Each segment has:
- `.log` — Append-only records
- `.idx` — Sparse index (offset, file position pairs)

### Check broker status

```bash
./bin/topic list --addrs 127.0.0.1:9094
```

### Monitor replication

```bash
# Consumer from different nodes
./bin/consumer connect --addrs 127.0.0.1:9094 --topic orders --id test
./bin/consumer connect --addrs 127.0.0.1:9097 --topic orders --id test  # node2
```
## References

- [Apache Kafka](https://kafka.apache.org/) — Inspiration for design
- [Raft Consensus](https://raft.github.io/) — Consensus protocol
- [Serf](https://www.serf.io/) — Cluster membership
- [Memory-mapped I/O](https://en.wikipedia.org/wiki/Memory-mapped_file) — Fast file access

---

Built for learning distributed systems. Inspired by Kafka, implemented from scratch.
