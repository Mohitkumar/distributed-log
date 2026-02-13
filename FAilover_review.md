## RPC and Topic Failover Review

Review of `client/*.go`, `rpc/*.go`, and `topic/*.go` from a failover perspective: topic leader shutdown, replica shutdown/comeback, client-side leader discovery, and how these pieces behave under partial failures.

---

## 1. Client RPC layer (`client/*.go`)

- **RemoteClient / ProducerClient / ConsumerClient**
  - All clients are very thin wrappers over `transport.TransportClient` and assume a **single fixed RPC address** for the lifetime of the client.
  - `NewRemoteClient`, `NewProducerClient`, and `NewConsumerClient` have a **DNS fallback**: if dialing `nodeX:port` fails with `no such host`, they transparently retry on `127.0.0.1:port`. This is helpful for Docker vs. host networking, but it is not a failover mechanism across nodes.
  - After construction, there is **no retry or re-resolve** of the leader address if the connection dies (e.g. leader restart, pod reschedule, network flap); all methods (`Call`, `Write`, `ReadResponse`) assume the underlying TCP connection is healthy.

- **Leader discovery**
  - `RemoteClient` exposes `FindLeader` and `GetRaftLeader`, which lets higher layers discover the current topic leader and Raft leader.
  - However, the usage pattern (outside this package) must take responsibility for **re-resolving and rebuilding clients** when RPC calls fail; there is no automatic failover built into the clients themselves.

**Implications for failover**

- If a producer/consumer holds a `ProducerClient`/`ConsumerClient` to a leader that dies or moves, all subsequent calls will fail until the caller:
  1. Calls `FindLeader`/`GetRaftLeader` again.
  2. Constructs a new client to the returned address.
- The code does not automatically **invalidate** or recreate clients when IO errors are seen, so the caller must implement this loop.

**Recommendations**

- Wrap client usage in a **retry-with-rediscovery** loop:
  - On transport-level errors (broken pipe, connection reset, timeout), close the current client, call `FindLeader`/`GetRaftLeader`, and create a new client to the returned address.
  - Ideally, centralize this in a higher-level producer/consumer library so application code gets automatic failover.
- Consider **short-lived clients** for one-shot calls (create topic, commit offset) rather than long-lived connections, or add connection health checks / idle reconnect logic in `transport.TransportClient`.

---

## 2. RPC server (`rpc/*.go`)

- **Leader routing for produce**
  - `Produce` / `ProduceBatch` explicitly check `IsLeader(topic)` and return `ErrNotTopicLeader` when called on a follower. This is good: followers will not accept writes.
  - Failover relies on **clients handling `ErrNotTopicLeader`** by calling `FindLeader` and retrying on the correct node.

- **Reads / consumer path**
  - `Fetch` and `FetchStream` read directly from `leaderNode.Log` obtained via `GetLeader(req.Topic)`.
  - `GetLeader` enforces that this node is the leader (`ErrThisNodeNotLeaderf`), so the RPC server on a follower will not serve reads for that topic.
  - Again, this is correct, but it **assumes clients handle `ErrTopicRequired`/`ErrTopicNotFound`/`ErrThisNodeNotLeader` by re-discovering the leader** when they hit the wrong node after a leader move.

- **ISR and replication coordination**
  - `handleReplicate` streams batches from the leader log; it returns `EndOfStream` when `LEO <= offset` and includes `LeaderLEO` so replicas can decide ISR status via `ReportLEOViaRaft`.
  - If the leader dies, replicas will see connection errors when calling `ReplicatePipeline` (see next section); there is no special RPC-level logic for that beyond normal IO errors.

**Implications for failover**

- RPC server behavior is consistent: non-leaders will reject produce/fetch for a topic. That is correct but forces **clients to be failover-aware**.
- There is **no transparent forwarding** to the current leader (e.g. follower forwarding produce to leader), which keeps the design simple but pushes the complexity to clients.

**Recommendations**

- Make sure client libraries:
  - Treat `ErrNotTopicLeader` / leader-related errors as triggers to **call `FindLeader` and retry**.
  - Optionally implement **bounded retries with backoff** to avoid hammering the cluster during failover.
- As an optional enhancement, consider **server-side forwarding** for produce/fetch when a follower knows the leader’s RPC address (trade-off: more complexity and coupling).

---

## 3. Topic manager and replication (`topic/*.go`)

### 3.1 Replication thread (`replication.go`)

- `StartReplicationThread` launches a ticker loop that calls `replicateAllTopics()` every second.
- `replicateAllTopics()`:
  - Builds a map of `leaderID -> []topicNames` using `ListReplicaTopics()`, which returns topics where this node is a **replica** (non-leader) and has a local log.
  - For each leader, it calls `DoReplicateTopicsForLeader`, which:
    - Obtains a **cached replication client** via `GetReplicationClient(leaderNodeID)`.
    - Issues `ReplicatePipeline` with current LEO per topic and applies chunks via `ApplyChunk`.
    - When `EndOfStream` is true, calls `ReportLEOViaRaft` so the Raft leader can update ISR and replica LEO metadata.

**Gaps**

- **Stale replication clients**
  - `NodeMetadata.GetReplicationClient()` lazily constructs and caches a `RemoteClient` for the node’s `RpcAddr`; it never invalidates this cache.
  - If the leader restarts, changes address, or its TCP connection is closed, the cached client may be permanently broken (e.g. broken pipe). `ReplicatePipeline` errors are returned but do not clear the cached client, so each tick can keep reusing the same dead connection.

**Recommendations**

- On replication errors that indicate connection failure, **invalidate the replication client**:
  - Extend `NodeMetadata` with a method to close and nil `replicationClient`.
  - In `DoReplicateTopicsForLeader`, if `ReplicatePipeline` returns a transport-level error, call this invalidation and return; the next tick will build a fresh connection.
- Optionally add **basic backoff** when repeatedly failing for the same leader (e.g. skip one or two ticks after a failure) to reduce log noise and connection churn.

### 3.2 ISR, LEO reporting, and leader change

- `ReportLEOViaRaft` sends `ApplyIsrUpdateEvent` to the Raft leader. It:
  - Computes `isr` based on how far behind `replicaLEO` is from `leaderLEO` (within 100 records).
  - Uses `GetRaftLeaderNodeID` to locate the Raft leader and `GetRPCClient` to dial it.
  - On success, the Raft leader applies `IsrUpdateEvent` to metadata, and `TopicManager.Apply` updates `ReplicaState` and recomputes high watermark (`maybeAdvanceHW`).

**Gaps**

- If `GetRaftLeaderNodeID` or `GetRPCClient` fails (e.g. Raft leader just moved, or cached RPC client is dead), `ReportLEOViaRaft` logs a debug message and returns an error to the replication loop.
  - Similar to `GetReplicationClient`, **cached RPC clients are not invalidated** on failure.
  - There is no retry/backoff beyond the replication tick itself.

**Recommendations**

- Mirror the replication pattern for Raft-leader RPC:
  - In `GetRPCClient` / `NodeMetadata.GetRpcClient`, add a way to **invalidate the cached client** when `ApplyIsrUpdateEvent` returns transport-level errors.
  - Consider a small retry loop that, on `ErrCannotReachLeader`-type errors, re-calls `GetRaftLeaderNodeID` (in case leadership has changed) before giving up.

### 3.3 Topic role and leader changes

- `TopicManager` tracks:
  - `Topics[topic].LeaderNodeID` and `Replicas` with ISR/LEO.
  - Local role via `CurrentNodeID` and the presence of a local log.
- On create topic:
  - Raft metadata applies `MetadataEventTypeCreateTopic`, and `TopicManager.Apply` calls `createTopicFromEvent` and `ensureLocalLogForTopic`, creating a leader log or replica log depending on `CurrentNodeID`.
- On leader change:
  - When the Raft coordinator applies `LeaderChangeEvent`, `TopicManager.Apply` handles `MetadataEventTypeLeaderChange` by:
    - Updating `LeaderNodeID` and `LeaderEpoch`.
    - Calling `ensureLocalLogAfterLeaderChange`, which:
      - Promotes this node to leader if `CurrentNodeID == newLeaderID`, opening or reusing the log.
      - Adjusts state for demotions and replicas, including deleting and recreating local logs as needed.

**Strengths**

- Leader promotion/demotion logic is centralized in `ensureLocalLogAfterLeaderChange`, which is invoked when metadata applies leader-change events.
- ISR updates feed into `maybeAdvanceHW`, so committed (replicated) offsets track ISR replicas and high watermark moves forward conservatively.

**Remaining sensitivities**

- Correctness depends on **metadata events being applied everywhere** and `TopicManager.Apply` being invoked reliably. If a node misses events during a long partition and restarts without replaying metadata, its view of leader/replica roles will be stale until full restore.
- Failover time for reads/writes is bounded by:
  - How quickly `maybeReassignTopicLeaders` (in the coordinator) chooses a new leader and applies a `LeaderChangeEvent`.
  - How quickly replication catches up ISR replicas and marks them `IsISR` so they are eligible for promotion.

---

## 4. End-to-end failover story

Putting the above together:

- **Topic leader dies**
  - Coordinator chooses an ISR replica as new leader and applies a leader-change event.
  - `TopicManager.Apply` on all nodes updates leader metadata and calls `ensureLocalLogAfterLeaderChange`, promoting the new leader and adjusting replicas.
  - RPC servers on non-leader nodes reject produce/fetch; clients must handle `ErrNotTopicLeader` by rediscovering via `FindLeader`.
  - Replication to remaining replicas continues using `DoReplicateTopicsForLeader`; they may need to refresh replication clients after the leader moves.

- **Replica dies and comes back**
  - While down, replication to that replica simply fails (IO errors); `ReportLEOViaRaft` stops sending updates for that replica, and ISR may mark it out.
  - When the replica restarts (or heals), `RestoreFromMetadata` recreates local replica logs, and the replication thread resumes pulling from the leader from its current LEO.
  - If cached replication/RPC clients are not invalidated, reconnection may be flaky until processes restart.

---

## 5. Concrete improvement checklist

- **Client failover**
  - Add a higher-level producer/consumer wrapper that:
    - On leader-related errors or transport failures, calls `FindLeader`/`GetRaftLeader`, rebuilds clients, and retries with backoff.
    - Optionally distinguishes between retryable (transient) and non-retryable errors.

- **Connection robustness**
  - In `NodeMetadata`:
    - Add methods to invalidate/close cached `remoteClient` and `replicationClient` on connection errors.
  - In replication and ISR reporting:
    - When `ReplicatePipeline` or `ApplyIsrUpdateEvent` return transport errors, invalidate the corresponding client so the next tick creates a fresh connection.

- **Observability**
  - Add structured logs around:
    - Leader changes (old leader, new leader, topics affected).
    - Replication failures per leader/topic (with clear error type).
    - Client-side retries and leader rediscovery events.

With these changes, the current design (explicit leader routing, ISR-based replication, and metadata-driven leader changes) will behave much more robustly under topic leader failures, replica failures, and network partitions, while keeping the architecture relatively simple.

