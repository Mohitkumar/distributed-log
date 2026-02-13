# Failover Review

This document reviews how the system behaves when the Raft leader, topic leader, or replicas go away, and how clients and replication recover. It was updated after implementing multiple-bootstrap support and any-node FindRaftLeader.

---

## 1. Raft leader failover

### How Raft leader is determined

- **Hashicorp Raft** is used for metadata (topics, nodes, ISR, leader changes). The Raft leader is elected by the Raft protocol; when the current leader dies or is partitioned away, a new leader is elected among the remaining voters.
- **Discovery**: Serf is used for membership. When a node leaves or fails, `EventMemberLeave` / `EventMemberFailed` triggers `handleLeave(member)`, which calls `coordinator.Leave(member.Name)`. The Raft leader (if still up) removes that node from the Raft configuration via `RemoveServer`, and applies a `RemoveNodeEvent` so all nodes drop that node from metadata.

### How clients find the Raft leader

- **FindRaftLeader** RPC returns the current Raft leader’s RPC address.
- **Implementation**: `GetRaftLeaderRPCAddr()` uses `Coordinator.GetRaftLeaderNodeID()`, which calls **`raft.LeaderWithID()`** (no longer restricted to the local node being leader). The leader’s node ID is then looked up in `Nodes` to get `RpcAddr`. So **any node that has a known Raft leader can answer FindRaftLeader**; clients can use any bootstrap address and still discover the current Raft leader after a failover.
- If there is no leader yet or the leader is not in metadata, the RPC returns an error (e.g. “raft leader node … not in metadata”).

### Server startup and restore

- On startup, **setupTopicManager** waits for Raft to have a leader (`WaitforRaftReadyWithRetryBackoff`). If Raft is not ready within the timeout, it logs a warning and still runs **RestoreFromMetadata** so the node can serve topic/consumer RPCs using in-memory metadata. So a node can come up and serve traffic even if it is not yet part of a Raft quorum; metadata may be stale until it catches up or becomes leader.

---

## 2. Topic leader failover

### How topic leader is stored and updated

- Topic leader is **metadata**: each topic has `LeaderNodeID` (and `LeaderEpoch`). This is replicated via Raft like all other metadata (CreateTopic, DeleteTopic, AddNode, RemoveNode, **LeaderChange**, ISR updates).
- **LeaderChange** events are applied by every node in `Apply(ev)` (MetadataEventTypeLeaderChange): they update `Topics[topic].LeaderNodeID` and call `ensureLocalLogAfterLeaderChange` so the local node opens/closes logs when it becomes leader or replica, or when it is no longer a replica.

### When topic leader is reassigned

- **maybeReassignTopicLeaders(nodeID)** is called only when a node is **removed from the cluster**: from `Apply()` when applying **RemoveNodeEvent** (after Serf reports MemberLeave/MemberFailed and the Raft leader runs `Leave(id)` and applies the remove-node event).
- For each topic whose leader was the removed node, the **current Raft leader** picks an **ISR replica** as the new leader and calls **ApplyLeaderChangeEvent**; that is applied via Raft, so all nodes update leader and run `ensureLocalLogAfterLeaderChange`.
- If there is **no ISR replica** for a topic, a warning is logged and that topic has no leader until an operator or future logic adds one.

### Summary

- **Topic leader failover** is driven by explicit node removal (Serf → Raft RemoveNode → RemoveNodeEvent → maybeReassignTopicLeaders). There is no automatic failover based only on “cannot reach the leader” without the node being removed from the cluster.

---

## 3. Client behavior (producer / consumer / topic CLI)

### Multiple bootstrap addresses (implemented)

- **Producer** and **consumer** use **`--addrs`** (comma-separated). They use **`client.TryAddrs`** to try each address in order: connect, call the discovery RPC (FindTopicLeader or FindRaftLeader), and on success use the result; on failure, if **ShouldReconnect(err)** then try the next address.
- **Topic** CLI (create, delete, list) also uses **`--addrs`**. Create and delete use **TryAddrs** to find the Raft leader (FindRaftLeader) then send CreateTopic/DeleteTopic to that address. List uses the first reachable address and calls ListTopics (any node can answer).

### Finding the topic leader

- **FindTopicLeader** can be answered by **any node** (in-memory metadata). After a topic leader change, as long as the client can reach any bootstrap address, it can re-resolve the leader and reconnect.

### Reconnect on failure

- After Produce or Fetch fails, the client uses **ShouldReconnect(err)** (RPC codes NotTopicLeader, TopicNotFound, RaftLeaderUnavailable, or connection errors). On reconnect, it calls **FindTopicLeader** again (via **TryAddrs** over `--addrs`), then creates a new producer/consumer client to the returned leader and retries.
- So after **topic leader** or **Raft leader** failover, the client recovers by trying the next address in `--addrs` and/or re-resolving the leader.

### Raft leader discovery (implemented)

- **FindRaftLeader** can be answered by **any node** (using `raft.LeaderWithID()` and `Nodes`). Create-topic and delete-topic use **TryAddrs** with FindRaftLeader, so after Raft leader failover they can still discover the new leader by contacting any other node in `--addrs`.

---

## 4. Replication

### How replication runs

- Each node runs a **replication thread** that periodically calls **replicateAllTopics**: for each topic where this node is a replica, it groups by leader node and calls **DoReplicateTopicsForLeader** (consumer FetchBatch with ReplicaNodeID set).

### When the leader is unreachable

- On **FetchBatch** failure: if **CodeReadOffset**, the topic is dropped for that round (caught up). If **ShouldReconnect(err)**, **InvalidateConsumerClient(leaderNodeID)** is called and the topic is retried next tick. After the topic leader is removed and a new leader is elected, replication uses the updated metadata and connects to the new leader automatically.

### When this node is the leader and a replica goes away

- Replica LEO is updated from Fetch. A dead replica is eventually removed via Serf → Leave → RemoveNodeEvent; **maybeReassignTopicLeaders** runs only when the **topic leader** node goes away, not when a replica goes away.

---

## 5. Gaps and recommendations (updated)

| Area | Status | Notes |
|------|--------|--------|
| **FindRaftLeader from any node** | **Done** | `GetRaftLeaderNodeID()` uses `raft.LeaderWithID()`; any node can return the current leader’s RPC address via `Nodes[leaderNodeID].RpcAddr`. |
| **Multiple bootstrap addresses** | **Done** | Producer, consumer, and topic CLI use `--addrs` and `client.TryAddrs` to try each address for FindRaftLeader / FindTopicLeader until one succeeds. |
| **Create-topic / delete-topic after Raft leader failover** | **Done** | Addressed by multiple addrs and any-node FindRaftLeader. |
| **Topic leader failover trigger** | Open | Topic leader is reassigned only when the node is removed from the cluster (Serf leave/fail → Raft remove). Acceptable if Serf failure detection is fast enough. Optional: reassign when “leader unreachable” for some time, at the cost of complexity and split-brain risk. |
| **No ISR replica** | Open | If the topic leader node dies and there is no ISR replica, the topic has no leader; a warning is logged. Consider documenting or alerting; optionally allow out-of-sync replicas as last resort (with possible data loss). |
| **Consumer non-recoverable errors** | By design | Consumer CLI returns on non-reconnect errors (e.g. CodeUnknown) so permanent errors are surfaced. No change unless a “retry forever with backoff” mode is desired. |

---

## 6. Summary

- **Raft**: A new leader is elected when the old one is gone. **Any node** can answer FindRaftLeader (via `raft.LeaderWithID()` and metadata). Producer, consumer, and topic CLIs use **`--addrs`** and **TryAddrs** so they can discover the Raft leader and topic leader after failover by trying multiple bootstrap addresses.
- **Topic leader**: Reassignment is triggered when the topic leader node is removed (Serf → Raft RemoveNode → maybeReassignTopicLeaders). New leader is chosen from ISR and applied via Raft.
- **Clients**: Use ShouldReconnect, re-resolve leader (FindTopicLeader or FindRaftLeader) via TryAddrs over `--addrs`, and reconnect. FindTopicLeader and FindRaftLeader can be answered by any node.
- **Replication**: Uses FetchBatch to the topic leader; on reconnect-worthy errors it invalidates the cached client and retries next tick; after topic leader failover it uses the new leader from metadata automatically.
