# Failover Review

This document reviews how the system behaves when the Raft leader, topic leader, or replicas go away, and how clients and replication recover.

---

## 1. Raft leader failover

### How Raft leader is determined

- **Hashicorp Raft** is used for metadata (topics, nodes, ISR, leader changes). The Raft leader is elected by the Raft protocol; when the current leader dies or is partitioned away, a new leader is elected among the remaining voters.
- **Discovery**: Serf is used for membership. When a node leaves or fails, `EventMemberLeave` / `EventMemberFailed` triggers `handleLeave(member)`, which calls `coordinator.Leave(member.Name)`. The Raft leader (if still up) removes that node from the Raft configuration via `RemoveServer`, and applies a `RemoveNodeEvent` so all nodes drop that node from metadata.

### How clients find the Raft leader

- **FindRaftLeader** RPC returns the current Raft leader’s RPC address.
- **Implementation**: `TopicManager.GetRaftLeaderRPCAddr()` calls `Coordinator.GetRaftLeaderNodeID()`, which **only succeeds when this node is the Raft leader** (`IsLeader()`); otherwise it returns `"not leader"`. So **only the current Raft leader node can successfully answer FindRaftLeader**.

**Implication**: If a client uses a single bootstrap address (`--addr`) and that address was the Raft leader, and the Raft leader node dies, the client gets connection failure. It never gets to ask another node “who is the Raft leader?” because:
- Followers do not implement “return current leader address”; they only return an error when they are not leader.
- So after Raft leader failover, **create-topic (and any metadata op that must go to the Raft leader) requires the client to be pointed at a node that is still the leader, or to try multiple bootstrap addresses** until one is the new leader.

### Server startup and restore

- On startup, **setupTopicManager** waits for Raft to have a leader (`WaitforRaftReadyWithRetryBackoff`). If Raft is not ready within the timeout, it logs a warning and still runs **RestoreFromMetadata** so the node can serve topic/consumer RPCs using in-memory metadata. So a node can come up and serve traffic even if it is not yet part of a Raft quorum; metadata may be stale until it catches up or becomes leader.

---

## 2. Topic leader failover

### How topic leader is stored and updated

- Topic leader is **metadata**: each topic has `LeaderNodeID` (and `LeaderEpoch`). This is replicated via Raft like all other metadata (CreateTopic, DeleteTopic, AddNode, RemoveNode, **LeaderChange**, ISR updates).
- **LeaderChange** events are applied by every node in `Apply(ev)` (MetadataEventTypeLeaderChange): they update `Topics[topic].LeaderNodeID` and call `ensureLocalLogAfterLeaderChange` so the local node opens/closes logs when it becomes leader or replica, or when it is no longer a replica.

### When topic leader is reassigned

- **maybeReassignTopicLeaders(nodeID)** is called only when a node is **removed from the cluster**: from `Apply()` when applying **RemoveNodeEvent** (after Serf reports MemberLeave/MemberFailed and the Raft leader runs `Leave(id)` and applies the remove-node event).
- So topic leader failover is **driven by explicit node removal**, not by a separate health-check or timeout. When a node is considered gone (Serf), the Raft leader removes it; then every node applies RemoveNode; the node that is **current Raft leader** runs `maybeReassignTopicLeaders(removedNodeID)`.
- For each topic whose leader was the removed node, it picks an **ISR replica** as the new leader (first one found in the topic’s `Replicas` that is ISR and present in `Nodes`), then calls **ApplyLeaderChangeEvent(topic, newLeader, epoch+1)**. That is applied via Raft, so all nodes update leader and run `ensureLocalLogAfterLeaderChange`.
- If there is **no ISR replica** for a topic, a warning is logged and that topic has no leader until an operator or future logic adds one.

### Summary

- **Topic leader failover** happens when the topic leader node is removed from membership (Serf leave/fail → Raft remove → RemoveNodeEvent → maybeReassignTopicLeaders). The new leader is chosen from in-sync replicas and committed through Raft. There is no automatic failover based only on “cannot reach the leader” without the node being removed from the cluster.

---

## 3. Client behavior (producer / consumer)

### Finding the topic leader

- **FindTopicLeader** can be answered by **any node**: it uses in-memory metadata (`Topics[topic].LeaderNodeID` → `Nodes[leader].RpcAddr`). All nodes that have applied the same Raft log have the same view, so any healthy node can return the current topic leader address.
- So after a **topic leader** change, as long as the client can reach **some** node (e.g. the same `--addr` if it’s a different node, or another bootstrap), it can call FindTopicLeader again and get the new leader.

### Reconnect on failure

- After Produce or Fetch, if the call fails, the client uses **ShouldReconnect(err)**:
  - RPC errors: **CodeNotTopicLeader**, **CodeTopicNotFound**, **CodeRaftLeaderUnavailable** → reconnect.
  - Connection errors: connection reset, broken pipe, closed connection, refused, timeout, EOF → reconnect.
- On reconnect, the **producer** and **consumer** CLI:
  - Call **FindTopicLeader** again (via the same `remoteClient` / `--addr`) to get the current leader address.
  - Close the old producer/consumer client and create a new one to the new leader address.
  - Retry the operation.
- So for **topic leader** failover, as long as the client can still reach the discovery node (`--addr`) and that node has updated metadata, the client recovers by re-resolving the leader and reconnecting.

### Raft leader vs topic leader

- **Create-topic** (and other metadata ops) must go to the **Raft leader**. The producer CLI gets the Raft leader with **FindRaftLeader** from the same `--addr`. So if `--addr` was the Raft leader and that node dies, FindRaftLeader fails (connection error or, if another node is hit, that node returns “not leader” and the client gets CodeRaftLeaderUnavailable). Recovery requires the user to point `--addr` at another node that is (or will be) the new Raft leader, or the system could be extended so that any node can return the current Raft leader (e.g. by mapping `raft.Leader()` to RpcAddr via metadata).

---

## 4. Replication

### How replication runs

- Each node runs a **replication thread** that periodically calls **replicateAllTopics**: for each topic where this node is a replica (not leader), it groups by leader node and calls **DoReplicateTopicsForLeader**.
- Replication uses the **consumer FetchBatch** API against the topic leader, with **ReplicaNodeID** set so the leader uses ReadUncommitted and records this replica’s LEO.

### When the leader is unreachable

- If **FetchBatch** fails:
  - If the error is **CodeReadOffset** (no data / caught up), the topic is dropped from this round (expected).
  - If **ShouldReconnect(err)** is true (leader change, topic not found, connection failure), **InvalidateConsumerClient(leaderNodeID)** is called so the next replication round creates a fresh consumer client to that node. The topic stays in **stillReplicating** and will be retried next tick.
- So when the **topic leader** dies or is unreachable, replication keeps retrying every tick; when the leader is removed and a new topic leader is elected (see above), the next time replication runs it will use the updated metadata (new leader for that topic) and connect to the new leader. No extra “reassign” is needed for replication; it just follows metadata.

### When this node is the leader and a replica goes away

- Replica LEO is updated from **Fetch** (RecordReplicaLEOFromFetch). If a replica disappears, the leader simply stops receiving fetches from it. ISR is updated via **ApplyIsrUpdateEvent** (replica can be removed from ISR when it’s known to be down, or when it’s removed from the cluster). When the node is removed (Serf → Leave → RemoveNodeEvent), that node is removed from **Nodes** and from topic replica sets in metadata; **maybeReassignTopicLeaders** only runs when the **leader** for a topic goes away, not when a replica goes away. So a dead replica is eventually removed from membership; the topic leader stays the same unless the leader node itself is removed.

---

## 5. Gaps and recommendations

| Area | Current behavior | Recommendation |
|------|------------------|-----------------|
| **FindRaftLeader** | Only the current Raft leader can answer. Followers return “not leader”. | Allow any node to answer FindRaftLeader by returning the current leader’s RPC address (e.g. from `raft.Leader()` and a mapping from Raft address to NodeID/RpcAddr in metadata, or by having the leader write its RpcAddr into the FSM periodically). Then clients can use any bootstrap node to discover the Raft leader after failover. |
| **Topic leader failover trigger** | Topic leader is reassigned only when the node is removed from the cluster (Serf leave/fail → Raft remove). | Acceptable if Serf failure detection is fast enough. If desired, an optional path could reassign when “leader unreachable” for some time without waiting for Serf, at the cost of complexity and risk of split-brain if network is flaky. |
| **No ISR replica** | If the topic leader node dies and there is no ISR replica, the topic has no leader; a warning is logged. | Document or add alerting. Optionally allow out-of-sync replicas as last resort (with possible data loss). |
| **Create-topic after Raft leader failover** | Client that used the old Raft leader as `--addr` cannot discover the new Raft leader. | Use multiple bootstrap addresses and try FindRaftLeader on each until one succeeds, and/or implement “any node can return current Raft leader” as above. |
| **Consumer non-recoverable errors** | Consumer CLI returns on non-reconnect errors (e.g. CodeUnknown) instead of looping. | Current behavior is intentional so that permanent errors are surfaced; no change required unless a “retry forever with backoff” mode is desired. |

---

## 6. Summary

- **Raft**: New leader is elected by Raft when the old one is gone. Metadata is replicated to all nodes. Only the current Raft leader can answer FindRaftLeader today, which limits client recovery after Raft leader failover when using a single bootstrap address.
- **Topic leader**: Reassignment is triggered when the topic leader node is removed from the cluster (Serf → Raft RemoveNode → maybeReassignTopicLeaders). New leader is chosen from ISR and applied via Raft; all nodes apply LeaderChange and update local logs.
- **Clients**: Use ShouldReconnect to detect leader/connection failures, then re-resolve topic leader (FindTopicLeader) and reconnect. Any node can answer FindTopicLeader. Producer/consumer CLI re-resolve and reconnect on ShouldReconnect.
- **Replication**: Uses consumer FetchBatch to the topic leader; on reconnect-worthy errors it invalidates the cached consumer client and retries next tick. After topic leader failover, replication uses the new leader from metadata automatically.
