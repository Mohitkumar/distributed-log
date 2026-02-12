# Coordinator Failover Review

Review of `coordinator/metadata.go`, `coordinator/coordinator.go`, and `coordinator/replication_thread.go` from a failover perspective: Raft leader shutdown, topic leader shutdown, topic replica shutdown/comeback, and related scenarios.

---

## 1. Raft leader shutdown

### Current behavior

- When a node **gracefully** shuts down, it calls `membership.Leave()`, so Serf emits a leave event. **Only the Raft leader** can successfully run `Coordinator.Leave(id)`: it removes the node from Raft, applies `RemoveNode`, and calls `maybeReassignTopicLeaders(id)`.
- When the **Raft leader itself** shuts down (gracefully or not), the node that is leaving does not run Leave for itself. Remaining nodes receive Serf `MemberLeave` / `MemberFailed` and each calls `handler.Leave(deadMemberName)`. The handler runs on **whoever receives the event** (e.g. node-2 or node-3), and at that moment that node is **not** yet the Raft leader (election may not have completed). So `Leave()` does `if !c.IsLeader() { return nil }` and **no removal or topic reassignment happens**.

### Gaps

1. **Dead Raft leader is never removed from Raft configuration**  
   Raft will elect a new leader, but the old leader’s server entry stays in the configuration until something explicitly removes it.

2. **Topic leader reassignment never runs for the dead node**  
   `maybeReassignTopicLeaders(deadNodeID)` is only invoked from `Leave()`, and only when the current node is Raft leader. So when the Raft leader dies, no one runs topic leader reassignment for that node.

3. **Metadata and topic layer can stay out of sync**  
   Even if we later add logic to remove the dead node and apply leader change in metadata, the **topic layer is never notified** when metadata changes (see “Topic leader shutdown” below).

### Recommended changes

- **Reconcile Raft config with Serf membership on the Raft leader**  
  Periodically (e.g. in a goroutine or in the same tick as `reconcileNodes`), if this node is Raft leader:
  - Get current Raft configuration and Serf member list.
  - For each Raft server not present in Serf (or marked failed), call `raft.RemoveServer` and then `ApplyNodeRemoveEvent(id)` and `maybeReassignTopicLeaders(id)`.
- Optionally, **on gaining Raft leadership** (if Raft exposes a leadership callback or you poll state), run the same reconciliation once so that the new leader quickly cleans up the previous leader and reassigns topics.

---

## 2. Topic leader shutdown (topic leader node dies, Raft leader still up)

### Current behavior

- Serf detects the node failure; a remaining node (possibly the Raft leader) gets `handleLeave(deadMember)`.
- If the handler runs on the **Raft leader**, `Leave(id)` runs fully: Raft remove, `ApplyNodeRemoveEvent`, and `maybeReassignTopicLeaders(id)`. So metadata is updated: topic gets a new `LeaderNodeID` (an ISR replica) and `LeaderChangeEvent` is applied via Raft to all nodes.

### Gaps

1. **TopicManager is never updated when metadata changes**  
   `MetadataStore.Apply(MetadataEventTypeLeaderChange)` only updates the in-memory metadata (`TopicMetadata.LeaderNodeID`, `LeaderEpoch`, and replicas). **TopicManager** (which decides “am I leader?” and “who do I replicate from?”) is **not** notified. It is only restored once at startup via `RestoreFromMetadata()`. So:
   - **New topic leader (e.g. node-2):** Metadata says node-2 is leader for topic T, but TopicManager on node-2 still has T as a **replica** of the old leader. So node-2 never accepts produces or serves consumer fetches for T.
   - **Other replicas:** They still have `LeaderNodeID` pointing at the old leader in their in-memory topic state, so replication and LEO reporting keep using the wrong leader until restart.

2. **Old leader not added to replica set in metadata**  
   In `metadata.go`, `MetadataEventTypeLeaderChange` updates `LeaderNodeID` and removes the **new** leader from `Replicas` (so the leader is not in its own replica set). It does **not** add the **previous** leader to `Replicas`. If the old leader is still in the cluster (e.g. temporary network partition), you may want it to rejoin as a follower; that would require either adding it in this path or a separate mechanism.

### Recommended changes

- **Notify TopicManager when metadata changes**  
  When a **LeaderChange** (or other relevant) event is applied in the FSM/metadata layer, the coordinator (or server) must tell TopicManager to reconcile for that topic, for example:
  - **This node is the new leader:** Tear down local “replica of X” state for T and call the same logic as `restoreLeaderTopic(topic)` (open local log as leader, no RPC to replicas).
  - **This node is a replica and the leader changed:** Update in-memory `LeaderNodeID` for T (and optionally reconnect replication to the new leader; replication thread already uses `ListReplicaTopics()` which reads from topic state).
  - **This node was leader and is no longer:** Either convert to replica of the new leader (open log as replica, set LeaderNodeID, replication thread will pull from new leader) or leave topic state consistent with metadata so that RestoreFromMetadata-like logic can run.
- **Wire FSM/Apply to TopicManager**  
  The FSM’s `Apply` (or a callback from the code that applies metadata events) should invoke a well-defined “on metadata applied” hook that TopicManager implements (e.g. “reconcile topic T from metadata”) so that all nodes update their in-memory topic role and leader reference as soon as Raft commits the leader change.

---

## 3. Topic replica shutdown and comeback

### Replica shutdown

- Replica node leaves (or fails). Serf emits leave/fail; Raft leader runs `Leave(replicaID)`: node is removed from Raft and from `metadataStore.Nodes`. `reconcileNodes()` on all nodes removes that node from `c.nodes`, so no one will try to use an RPC client to that node.
- Topic metadata still has that node in `TopicMetadata.Replicas` (there is no “remove replica from topic” event when a node leaves). So metadata still lists the replica; only `Nodes` no longer has it. That is mostly acceptable: replication thread uses `c.nodes` and `GetReplicationClient(leaderID)`, and the **replica** node is the one that pulls from the leader. So when the replica is down, the leader just stops receiving RecordLEO from it; ISR can be updated out-of-band (e.g. when LEO is too far behind).

### Replica comeback

- Node rejoins; Serf `MemberJoin`; Raft leader runs `Join()` and `ApplyNodeAddEvent`. Node is back in Raft and in `metadataStore.Nodes`. `reconcileNodes()` adds the node back to `c.nodes`.
- **If the replica process restarted:** On startup, `RestoreFromMetadata()` runs and the node recreates replica topics via `CreateReplicaRemote(topic, leaderID)`, so it has local topic state and will replicate from the leader. Good.
- **If the replica process did not restart** (e.g. network partition healed): The node still has in-memory topic state (replica of leader X). It is back in `c.nodes` on other nodes. The **replication thread on the replica node** will call `GetReplicationClient(leaderID)` and then `ReplicatePipeline`. If the connection was closed while the replica was “away”, the cached **replication client** in `common.Node` may be broken (broken pipe, connection reset). The code does **not** invalidate the client on error, so the same dead connection can be reused every tick and replication keeps failing.

### Gaps

1. **Cached RPC/replication clients never invalidated on failure**  
   `common.Node` keeps `remoteClient` and `replicationClient` forever once created. If the remote node restarted or the connection was closed, the next call still uses the same client and can get “broken pipe” or “connection reset”. The replication thread in `replication_thread.go` does one shot per tick; on error it returns and will retry next tick but with the **same** client.

### Recommended changes

- **Invalidate replication (and optionally RPC) client on connection/IO errors**  
  In `common.Node`, add a method (e.g. `InvalidateReplicationClient()`) that closes and nils `replicationClient`. In `ReplicateTopicsForLeader`, when `replClient.ReplicatePipeline(requests)` returns an error that looks like a connection/transport failure (e.g. broken pipe, connection reset, use of closed connection), call that invalidation for that leader’s node before returning. Next tick, `GetReplicationClient(leaderID)` will create a new connection. Optionally do the same for the general RPC client if you see similar issues for produce/fetch.
- **Optionally: remove replica from topic metadata on node leave**  
  If you want a clean “replica set” in metadata when a node is removed, you could apply an event that removes that node from each topic’s `Replicas` (and possibly trigger ISR recalc). Today the replica set in metadata can contain nodes that are no longer in the cluster; GetTopicReplicaNodes already filters by `c.nodes`, so behavior is mostly correct, but metadata could be kept in sync for clarity and for future “add replica back” flows.

---

## 4. Replication thread robustness

### Current behavior

- `replicateAllTopics()` runs every 1 second. It builds `leaderToTopics` from `ListReplicaTopics()` (so it uses TopicManager’s in-memory view of “I am replica of leader X for topic T”).
- For each leader, it calls `ReplicateTopicsForLeader`. That gets a **cached** `replicationClient` via `GetReplicationClient(leaderNodeID)`. One failure (e.g. leader down, connection closed) causes the function to return an error; the next tick will call it again with the **same** cached client unless the node was removed from `c.nodes` (e.g. after Leave).

### Gaps

1. **No client invalidation on error** (see above).
2. **No backoff or jitter**  
   If the leader is down, every 1s we hammer the (cached) connection. Not critical if we fix invalidation (then we’d reconnect once per tick), but backoff could reduce log noise and load.
3. **Topic leader change not reflected in TopicManager**  
   Even if we invalidate the client, `ListReplicaTopics()` still returns the **old** leader for this topic until TopicManager is updated when metadata applies LeaderChange (see section 2).

### Recommended changes

- Invalidate replication client on pipeline/connection errors (see section 3).
- After fixing TopicManager sync on LeaderChange, replicas will automatically point at the new leader and replication will pull from the right node once the client is refreshed.

---

## 5. Metadata store (metadata.go)

### LeaderChange apply

- In `Apply(MetadataEventTypeLeaderChange)` you do:
  - `tm.LeaderNodeID = e.LeaderNodeID`
  - `tm.LeaderEpoch = e.LeaderEpoch`
  - `delete(tm.Replicas, e.LeaderNodeID)`  
  All of this is inside `if tm != nil`, so no nil dereference. Removing the new leader from the replica set is correct.

### Optional improvement

- **Add previous leader to Replicas when applying LeaderChange**  
  If the event included `PreviousLeaderNodeID` (and that node is still in `ms.Nodes`), you could add it to `tm.Replicas` with a default state (e.g. LEO 0, IsISR false) so that when the old leader rejoins, it is already in the replica set and can catch up. This is optional and depends on whether you want the old leader to automatically rejoin as a follower.

### Replicas and Nodes consistency

- Topic metadata can reference node IDs in `Replicas` that are no longer in `ms.Nodes` (e.g. after RemoveNode). Code that iterates replicas and looks up nodes (e.g. `GetTopicReplicaNodes`) already filters by `c.nodes`, so behavior is safe. Keeping replica set in sync with cluster membership on Leave (e.g. remove left node from all topic Replicas) would make metadata easier to reason about.

---

## 6. Summary: what to change

| Area | Change |
|------|--------|
| **Raft leader shutdown** | On the node that becomes Raft leader (periodically or on leadership gain), reconcile Raft configuration with Serf: remove servers that are not in Serf, apply RemoveNode and `maybeReassignTopicLeaders` for each removed node. |
| **Topic leader shutdown** | When metadata applies a LeaderChange (or equivalent), notify TopicManager so it can update in-memory leader/replica state (new leader promotes to leader, replicas update LeaderNodeID, old leader demotes to replica). Wire FSM/Apply or a callback from metadata apply to TopicManager. |
| **Replica / connection failures** | Invalidate cached replication client in `common.Node` when ReplicatePipeline (or similar) fails with a connection/transport error so the next tick gets a fresh connection. Optionally invalidate general RPC client on similar errors. |
| **Replication thread** | Rely on client invalidation and TopicManager sync; optionally add backoff when replication repeatedly fails for a leader. |
| **Metadata** | Optionally: add previous leader to Replicas in LeaderChange; optionally: on RemoveNode, remove that node from all topic Replicas for consistency. |

Implementing the first three rows (Raft–Serf reconciliation, TopicManager sync on metadata leader change, and client invalidation on connection errors) will make the system robust across Raft leader shutdown, topic leader shutdown, and replica shutdown/comeback.
