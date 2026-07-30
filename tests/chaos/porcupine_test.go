//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/mohitkumar/mlog/api/protocol"
	"github.com/mohitkumar/mlog/client"
	producerclient "github.com/mohitkumar/mlog/producer/client"
	"github.com/mohitkumar/mlog/tests"
)

// logOp is the Porcupine input for one operation against the topic log: an append of
// Value (an AckAll Produce call), or a read of the value at Offset (a Fetch/log read).
type logOp struct {
	isAppend bool
	value    string
	offset   int
}

// logOutput is what an operation is observed to return: for an append, the offset the
// server assigned; for a read, the value found (if any) at the requested offset.
type logOutput struct {
	offset int
	value  string
	found  bool
}

// logModel treats the topic as a single linearizable FIFO log: every Append extends the
// sequence by exactly one element and must report the index it landed at; a Read at a
// given offset is only linearizable once that many elements have already been appended,
// and must return exactly what's there. This is the property AckAll + leader epochs are
// supposed to guarantee across a leader failover — Porcupine reports the history
// non-linearizable if fault injection ever lets two concurrent appends claim the same
// offset, or lets a read observe something other than what a real append put there,
// even if no single assertion in the fault_test.go-style tests happens to catch it.
var logModel = porcupine.Model{
	Init: func() interface{} { return []string{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.([]string)
		in := input.(logOp)
		out := output.(logOutput)
		if in.isAppend {
			if out.offset != len(st) {
				return false, st
			}
			next := make([]string, len(st)+1)
			copy(next, st)
			next[len(st)] = in.value
			return true, next
		}
		if in.offset < 0 || in.offset >= len(st) {
			return false, st
		}
		return out.found && out.value == st[in.offset], st
	},
	Equal: func(a, b interface{}) bool {
		return slices.Equal(a.([]string), b.([]string))
	},
	DescribeOperation: func(input, output interface{}) string {
		in := input.(logOp)
		out := output.(logOutput)
		if in.isAppend {
			return fmt.Sprintf("append(%q) -> offset %d", in.value, out.offset)
		}
		return fmt.Sprintf("read(%d) -> %q (found=%v)", in.offset, out.value, out.found)
	},
}

// opRecorder collects a Porcupine history under concurrent access.
type opRecorder struct {
	mu      sync.Mutex
	history []porcupine.Operation
}

func (r *opRecorder) record(clientID int, in logOp, call time.Time, out logOutput, ret time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.history = append(r.history, porcupine.Operation{
		ClientId: clientID,
		Input:    in,
		Call:     call.UnixNano(),
		Output:   out,
		Return:   ret.UnixNano(),
	})
}

// TestChaos_LinearizableProduceUnderLeaderKill runs several producers concurrently
// against a 3-node cluster while the topic leader is hard-killed mid-stream, records
// every AckAll produce (and, afterward, every offset read back) as a Porcupine
// operation, and checks the resulting history against logModel: is there any valid
// linearization consistent with a single, unbroken FIFO sequence? A broken leader-epoch
// or ISR handoff would show up here as a non-linearizable history — e.g. two concurrent
// appends landing at the same offset, or a later read not matching what was appended —
// which is a strictly stronger check than "nothing in this fixed order was lost"
// (see TestFault_KillLeaderDuringProduce in fault_test.go).
func TestChaos_LinearizableProduceUnderLeaderKill(t *testing.T) {
	node1, node2, node3, cleanup := tests.StartRealThreeNodeCluster(t, "chaos-porcupine")
	defer cleanup()
	nodes := []*tests.RealTestServer{node1, node2, node3}

	ctx := context.Background()
	topicName := "chaos-porcupine-topic"

	remoteClient, err := client.NewRemoteClient(node1.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	if _, err := remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 2,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	remoteClient.Close()

	const numProducers = 4
	const messagesPerProducer = 25
	total := numProducers * messagesPerProducer

	rec := &opRecorder{}
	var wg sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			pc, err := producerclient.NewClient(ctx, bootstrapAddrs(nodes), topicName)
			if err != nil {
				t.Errorf("client %d: NewClient: %v", clientID, err)
				return
			}
			defer pc.Close()
			for i := 0; i < messagesPerProducer; i++ {
				val := fmt.Sprintf("c%d-m%d", clientID, i)
				sendCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
				call := time.Now()
				offset, err := pc.Send(sendCtx, []byte(val), protocol.AckAll)
				ret := time.Now()
				cancel()
				if err != nil {
					t.Errorf("client %d msg %d: Send: %v", clientID, i, err)
					return
				}
				rec.record(clientID, logOp{isAppend: true, value: val}, call, logOutput{offset: int(offset)}, ret)
			}
		}(p)
	}

	// Kill the leader partway through the concurrent produce load, then let the cluster
	// recover on its own — same fault as TestFault_KillLeaderDuringProduce, but now
	// under concurrent multi-producer load and checked for linearizability rather than
	// a single fixed-order "nothing was lost" assertion.
	go func() {
		time.Sleep(300 * time.Millisecond)
		leaderID := waitForTopicLeaderID(t, nodes, topicName, "", 15*time.Second)
		leader := nodeByID(nodes, leaderID)
		if leader == nil {
			return
		}
		t.Logf("chaos: killing leader %s mid-produce", leader.NodeID)
		_ = leader.Kill()
	}()

	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	newLeaderID := waitForTopicLeaderID(t, nodes, topicName, "", 30*time.Second)
	newLeader := nodeByID(nodes, newLeaderID)
	if newLeader == nil {
		t.Fatalf("could not find surviving node for new leader %q", newLeaderID)
	}

	// Fold a read of every offset back from the post-chaos leader into the same
	// history, so Porcupine checks produce and fetch against one consistent model.
	l, err := newLeader.TopicManager.GetLog(topicName)
	if err != nil {
		t.Fatalf("GetLog on leader %s: %v", newLeaderID, err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for uint64(total) > l.LEO() {
		if !time.Now().Before(deadline) {
			t.Fatalf("leader LEO=%d, want %d within timeout", l.LEO(), total)
		}
		time.Sleep(100 * time.Millisecond)
	}
	const offWidth = 8
	for i := 0; i < total; i++ {
		call := time.Now()
		entry, err := l.ReadUncommitted(uint64(i))
		ret := time.Now()
		if err != nil {
			t.Fatalf("ReadUncommitted offset %d: %v", i, err)
		}
		rec.record(numProducers, logOp{isAppend: false, offset: i}, call, logOutput{value: string(entry[offWidth:]), found: true}, ret)
	}

	result, info := porcupine.CheckOperationsVerbose(logModel, rec.history, 0)
	if result != porcupine.Ok {
		const visPath = "/tmp/mlog-chaos-porcupine.html"
		if f, ferr := os.Create(visPath); ferr == nil {
			defer f.Close()
			if verr := porcupine.Visualize(logModel, info, f); verr == nil {
				t.Logf("visualization written to %s", visPath)
			}
		}
		t.Fatalf("produce/read history under leader kill is not linearizable (result=%v)", result)
	}
	t.Logf("✓ %d operations (%d appends, %d reads) linearizable under leader kill", len(rec.history), total, total)
}

// TestChaos_LinearizableProduceUnderFollowerKill runs several producers concurrently
// against a 3-node cluster while a FOLLOWER (not the leader) is hard-killed mid-stream,
// and checks the resulting produce/read history for linearizability — the Porcupine
// counterpart to TestChaos_FollowerFaultIntegrity's direct LEO/ISR/HW assertions, and
// specifically exercises the ISR-expiry fix (TopicManager.expireStaleISR, matching
// Kafka's replica.lag.time.max.ms): without it, the dead follower's stale ISR entry
// pins HW forever, so every AckAll Send after the kill would hang until it timed out
// instead of completing. Here that's checked the Porcupine way: is there a valid
// linearization of every concurrent append/read once the dead follower is fully
// expired out of the picture, not just "did the calls eventually return".
func TestChaos_LinearizableProduceUnderFollowerKill(t *testing.T) {
	node1, node2, node3, cleanup := tests.StartRealThreeNodeCluster(t, "chaos-porcupine-follower")
	defer cleanup()
	nodes := []*tests.RealTestServer{node1, node2, node3}

	ctx := context.Background()
	topicName := "chaos-porcupine-follower-topic"

	remoteClient, err := client.NewRemoteClient(node1.Addr)
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}
	if _, err := remoteClient.CreateTopic(ctx, &protocol.CreateTopicRequest{
		Topic:        topicName,
		ReplicaCount: 2,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	remoteClient.Close()

	leaderID := waitForTopicLeaderID(t, nodes, topicName, "", 15*time.Second)
	leader := nodeByID(nodes, leaderID)
	var follower *tests.RealTestServer
	for _, n := range nodes {
		if n.NodeID != leaderID {
			follower = n
			break
		}
	}
	if leader == nil || follower == nil {
		t.Fatalf("could not resolve leader/follower for topic %q", topicName)
	}

	// Shorten ISRLagTime so AckAll's 5s waitForAllFollowersToCatchUp timeout has
	// comfortable margin over how long the dead follower stays wrongly counted as ISR
	// — see TestChaos_FollowerFaultIntegrity for the same tuning, applied there via
	// direct assertions instead of a linearizability check.
	leader.TopicManager.ISRLagTime = 2 * time.Second

	const numProducers = 4
	const messagesPerProducer = 25
	total := numProducers * messagesPerProducer

	rec := &opRecorder{}
	var wg sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			pc, err := producerclient.NewClient(ctx, bootstrapAddrs(nodes), topicName)
			if err != nil {
				t.Errorf("client %d: NewClient: %v", clientID, err)
				return
			}
			defer pc.Close()
			for i := 0; i < messagesPerProducer; i++ {
				val := fmt.Sprintf("f%d-m%d", clientID, i)
				sendCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
				call := time.Now()
				offset, err := pc.Send(sendCtx, []byte(val), protocol.AckAll)
				ret := time.Now()
				cancel()
				if err != nil {
					t.Errorf("client %d msg %d: Send: %v", clientID, i, err)
					return
				}
				rec.record(clientID, logOp{isAppend: true, value: val}, call, logOutput{offset: int(offset)}, ret)
			}
		}(p)
	}

	// Kill the follower partway through the concurrent produce load. The leader never
	// changes here — only ISR shrinks — so unlike the leader-kill test above, producers
	// never need to reconnect; the only thing keeping AckAll alive is ExpireStaleISR.
	go func() {
		time.Sleep(300 * time.Millisecond)
		t.Logf("chaos: killing follower %s mid-produce", follower.NodeID)
		_ = follower.Kill()
	}()

	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// Fold a read of every offset back from the (unchanged) leader into the same
	// history, so Porcupine checks produce and fetch against one consistent model.
	l, err := leader.TopicManager.GetLog(topicName)
	if err != nil {
		t.Fatalf("GetLog on leader: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for uint64(total) > l.LEO() {
		if !time.Now().Before(deadline) {
			t.Fatalf("leader LEO=%d, want %d within timeout", l.LEO(), total)
		}
		time.Sleep(100 * time.Millisecond)
	}
	const offWidth = 8
	for i := 0; i < total; i++ {
		call := time.Now()
		entry, err := l.ReadUncommitted(uint64(i))
		ret := time.Now()
		if err != nil {
			t.Fatalf("ReadUncommitted offset %d: %v", i, err)
		}
		rec.record(numProducers, logOp{isAppend: false, offset: i}, call, logOutput{value: string(entry[offWidth:]), found: true}, ret)
	}

	result, info := porcupine.CheckOperationsVerbose(logModel, rec.history, 0)
	if result != porcupine.Ok {
		const visPath = "/tmp/mlog-chaos-porcupine-follower.html"
		if f, ferr := os.Create(visPath); ferr == nil {
			defer f.Close()
			if verr := porcupine.Visualize(logModel, info, f); verr == nil {
				t.Logf("visualization written to %s", visPath)
			}
		}
		t.Fatalf("produce/read history under follower kill is not linearizable (result=%v)", result)
	}
	t.Logf("✓ %d operations (%d appends, %d reads) linearizable under follower kill (AckAll stayed alive via ISR expiry)", len(rec.history), total, total)
}
