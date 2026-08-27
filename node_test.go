package miniraft

import (
	"errors"
	"testing"
)

func newThreeNodeCluster(t *testing.T) (map[uint64]*Node, *MemoryTransport) {
	t.Helper()
	transport := NewMemoryTransport()
	nodes := make(map[uint64]*Node)
	for id := uint64(1); id <= 3; id++ {
		peers := make([]uint64, 0, 2)
		for peer := uint64(1); peer <= 3; peer++ {
			if peer != id {
				peers = append(peers, peer)
			}
		}
		node, err := NewNode(Config{
			ID: id, Peers: peers, ElectionTick: 4, HeartbeatTick: 1,
			Transport: transport,
		})
		if err != nil {
			t.Fatalf("NewNode(%d): %v", id, err)
		}
		nodes[id] = node
		transport.Register(node)
	}
	return nodes, transport
}

func electNode(t *testing.T, node *Node) {
	t.Helper()
	for tick := 0; tick < 16 && node.Status().Role != RoleLeader; tick++ {
		if err := node.Tick(); err != nil {
			t.Fatalf("Tick: %v", err)
		}
	}
	if status := node.Status(); status.Role != RoleLeader {
		t.Fatalf("node did not become leader: %+v", status)
	}
}

func TestElectionAndProposalReplication(t *testing.T) {
	nodes, _ := newThreeNodeCluster(t)
	electNode(t, nodes[1])
	if err := nodes[1].Propose([]byte("value")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	for id, node := range nodes {
		status := node.Status()
		if status.Term == 0 || status.LastIndex != 1 || status.CommitIndex != 1 {
			t.Fatalf("node %d did not replicate proposal: %+v", id, status)
		}
	}
}

func TestTickIsExplicitLogicalTime(t *testing.T) {
	nodes, _ := newThreeNodeCluster(t)
	before := nodes[1].Status()
	after := nodes[1].Status()
	if before.Term != after.Term || before.Role != after.Role {
		t.Fatalf("state changed without Tick: before=%+v after=%+v", before, after)
	}
	electNode(t, nodes[1])
}

func TestPauseIsNotCrashRecovery(t *testing.T) {
	nodes, _ := newThreeNodeCluster(t)
	node := nodes[1]
	node.Pause()
	if err := node.Tick(); !errors.Is(err, ErrPaused) {
		t.Fatalf("paused Tick error = %v, want %v", err, ErrPaused)
	}
	if err := node.Step(Message{From: 2, To: 1, Type: MessageRequestVote, Term: 1}); !errors.Is(err, ErrPaused) {
		t.Fatalf("paused Step error = %v, want %v", err, ErrPaused)
	}
	node.Resume()
	if err := node.Tick(); err != nil {
		t.Fatalf("resumed Tick: %v", err)
	}
}

func TestTransportCopiesDeliveredMessages(t *testing.T) {
	nodes, transport := newThreeNodeCluster(t)
	message := Message{
		From: 1, To: 2, Type: MessageAppendEntries, Term: 1,
		Entries: []Entry{{Index: 1, Term: 1, Data: []byte("original")}},
		Commit:  1,
	}
	if err := transport.Send(message); err != nil {
		t.Fatalf("Send: %v", err)
	}
	message.Entries[0].Data[0] = 'X'
	delivered := transport.Delivered()
	if got := string(delivered[0].Entries[0].Data); got != "original" {
		t.Fatalf("delivered payload = %q, want stable copy", got)
	}
	if status := nodes[2].Status(); status.LastIndex != 1 {
		t.Fatalf("target did not process message: %+v", status)
	}
}
