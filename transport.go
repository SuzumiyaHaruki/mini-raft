package miniraft

import (
	"errors"
	"sync"
)

var ErrUnknownNode = errors.New("mini-raft: message target is not registered")

// Transport is the environment-owned outbound protocol boundary.
type Transport interface {
	Send(Message) error
}

// MemoryTransport is the normal benchmark transport. Send immediately calls the
// target node's Step method, so delivery is synchronous and uncontrollable.
type MemoryTransport struct {
	mu        sync.Mutex
	nodes     map[uint64]*Node
	delivered []Message
}

func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{nodes: make(map[uint64]*Node)}
}

func (transport *MemoryTransport) Register(node *Node) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	transport.nodes[node.ID()] = node
}

func (transport *MemoryTransport) Send(message Message) error {
	stable := message.Clone()
	transport.mu.Lock()
	target := transport.nodes[stable.To]
	if target != nil {
		transport.delivered = append(transport.delivered, stable.Clone())
	}
	transport.mu.Unlock()
	if target == nil {
		return ErrUnknownNode
	}
	return target.Step(stable)
}

func (transport *MemoryTransport) Delivered() []Message {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	result := make([]Message, len(transport.delivered))
	for index, message := range transport.delivered {
		result[index] = message.Clone()
	}
	return result
}

func (transport *MemoryTransport) ClearDelivered() {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	transport.delivered = nil
}
