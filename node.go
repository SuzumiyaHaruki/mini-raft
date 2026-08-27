package miniraft

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrInvalidConfig = errors.New("mini-raft: invalid configuration")
	ErrPaused        = errors.New("mini-raft: node is paused")
	ErrNotLeader     = errors.New("mini-raft: proposal requires a leader")
	ErrWrongTarget   = errors.New("mini-raft: message delivered to the wrong node")
)

type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)

type Config struct {
	ID            uint64
	Peers         []uint64
	ElectionTick  int
	HeartbeatTick int
	Transport     Transport
}

type Status struct {
	ID                        uint64
	Running                   bool
	Role                      Role
	Term                      uint64
	Vote                      uint64
	Leader                    uint64
	CommitIndex               uint64
	LastIndex                 uint64
	RandomizedElectionTimeout int
}

// Node owns both persistent-looking and volatile-looking state in one object.
// Pause and Resume only control execution; they do not define crash recovery.
type Node struct {
	mu sync.Mutex

	id        uint64
	peers     []uint64
	transport Transport
	random    *rand.Rand

	electionTick              int
	heartbeatTick             int
	randomizedElectionTimeout int
	electionElapsed           int
	heartbeatElapsed          int

	running bool
	role    Role
	term    uint64
	vote    uint64
	leader  uint64
	votes   map[uint64]bool
	log     []Entry
	commit  uint64
}

func NewNode(config Config) (*Node, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	node := &Node{
		id:            config.ID,
		peers:         append([]uint64(nil), config.Peers...),
		transport:     config.Transport,
		random:        rand.New(rand.NewSource(time.Now().UnixNano() + int64(config.ID))),
		electionTick:  config.ElectionTick,
		heartbeatTick: config.HeartbeatTick,
		running:       true,
		role:          RoleFollower,
		votes:         make(map[uint64]bool),
	}
	node.resetElectionTimeoutLocked()
	return node, nil
}

func validateConfig(config Config) error {
	if config.ID == 0 || config.Transport == nil || config.ElectionTick <= 0 || config.HeartbeatTick <= 0 || config.HeartbeatTick >= config.ElectionTick {
		return ErrInvalidConfig
	}
	seen := map[uint64]bool{config.ID: true}
	for _, peer := range config.Peers {
		if peer == 0 || seen[peer] {
			return ErrInvalidConfig
		}
		seen[peer] = true
	}
	return nil
}

func (node *Node) ID() uint64 {
	return node.id
}

// Tick advances exactly one unit of protocol logical time.
func (node *Node) Tick() error {
	node.mu.Lock()
	if !node.running {
		node.mu.Unlock()
		return ErrPaused
	}
	var outbound []Message
	if node.role == RoleLeader {
		node.heartbeatElapsed++
		if node.heartbeatElapsed >= node.heartbeatTick {
			node.heartbeatElapsed = 0
			outbound = node.appendMessagesLocked(nil)
		}
	} else {
		node.electionElapsed++
		if node.electionElapsed >= node.randomizedElectionTimeout {
			outbound = node.campaignLocked()
		}
	}
	node.mu.Unlock()
	return node.emit(outbound)
}

// Step is the normal protocol input boundary.
func (node *Node) Step(message Message) error {
	if message.To != node.id {
		return ErrWrongTarget
	}
	node.mu.Lock()
	if !node.running {
		node.mu.Unlock()
		return ErrPaused
	}
	outbound := node.stepLocked(message.Clone())
	node.mu.Unlock()
	return node.emit(outbound)
}

func (node *Node) Propose(data []byte) error {
	node.mu.Lock()
	if !node.running {
		node.mu.Unlock()
		return ErrPaused
	}
	if node.role != RoleLeader {
		node.mu.Unlock()
		return ErrNotLeader
	}
	entry := Entry{
		Index: uint64(len(node.log) + 1),
		Term:  node.term,
		Data:  append([]byte(nil), data...),
	}
	node.log = append(node.log, entry)
	node.commit = entry.Index
	outbound := node.appendMessagesLocked([]Entry{entry})
	node.mu.Unlock()
	return node.emit(outbound)
}

// Pause and Resume model a scheduler pause, not process crash/restart.
func (node *Node) Pause() {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.running = false
}

func (node *Node) Resume() {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.running = true
}

func (node *Node) Status() Status {
	node.mu.Lock()
	defer node.mu.Unlock()
	return Status{
		ID:                        node.id,
		Running:                   node.running,
		Role:                      node.role,
		Term:                      node.term,
		Vote:                      node.vote,
		Leader:                    node.leader,
		CommitIndex:               node.commit,
		LastIndex:                 uint64(len(node.log)),
		RandomizedElectionTimeout: node.randomizedElectionTimeout,
	}
}

func (node *Node) stepLocked(message Message) []Message {
	switch message.Type {
	case MessageRequestVote:
		return node.handleRequestVoteLocked(message)
	case MessageRequestVoteResponse:
		return node.handleRequestVoteResponseLocked(message)
	case MessageAppendEntries:
		return node.handleAppendEntriesLocked(message)
	case MessageAppendResponse:
		return nil
	default:
		return nil
	}
}

func (node *Node) handleRequestVoteLocked(message Message) []Message {
	if message.Term > node.term {
		node.becomeFollowerLocked(message.Term, 0)
	}
	granted := false
	if message.Term == node.term && (node.vote == 0 || node.vote == message.From) {
		node.vote = message.From
		node.electionElapsed = 0
		node.resetElectionTimeoutLocked()
		granted = true
	}
	return []Message{{
		From:   node.id,
		To:     message.From,
		Type:   MessageRequestVoteResponse,
		Term:   node.term,
		Reject: !granted,
	}}
}

func (node *Node) handleRequestVoteResponseLocked(message Message) []Message {
	if node.role != RoleCandidate || message.Term != node.term || message.Reject {
		return nil
	}
	node.votes[message.From] = true
	if len(node.votes) < node.quorumLocked() {
		return nil
	}
	node.role = RoleLeader
	node.leader = node.id
	node.heartbeatElapsed = 0
	return node.appendMessagesLocked(nil)
}

func (node *Node) handleAppendEntriesLocked(message Message) []Message {
	if message.Term < node.term {
		return []Message{{
			From: node.id, To: message.From, Type: MessageAppendResponse,
			Term: node.term, Reject: true,
		}}
	}
	if message.Term > node.term || node.role != RoleFollower {
		node.becomeFollowerLocked(message.Term, message.From)
	}
	node.leader = message.From
	node.electionElapsed = 0
	for _, entry := range message.Entries {
		if entry.Index <= uint64(len(node.log)) {
			continue
		}
		node.log = append(node.log, entry.Clone())
	}
	if message.Commit > node.commit {
		node.commit = min(message.Commit, uint64(len(node.log)))
	}
	return []Message{{
		From: node.id, To: message.From, Type: MessageAppendResponse,
		Term: node.term,
	}}
}

func (node *Node) campaignLocked() []Message {
	node.role = RoleCandidate
	node.term++
	node.vote = node.id
	node.leader = 0
	node.votes = map[uint64]bool{node.id: true}
	node.electionElapsed = 0
	node.resetElectionTimeoutLocked()
	if node.quorumLocked() == 1 {
		node.role = RoleLeader
		node.leader = node.id
		return nil
	}
	messages := make([]Message, 0, len(node.peers))
	for _, peer := range node.peers {
		messages = append(messages, Message{
			From: node.id, To: peer, Type: MessageRequestVote, Term: node.term,
		})
	}
	return messages
}

func (node *Node) becomeFollowerLocked(term uint64, leader uint64) {
	if term > node.term {
		node.vote = 0
	}
	node.term = term
	node.role = RoleFollower
	node.leader = leader
	node.votes = make(map[uint64]bool)
	node.electionElapsed = 0
	node.resetElectionTimeoutLocked()
}

func (node *Node) appendMessagesLocked(entries []Entry) []Message {
	messages := make([]Message, 0, len(node.peers))
	for _, peer := range node.peers {
		messageEntries := make([]Entry, len(entries))
		for index, entry := range entries {
			messageEntries[index] = entry.Clone()
		}
		messages = append(messages, Message{
			From: node.id, To: peer, Type: MessageAppendEntries, Term: node.term,
			Entries: messageEntries, Commit: node.commit,
		})
	}
	return messages
}

func (node *Node) resetElectionTimeoutLocked() {
	node.randomizedElectionTimeout = node.electionTick + node.random.Intn(node.electionTick)
}

func (node *Node) quorumLocked() int {
	return (len(node.peers)+1)/2 + 1
}

func (node *Node) emit(messages []Message) error {
	var firstError error
	for _, message := range messages {
		if err := node.transport.Send(message); err != nil && firstError == nil {
			firstError = fmt.Errorf("send %s from %d to %d: %w", message.Type, message.From, message.To, err)
		}
	}
	return firstError
}
