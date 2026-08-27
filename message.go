package miniraft

// MessageType identifies one protocol message family.
type MessageType string

const (
	MessageRequestVote         MessageType = "request_vote"
	MessageRequestVoteResponse MessageType = "request_vote_response"
	MessageAppendEntries       MessageType = "append_entries"
	MessageAppendResponse      MessageType = "append_entries_response"
)

// Entry is one replicated application value.
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// Clone returns a stable copy that does not share entry payloads.
func (entry Entry) Clone() Entry {
	copy := entry
	copy.Data = append([]byte(nil), entry.Data...)
	return copy
}

// Message is the protocol's normal transport value.
type Message struct {
	From    uint64
	To      uint64
	Type    MessageType
	Term    uint64
	Reject  bool
	Entries []Entry
	Commit  uint64
}

// Clone returns a stable copy that can be retained after Send returns.
func (message Message) Clone() Message {
	copy := message
	copy.Entries = make([]Entry, len(message.Entries))
	for index, entry := range message.Entries {
		copy.Entries[index] = entry.Clone()
	}
	return copy
}
