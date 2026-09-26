package paxos

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/tusharpanthri/distributed-banking-system/internal/transport"
)

// Node is one replica: an acceptor, a log, and the state machine that log
// produces.
//
// Every method here is short and holds the mutex only for its own bookkeeping.
// No handler makes an outbound call, and the leader code never holds this lock
// across a Call. The v1 lab held its server mutex across synchronous RPCs to
// peers and relied on a 10ms gap between transactions to avoid deadlocking;
// that is not a property worth reproducing.
type Node struct {
	id    string
	shard int

	mu sync.Mutex
	// promised is the highest ballot this acceptor has promised to. It may only
	// ever increase.
	promised Ballot
	// accepted holds entries this node has accepted, by slot. An entry is
	// present here before it is known to be chosen.
	accepted map[uint64]Entry
	// committed marks slots this node has been told are chosen.
	committed map[uint64]bool
	// applied is the highest contiguous slot folded into state.
	applied uint64

	state map[string]int64
	// locks records which transaction holds a key, set by CmdPrepare and
	// cleared by CmdCommit or CmdAbort. A locked key refuses further writes.
	locks map[string]string
	// held remembers each prepared transaction's delta so CmdAbort can reverse
	// exactly what CmdPrepare applied.
	held map[string][]Command
	// seen records the command ids already folded into state, so a command that
	// reached the log twice takes effect once. See Command.ID.
	seen map[string]bool
}

// NewNode returns an empty replica.
func NewNode(id string, shard int) *Node {
	return &Node{
		id:        id,
		shard:     shard,
		accepted:  map[uint64]Entry{},
		committed: map[uint64]bool{},
		state:     map[string]int64{},
		locks:     map[string]string{},
		held:      map[string][]Command{},
		seen:      map[string]bool{},
	}
}

func (n *Node) ID() string { return n.id }

// Handle is the transport entry point for this node.
func (n *Node) Handle(_ context.Context, method transport.Method, req any) (any, error) {
	switch method {
	case transport.MethodPrepare:
		r, ok := req.(PrepareReq)
		if !ok {
			return nil, fmt.Errorf("prepare: unexpected request %T", req)
		}
		return n.Prepare(r), nil
	case transport.MethodAccept:
		r, ok := req.(AcceptReq)
		if !ok {
			return nil, fmt.Errorf("accept: unexpected request %T", req)
		}
		return n.Accept(r), nil
	case transport.MethodCommit:
		r, ok := req.(CommitReq)
		if !ok {
			return nil, fmt.Errorf("commit: unexpected request %T", req)
		}
		return n.Commit(r), nil
	case transport.MethodLearn:
		r, ok := req.(LearnReq)
		if !ok {
			return nil, fmt.Errorf("learn: unexpected request %T", req)
		}
		return n.Learn(r), nil
	case transport.MethodPing:
		r, ok := req.(PingReq)
		if !ok {
			return nil, fmt.Errorf("ping: unexpected request %T", req)
		}
		return n.Ping(r), nil
	}
	return nil, fmt.Errorf("unknown method %q", method)
}

// Prepare answers an election. The acceptor promises only to a strictly higher
// ballot, and hands back everything it has accepted so that the new leader can
// finish whatever its predecessor left in flight.
func (n *Node) Prepare(req PrepareReq) PrepareResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !req.Ballot.GreaterThan(n.promised) {
		// Refusing is not enough on its own: reporting the promise we already
		// hold lets the candidate jump above it instead of retrying forever at
		// a number that can never win.
		return PrepareResp{Node: n.id, Promised: false, Ballot: n.promised}
	}

	n.promised = req.Ballot
	return PrepareResp{
		Node:     n.id,
		Promised: true,
		Ballot:   n.promised,
		Accepted: n.acceptedEntriesLocked(),
	}
}

// Accept records a value at a slot, if the proposing ballot is at least the one
// this acceptor promised to.
func (n *Node) Accept(req AcceptReq) AcceptResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.promised.GreaterThan(req.Ballot) {
		return AcceptResp{Node: n.id, Accepted: false, Ballot: n.promised}
	}
	// Accepting also promises: a node that accepts under a ballot must not
	// afterwards promise to something lower.
	n.promised = req.Ballot
	n.accepted[req.Entry.Slot] = req.Entry
	return AcceptResp{Node: n.id, Accepted: true, Ballot: n.promised, Applied: n.applied}
}

// Learn takes committed entries this replica missed and folds in whatever
// becomes contiguous.
//
// Nothing is ever removed: no slot this replica holds is dropped, and no state
// is wiped. The v1 lab synchronised by picking whichever peer reported the most
// rows and overwriting the local table from it, so one confused peer could
// erase a replica's committed history. Length is not authority.
//
// A chosen value, however, is authority. These entries are committed, which
// means a majority accepted them and they can never change, so where this
// replica holds something else at that slot -- a leftover from a round that was
// never chosen -- the chosen value replaces it. Keeping the local one and merely
// marking the slot committed is how two replicas end up committed on different
// values at the same slot.
func (n *Node) Learn(req LearnReq) LearnResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, e := range req.Entries {
		n.accepted[e.Slot] = e
		n.committed[e.Slot] = true
	}
	n.applyContiguousLocked()
	return LearnResp{Node: n.id, Applied: n.applied}
}

// HasApplied reports whether a command with this id has been folded into state.
// It is how a leader answers "did that actually happen?" after a round whose
// replies it never saw.
func (n *Node) HasApplied(id string) bool {
	if id == "" {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.seen[id]
}

// Applied is the highest contiguous slot this replica has folded into state.
func (n *Node) Applied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.applied
}

// CommittedRange returns the committed entries in [from, to], for backfilling a
// replica that is behind.
func (n *Node) CommittedRange(from, to uint64) []Entry {
	n.mu.Lock()
	defer n.mu.Unlock()

	var out []Entry
	for slot := from; slot <= to; slot++ {
		if !n.committed[slot] {
			continue
		}
		if entry, ok := n.accepted[slot]; ok {
			out = append(out, entry)
		}
	}
	return out
}

// Commit records a chosen value and folds everything newly contiguous into
// state.
//
// The entry is taken as authoritative. The leader sends this only once a
// majority has accepted, which makes the value chosen, and a chosen value never
// changes. So a replica holding something else at that slot -- because it missed
// this round's accept and still has a stale entry from a failed one -- must take
// the leader's, not keep its own.
func (n *Node) Commit(req CommitReq) CommitResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.accepted[req.Entry.Slot] = req.Entry
	n.committed[req.Entry.Slot] = true
	n.applyContiguousLocked()
	return CommitResp{Node: n.id}
}

// Ping reports whether this acceptor still follows the caller's ballot.
func (n *Node) Ping(req PingReq) PingResp {
	n.mu.Lock()
	defer n.mu.Unlock()
	return PingResp{
		Node:    n.id,
		Follows: n.promised.Equal(req.Ballot),
		Ballot:  n.promised,
	}
}

// acceptedEntriesLocked returns every accepted entry in slot order.
func (n *Node) acceptedEntriesLocked() []Entry {
	out := make([]Entry, 0, len(n.accepted))
	for _, e := range n.accepted {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slot < out[j].Slot })
	return out
}

// applyContiguousLocked folds committed slots into the state machine in order,
// stopping at the first gap.
//
// Applying strictly in slot order is what makes every replica's state a
// function of its log alone. A gap means an earlier decision has not arrived
// yet; applying past it would let two replicas that hold the same entries end
// up in different states depending on arrival order.
func (n *Node) applyContiguousLocked() {
	for {
		next := n.applied + 1
		if !n.committed[next] {
			return
		}
		entry, ok := n.accepted[next]
		if !ok {
			return
		}
		n.applyLocked(entry.Cmd)
		n.applied = next
	}
}

func (n *Node) applyLocked(cmd Command) {
	// A duplicate reaches every replica identically, so skipping it keeps the
	// replicas in step rather than pulling them apart.
	if cmd.ID != "" {
		if n.seen[cmd.ID] {
			return
		}
		n.seen[cmd.ID] = true
	}

	switch cmd.Type {
	case CmdPut:
		n.state[cmd.Key] = cmd.Value
	case CmdTransfer:
		n.state[cmd.Key] -= cmd.Value
		n.state[cmd.Dest] += cmd.Value
	case CmdPrepare:
		n.state[cmd.Key] += cmd.Value
		n.locks[cmd.Key] = cmd.TxID
		n.held[cmd.TxID] = append(n.held[cmd.TxID], cmd)
	case CmdCommit:
		for _, held := range n.held[cmd.TxID] {
			if n.locks[held.Key] == cmd.TxID {
				delete(n.locks, held.Key)
			}
		}
		delete(n.held, cmd.TxID)
	case CmdAbort:
		for _, held := range n.held[cmd.TxID] {
			n.state[held.Key] -= held.Value
			if n.locks[held.Key] == cmd.TxID {
				delete(n.locks, held.Key)
			}
		}
		delete(n.held, cmd.TxID)
	case CmdNoop:
	}
}

// Snapshot is a read-only view of a replica, for status and for tests.
type Snapshot struct {
	ID        string
	Promised  Ballot
	Applied   uint64
	LogSize   int
	State     map[string]int64
	Locks     map[string]string
	Committed []Entry
}

// Snapshot returns the replica's current state.
func (n *Node) Snapshot() Snapshot {
	n.mu.Lock()
	defer n.mu.Unlock()

	state := make(map[string]int64, len(n.state))
	for k, v := range n.state {
		state[k] = v
	}
	locks := make(map[string]string, len(n.locks))
	for k, v := range n.locks {
		locks[k] = v
	}
	var committed []Entry
	for _, e := range n.acceptedEntriesLocked() {
		if n.committed[e.Slot] {
			committed = append(committed, e)
		}
	}
	return Snapshot{
		ID:        n.id,
		Promised:  n.promised,
		Applied:   n.applied,
		LogSize:   len(n.accepted),
		State:     state,
		Locks:     locks,
		Committed: committed,
	}
}

// Value reads a key from the applied state. It is the local read a leader
// answers with once it has confirmed it still leads.
func (n *Node) Value(key string) (int64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	v, ok := n.state[key]
	return v, ok
}

// Locked reports the transaction holding a key, if any.
func (n *Node) Locked(key string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	tx, ok := n.locks[key]
	return tx, ok
}

// LastSlot is the highest slot this node has accepted.
func (n *Node) LastSlot() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	var max uint64
	for slot := range n.accepted {
		if slot > max {
			max = slot
		}
	}
	return max
}
