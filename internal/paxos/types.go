// Package paxos implements Multi-Paxos over a Transport: one replicated log per
// shard, holding commands against a map of key to balance.
//
// Multi-Paxos, not single-decree: a leader runs prepare once to win the shard,
// and then every write is a single accept round at the next slot. The v1 lab in
// distributed-banking/ re-ran prepare for every transaction and proposed its
// whole log as the value, which is correct but is not what anyone means by
// Multi-Paxos.
package paxos

import "fmt"

// Ballot orders proposals. The node id breaks ties so that two nodes reaching
// the same round number still produce strictly ordered ballots — comparing only
// the number lets an acceptor follow two different proposals at "the same"
// ballot, which is the bug the v1 lab shipped with.
type Ballot struct {
	Number uint64
	Node   string
}

// GreaterThan reports whether b is strictly above other.
func (b Ballot) GreaterThan(other Ballot) bool {
	if b.Number != other.Number {
		return b.Number > other.Number
	}
	return b.Node > other.Node
}

// Equal reports exact equality.
func (b Ballot) Equal(other Ballot) bool {
	return b.Number == other.Number && b.Node == other.Node
}

// IsZero reports whether this is the empty ballot, meaning nothing promised.
func (b Ballot) IsZero() bool { return b.Number == 0 && b.Node == "" }

func (b Ballot) String() string {
	if b.IsZero() {
		return "<none>"
	}
	return fmt.Sprintf("<%d,%s>", b.Number, b.Node)
}

// CmdType names what a log entry does to the state machine.
type CmdType string

const (
	// CmdPut sets a key outright.
	CmdPut CmdType = "put"
	// CmdTransfer moves value between two keys in the same shard, in one entry.
	CmdTransfer CmdType = "transfer"
	// CmdPrepare is the 2PC prepared state: apply the delta and hold the key
	// locked until the coordinator decides. Value is signed — negative on the
	// source shard, positive on the destination.
	CmdPrepare CmdType = "prepare"
	// CmdCommit releases the locks a CmdPrepare took, keeping the delta.
	CmdCommit CmdType = "commit"
	// CmdAbort reverses the delta and releases the locks.
	CmdAbort CmdType = "abort"
	// CmdNoop occupies a slot the new leader recovered but could not identify.
	CmdNoop CmdType = "noop"
)

// Command is one entry's effect on the state machine.
type Command struct {
	// ID identifies this command uniquely, and survives re-proposal.
	//
	// A leader that loses a quorum of replies cannot tell "not chosen" from
	// "chosen, but I did not hear about it", so it re-proposes at a fresh slot.
	// If the first round had in fact been chosen, the command is now in the log
	// twice. Ordering cannot fix that and neither can a cleverer leader: the
	// state machine has to recognise a command it has already applied. Every
	// command that is not naturally idempotent needs this.
	ID    string
	Type  CmdType
	TxID  string // set for the 2PC commands
	Key   string
	Dest  string // CmdTransfer only
	Value int64
}

func (c Command) String() string {
	switch c.Type {
	case CmdPut:
		return fmt.Sprintf("put %s=%d", c.Key, c.Value)
	case CmdTransfer:
		return fmt.Sprintf("transfer %s->%s %d", c.Key, c.Dest, c.Value)
	case CmdPrepare:
		return fmt.Sprintf("prepare %s %s%+d", c.TxID, c.Key, c.Value)
	case CmdCommit:
		return fmt.Sprintf("commit %s", c.TxID)
	case CmdAbort:
		return fmt.Sprintf("abort %s", c.TxID)
	default:
		return "noop"
	}
}

// Entry is one slot of the replicated log.
type Entry struct {
	Slot   uint64
	Ballot Ballot
	Cmd    Command
}

// Wire types. They are plain structs so that a future gRPC transport has an
// obvious mapping and so that the in-process transport can pass them directly.

type PrepareReq struct {
	Ballot Ballot
}

type PrepareResp struct {
	Node string
	// Promised is false when the acceptor has already promised to a higher
	// ballot. Ballot then reports what it is following, so the candidate can
	// step over it instead of retrying at the same number forever.
	Promised bool
	Ballot   Ballot
	// Accepted is everything this acceptor holds. A new leader needs it to
	// finish rounds its predecessor started rather than overwriting them.
	Accepted []Entry
}

type AcceptReq struct {
	Ballot Ballot
	Entry  Entry
}

type AcceptResp struct {
	Node     string
	Accepted bool
	Ballot   Ballot
	// Applied is how far this replica has folded its log into state. A leader
	// uses it to notice a replica that is behind and backfill it.
	Applied uint64
}

// LearnReq carries committed entries to a replica that missed them.
type LearnReq struct {
	Entries []Entry
}

type LearnResp struct {
	Node    string
	Applied uint64
}

// CommitReq announces that a value has been chosen.
//
// It carries the whole entry rather than just its slot number. A replica whose
// accept was dropped still holds whatever the previous, failed round left at
// that slot; told only "slot 77 is committed", it would commit that stale value
// while the rest of the cluster commits the real one, and the replicas would
// diverge with nothing to detect it. Naming the value removes the ambiguity,
// and a chosen value is immutable, so a replica can safely take it as given.
type CommitReq struct {
	Ballot Ballot
	Entry  Entry
}

type CommitResp struct {
	Node string
}

type PingReq struct {
	Ballot Ballot
}

type PingResp struct {
	Node string
	// Follows reports whether the acceptor still promises to the caller's
	// ballot. A leader that cannot collect a quorum of these has lost the
	// shard, whether or not it has noticed yet.
	Follows bool
	Ballot  Ballot
}
