package paxos

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/tusharpanthri/distributed-banking-system/internal/logstream"
	"github.com/tusharpanthri/distributed-banking-system/internal/transport"
)

// callTimeout bounds one outbound message. A dead peer has to fail fast rather
// than hold up the round.
const callTimeout = 2 * time.Second

var (
	// ErrNoQuorum means not enough replicas answered to make progress.
	ErrNoQuorum = errors.New("no quorum")
	// ErrNoLeader means no replica could win an election.
	ErrNoLeader = errors.New("no leader")
	// ErrLocked means a key is held by an in-flight transaction.
	ErrLocked = errors.New("key is locked by another transaction")
)

// Shard is one replication group: a set of replicas and whatever is currently
// known about who leads them.
//
// Leadership is established on demand rather than by a background heartbeat
// timer. A command that finds no leader runs an election first; a command whose
// accept round fails to reach quorum concludes it has lost the shard and
// re-elects. This is a deliberate choice for a demo that is idle between
// keystrokes — a heartbeat loop would burn CPU and flood the log stream to
// discover, every 150ms, that nothing has changed. The election itself is a
// real Paxos prepare round that can and does fail.
type Shard struct {
	id     int
	nodes  []*Node
	byID   map[string]*Node
	ids    []string
	tp     transport.Transport
	log    *logstream.Emitter
	quorum int

	mu       sync.Mutex
	leader   string
	ballot   Ballot
	nextSlot uint64
	round    uint64
}

// NewShard builds a shard of n replicas and registers them on the transport.
func NewShard(id, replicas int, tp transport.Transport, sink logstream.Sink) *Shard {
	s := &Shard{
		id:     id,
		byID:   map[string]*Node{},
		tp:     tp,
		log:    logstream.NewEmitter(sink, fmt.Sprintf("[s%d]", id)),
		quorum: replicas/2 + 1,
	}
	for i := 0; i < replicas; i++ {
		nodeID := fmt.Sprintf("s%dn%d", id, i)
		node := NewNode(nodeID, id)
		s.nodes = append(s.nodes, node)
		s.byID[nodeID] = node
		s.ids = append(s.ids, nodeID)
		tp.Register(nodeID, node.Handle)
	}
	return s
}

func (s *Shard) ID() int           { return s.id }
func (s *Shard) Quorum() int       { return s.quorum }
func (s *Shard) NodeIDs() []string { return append([]string(nil), s.ids...) }
func (s *Shard) Node(id string) *Node {
	return s.byID[id]
}

// Nodes returns the replicas in index order.
func (s *Shard) Nodes() []*Node { return append([]*Node(nil), s.nodes...) }

// Leader reports the currently believed leader, which may be stale: the only
// way to know for sure is to run a round.
func (s *Shard) Leader() (string, Ballot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leader, s.ballot
}

// Term is the ballot number of the current leadership, for display.
func (s *Shard) Term() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ballot.Number
}

// AliveCount is how many of this shard's replicas are running.
func (s *Shard) AliveCount() int {
	faults := s.tp.Faults()
	n := 0
	for _, id := range s.ids {
		if faults.Alive(id) {
			n++
		}
	}
	return n
}

// StepDown forgets the current leader, forcing the next command to elect. It is
// what kill, partition and heal call: they do not choose a leader, they just
// invalidate what was known so the next round finds out for itself.
func (s *Shard) StepDown(reason string) {
	s.mu.Lock()
	had := s.leader
	s.leader = ""
	s.mu.Unlock()

	if had != "" {
		s.log.Leader(fmt.Sprintf("%s is no longer leader: %s", had, reason))
	}
}

// call issues one bounded request.
func (s *Shard) call(ctx context.Context, from, to string, method transport.Method, req any) (any, error) {
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	return s.tp.Call(callCtx, from, to, method, req)
}

// broadcast sends the same method to every replica in parallel and returns the
// replies that came back. Failures are simply absent: a caller counts what it
// received and never waits on what it did not.
func (s *Shard) broadcast(ctx context.Context, from string, method transport.Method, req func(to string) any) []any {
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		replies []any
	)
	for _, to := range s.ids {
		wg.Add(1)
		go func(to string) {
			defer wg.Done()
			reply, err := s.call(ctx, from, to, method, req(to))
			if err != nil {
				return
			}
			mu.Lock()
			replies = append(replies, reply)
			mu.Unlock()
		}(to)
	}
	wg.Wait()
	return replies
}

// Elect runs an election and returns the winning leader and ballot.
//
// Candidates are tried in index order rather than all at once. Simultaneous
// candidates would duel, each invalidating the other's promises, and a real
// deployment breaks that with randomised timeouts. Sequential attempts get the
// same outcome without the noise, and each attempt is a genuine prepare round.
func (s *Shard) Elect(ctx context.Context) (string, Ballot, error) {
	faults := s.tp.Faults()

	if s.AliveCount() < s.quorum {
		return "", Ballot{}, fmt.Errorf("shard s%d: %w (%d/%d alive, needs %d)",
			s.id, ErrNoQuorum, s.AliveCount(), len(s.ids), s.quorum)
	}

	for _, candidate := range s.ids {
		if !faults.Alive(candidate) {
			continue
		}
		leader, ballot, err := s.runElection(ctx, candidate)
		if err == nil {
			return leader, ballot, nil
		}
	}

	return "", Ballot{}, fmt.Errorf("shard s%d: %w (no replica could reach a quorum of promises)",
		s.id, ErrNoLeader)
}

func (s *Shard) runElection(ctx context.Context, candidate string) (string, Ballot, error) {
	s.mu.Lock()
	s.round++
	ballot := Ballot{Number: s.round, Node: candidate}
	s.mu.Unlock()

	s.log.Paxos(fmt.Sprintf("PREPARE %s from %s", ballot, candidate))

	replies := s.broadcast(ctx, candidate, transport.MethodPrepare, func(string) any {
		return PrepareReq{Ballot: ballot}
	})

	promises := 0
	highest := ballot
	adopted := map[uint64]Entry{}
	for _, raw := range replies {
		resp, ok := raw.(PrepareResp)
		if !ok {
			continue
		}
		if resp.Ballot.GreaterThan(highest) {
			highest = resp.Ballot
		}
		if !resp.Promised {
			continue
		}
		promises++
		// Paxos requires the new leader to re-propose the highest-ballot value
		// already accepted at each slot, not its own. Skipping this is how an
		// in-flight decision gets silently replaced.
		for _, e := range resp.Accepted {
			if cur, seen := adopted[e.Slot]; !seen || e.Ballot.GreaterThan(cur.Ballot) {
				adopted[e.Slot] = e
			}
		}
	}

	if promises < s.quorum {
		s.log.Paxos(fmt.Sprintf("PROMISE %d/%d for %s, short of quorum %d",
			promises, len(s.ids), ballot, s.quorum))
		// Jump above whatever we saw so the next attempt is not doomed to the
		// same number.
		s.mu.Lock()
		if highest.Number > s.round {
			s.round = highest.Number
		}
		s.mu.Unlock()
		return "", Ballot{}, ErrNoQuorum
	}

	s.log.Paxos(fmt.Sprintf("PROMISE %d/%d for %s, quorum reached", promises, len(s.ids), ballot))

	// Finish anything the previous leader left in flight before serving.
	slots := make([]uint64, 0, len(adopted))
	for slot := range adopted {
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })

	// Only slots the new leader has not already applied need re-proposing. An
	// applied slot was committed, and a committed value is chosen and immutable
	// — re-proposing it cannot change the outcome and would make every election
	// cost a round per entry in the log.
	settled := s.byID[candidate].Applied()

	var maxSlot uint64
	replayed := 0
	for _, slot := range slots {
		if slot > maxSlot {
			maxSlot = slot
		}
		if slot <= settled {
			continue
		}
		entry := adopted[slot]
		entry.Ballot = ballot
		if err := s.accept(ctx, candidate, ballot, entry); err != nil {
			s.log.Paxos(fmt.Sprintf("recovery of slot %d failed, standing down", slot))
			return "", Ballot{}, err
		}
		replayed++
	}
	if replayed > 0 {
		s.log.Paxos(fmt.Sprintf("recovered %d unfinished slot(s) from the previous leader", replayed))
	}

	s.mu.Lock()
	s.leader = candidate
	s.ballot = ballot
	s.nextSlot = maxSlot + 1
	s.mu.Unlock()

	s.log.Leader(fmt.Sprintf("%s leads s%d at %s", candidate, s.id, ballot))
	return candidate, ballot, nil
}

// accept runs one accept round and commits on success.
func (s *Shard) accept(ctx context.Context, leader string, ballot Ballot, entry Entry) error {
	s.log.Paxos(fmt.Sprintf("ACCEPT slot=%d %s %s", entry.Slot, ballot, entry.Cmd))

	replies := s.broadcast(ctx, leader, transport.MethodAccept, func(string) any {
		return AcceptReq{Ballot: ballot, Entry: entry}
	})

	accepts := 0
	var accepted []AcceptResp
	for _, raw := range replies {
		if resp, ok := raw.(AcceptResp); ok && resp.Accepted {
			accepts++
			accepted = append(accepted, resp)
		}
	}

	if accepts < s.quorum {
		s.log.Paxos(fmt.Sprintf("ACCEPTED %d/%d, short of quorum %d: value not chosen",
			accepts, len(s.ids), s.quorum))
		return fmt.Errorf("shard s%d: %w (%d/%d accepted, needs %d)",
			s.id, ErrNoQuorum, accepts, len(s.ids), s.quorum)
	}

	s.log.Paxos(fmt.Sprintf("ACCEPTED %d/%d, value chosen", accepts, len(s.ids)))

	// Only now is the value chosen, so only now may anyone apply it.
	s.broadcast(ctx, leader, transport.MethodCommit, func(string) any {
		return CommitReq{Ballot: ballot, Entry: entry}
	})
	s.log.Paxos(fmt.Sprintf("COMMIT slot=%d %s", entry.Slot, entry.Cmd))

	s.catchUp(ctx, leader, entry.Slot, accepted)
	return nil
}

// catchUp backfills replicas that accepted this round but are missing earlier
// entries.
//
// A replica that was dead or partitioned away comes back holding an old log. It
// will happily accept the next entry, but it cannot apply it: the state machine
// only folds in slots contiguously, because applying past a gap would make a
// replica's state depend on the order its messages happened to arrive rather
// than on its log. So accepting is not enough — the gap has to be filled, and
// the leader is the one that knows what belongs in it.
func (s *Shard) catchUp(ctx context.Context, leader string, slot uint64, replies []AcceptResp) {
	leaderNode := s.byID[leader]
	if leaderNode == nil {
		return
	}

	for _, reply := range replies {
		// The watermark in the reply was read before this round committed, so
		// every follower reports itself one slot behind. What matters is
		// whether anything is missing *below* the entry just accepted: that is
		// a real gap, and this round's commit will not close it.
		if reply.Node == leader || reply.Applied+1 >= slot {
			continue
		}
		missing := leaderNode.CommittedRange(reply.Applied+1, slot-1)
		if len(missing) == 0 {
			continue
		}
		if _, err := s.call(ctx, leader, reply.Node, transport.MethodLearn, LearnReq{Entries: missing}); err != nil {
			continue
		}
		s.log.Paxos(fmt.Sprintf("%s was behind, sent %d missing entries", reply.Node, len(missing)))
	}
}

// Propose replicates one command. check runs on the leader replica once
// leadership is established and before anything is proposed, so a precondition
// is evaluated against the state the leader will actually write to.
//
// A failed accept round means this node is no longer the leader, or cannot
// reach enough peers to be one. Either way the answer is to find out who leads
// now and try once more, which is what the retry does.
func (s *Shard) Propose(ctx context.Context, cmd Command, check func(*Node) error) error {
	var lastErr error

	for attempt := 0; attempt < 2; attempt++ {
		leader, ballot, err := s.leaderFor(ctx)
		if err != nil {
			return err
		}

		node := s.byID[leader]

		// A leader that loses a quorum of replies cannot tell a value that was
		// never chosen from one that was chosen without it hearing. Reporting
		// failure for the second is how a caller is told nothing happened while
		// the money moved. So before proposing again, ask: after the
		// re-election above, a value that really was chosen has been recovered
		// and applied, and the command id says so.
		if node.HasApplied(cmd.ID) {
			s.log.Paxos(fmt.Sprintf("%s was already chosen; not proposing it twice", cmd))
			return nil
		}

		if check != nil {
			if err := check(node); err != nil {
				return err
			}
		}

		s.mu.Lock()
		slot := s.nextSlot
		s.nextSlot++
		s.mu.Unlock()

		err = s.accept(ctx, leader, ballot, Entry{Slot: slot, Ballot: ballot, Cmd: cmd})
		if err == nil {
			return nil
		}
		lastErr = err

		s.StepDown("lost contact with a quorum")
	}

	// One last look, for the same reason: the second attempt may itself have
	// been chosen without the leader hearing it.
	if leader, _, err := s.leaderFor(ctx); err == nil {
		if s.byID[leader].HasApplied(cmd.ID) {
			s.log.Paxos(fmt.Sprintf("%s was chosen after all", cmd))
			return nil
		}
	}
	return lastErr
}

// leaderFor returns a leader, electing one if none is known or the known one is
// dead.
func (s *Shard) leaderFor(ctx context.Context) (string, Ballot, error) {
	s.mu.Lock()
	leader, ballot := s.leader, s.ballot
	s.mu.Unlock()

	if leader != "" && s.tp.Faults().Alive(leader) {
		return leader, ballot, nil
	}
	return s.Elect(ctx)
}

// ConfirmedLeader returns a leader that has just proved, to a quorum, that it
// still holds the shard.
//
// This is what makes a read linearizable. A leader that has been partitioned
// away still believes it leads and still holds the last value it wrote;
// answering from that belief alone is how a stale read escapes. Asking a
// majority first means at least one replica in any future quorum has confirmed
// this ballot is still current.
func (s *Shard) ConfirmedLeader(ctx context.Context) (string, error) {
	for attempt := 0; attempt < 2; attempt++ {
		leader, ballot, err := s.leaderFor(ctx)
		if err != nil {
			return "", err
		}

		replies := s.broadcast(ctx, leader, transport.MethodPing, func(string) any {
			return PingReq{Ballot: ballot}
		})
		follows := 0
		for _, raw := range replies {
			if resp, ok := raw.(PingResp); ok && resp.Follows {
				follows++
			}
		}
		if follows >= s.quorum {
			s.log.Paxos(fmt.Sprintf("read confirmed by %d/%d at %s", follows, len(s.ids), ballot))
			return leader, nil
		}

		s.log.Paxos(fmt.Sprintf("read not confirmed: %d/%d still follow %s", follows, len(s.ids), ballot))
		s.StepDown("a quorum no longer follows this ballot")
	}
	return "", fmt.Errorf("shard s%d: %w", s.id, ErrNoQuorum)
}
