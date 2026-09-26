package paxos

import "testing"

func TestBallotOrdersByNumberThenNode(t *testing.T) {
	tests := []struct {
		a, b Ballot
		want bool
	}{
		{Ballot{2, "s0n0"}, Ballot{1, "s0n9"}, true}, // number dominates
		{Ballot{1, "s0n9"}, Ballot{2, "s0n0"}, false},
		{Ballot{1, "s0n1"}, Ballot{1, "s0n0"}, true}, // node breaks the tie
		{Ballot{1, "s0n0"}, Ballot{1, "s0n1"}, false},
		{Ballot{1, "s0n0"}, Ballot{1, "s0n0"}, false}, // strict, not >=
		{Ballot{1, "s0n0"}, Ballot{}, true},           // anything beats nothing
	}

	for _, tt := range tests {
		if got := tt.a.GreaterThan(tt.b); got != tt.want {
			t.Errorf("%v.GreaterThan(%v) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

// Two nodes independently reaching round 1 must still produce strictly ordered
// ballots. Comparing only the number leaves an acceptor unable to tell the two
// proposals apart, so it follows both — which is the safety bug the v1 lab had.
func TestSameRoundDifferentNodesAreStillOrdered(t *testing.T) {
	a := Ballot{Number: 1, Node: "s0n0"}
	b := Ballot{Number: 1, Node: "s0n1"}

	if a.GreaterThan(b) == b.GreaterThan(a) {
		t.Fatalf("%v and %v are not strictly ordered against each other", a, b)
	}
	if a.Equal(b) {
		t.Errorf("%v and %v compare equal", a, b)
	}
}

func TestBallotOrderIsTransitive(t *testing.T) {
	ballots := []Ballot{
		{1, "s0n0"}, {1, "s0n1"}, {1, "s0n2"},
		{2, "s0n0"}, {2, "s0n1"}, {3, "s0n0"},
	}
	for i := range ballots {
		for j := range ballots {
			later := ballots[i].GreaterThan(ballots[j])
			earlier := ballots[j].GreaterThan(ballots[i])
			if i == j {
				if later || earlier {
					t.Errorf("%v compares unequal to itself", ballots[i])
				}
				continue
			}
			if later == earlier {
				t.Errorf("%v and %v are not strictly ordered", ballots[i], ballots[j])
			}
			if (i > j) != later {
				t.Errorf("%v vs %v ordered against the declared sequence", ballots[i], ballots[j])
			}
		}
	}
}

// An acceptor promises only upward, and reports what it is already holding so a
// losing candidate can step over it rather than retrying at a dead number.
func TestAcceptorPromisesOnlyToHigherBallots(t *testing.T) {
	n := NewNode("s0n0", 0)

	resp := n.Prepare(PrepareReq{Ballot: Ballot{2, "s0n1"}})
	if !resp.Promised {
		t.Fatal("first prepare was refused")
	}

	lower := n.Prepare(PrepareReq{Ballot: Ballot{1, "s0n2"}})
	if lower.Promised {
		t.Error("acceptor promised to a lower ballot")
	}
	if !lower.Ballot.Equal(Ballot{2, "s0n1"}) {
		t.Errorf("refusal reported %v, want the promise it is holding", lower.Ballot)
	}

	same := n.Prepare(PrepareReq{Ballot: Ballot{2, "s0n1"}})
	if same.Promised {
		t.Error("acceptor re-promised to the ballot it already holds; promises must be strict")
	}
}

func TestAcceptorRejectsAcceptsBelowItsPromise(t *testing.T) {
	n := NewNode("s0n0", 0)
	n.Prepare(PrepareReq{Ballot: Ballot{5, "s0n1"}})

	resp := n.Accept(AcceptReq{
		Ballot: Ballot{4, "s0n2"},
		Entry:  Entry{Slot: 1, Cmd: Command{Type: CmdPut, Key: "a", Value: 1}},
	})
	if resp.Accepted {
		t.Error("acceptor accepted a value under a ballot below its promise")
	}
	if _, ok := n.Value("a"); ok {
		t.Error("a rejected accept still reached the state machine")
	}
}

// A gap in the log must stop the state machine, not be applied around.
func TestStateMachineAppliesOnlyContiguousSlots(t *testing.T) {
	n := NewNode("s0n0", 0)
	ballot := Ballot{1, "s0n0"}
	n.Prepare(PrepareReq{Ballot: ballot})

	n.Accept(AcceptReq{Ballot: ballot, Entry: Entry{Slot: 1, Ballot: ballot,
		Cmd: Command{Type: CmdPut, Key: "a", Value: 1}}})
	n.Accept(AcceptReq{Ballot: ballot, Entry: Entry{Slot: 3, Ballot: ballot,
		Cmd: Command{Type: CmdPut, Key: "a", Value: 3}}})

	n.Commit(CommitReq{Ballot: ballot, Entry: Entry{Slot: 1, Ballot: ballot,
		Cmd: Command{Type: CmdPut, Key: "a", Value: 1}}})
	n.Commit(CommitReq{Ballot: ballot, Entry: Entry{Slot: 3, Ballot: ballot,
		Cmd: Command{Type: CmdPut, Key: "a", Value: 3}}})

	if value, _ := n.Value("a"); value != 1 {
		t.Errorf("a = %d, want 1: slot 3 was applied across the gap at slot 2", value)
	}
	if n.Applied() != 1 {
		t.Errorf("applied watermark is %d, want 1", n.Applied())
	}

	// Filling the gap releases both.
	n.Accept(AcceptReq{Ballot: ballot, Entry: Entry{Slot: 2, Ballot: ballot,
		Cmd: Command{Type: CmdPut, Key: "a", Value: 2}}})
	n.Commit(CommitReq{Ballot: ballot, Entry: Entry{Slot: 2, Ballot: ballot,
		Cmd: Command{Type: CmdPut, Key: "a", Value: 2}}})

	if value, _ := n.Value("a"); value != 3 {
		t.Errorf("a = %d after the gap was filled, want 3", value)
	}
	if n.Applied() != 3 {
		t.Errorf("applied watermark is %d, want 3", n.Applied())
	}
}

// Learning is additive. The v1 lab replaced a replica's table with whichever
// peer reported the most rows, so a confused peer could erase committed
// history.
func TestLearnNeverDropsCommittedEntries(t *testing.T) {
	n := NewNode("s0n0", 0)
	ballot := Ballot{1, "s0n0"}
	n.Prepare(PrepareReq{Ballot: ballot})
	n.Accept(AcceptReq{Ballot: ballot, Entry: Entry{Slot: 1, Ballot: ballot,
		Cmd: Command{Type: CmdPut, Key: "a", Value: 7}}})
	n.Commit(CommitReq{Ballot: ballot, Entry: Entry{Slot: 1, Ballot: ballot,
		Cmd: Command{Type: CmdPut, Key: "a", Value: 7}}})

	// A peer sends an empty view of the world.
	n.Learn(LearnReq{Entries: nil})

	if value, ok := n.Value("a"); !ok || value != 7 {
		t.Errorf("a = %d (present=%v) after learning nothing, want 7", value, ok)
	}
	if n.Applied() != 1 {
		t.Errorf("applied watermark fell to %d", n.Applied())
	}
}

func TestPingReportsWhetherTheBallotIsStillCurrent(t *testing.T) {
	n := NewNode("s0n0", 0)
	first := Ballot{1, "s0n0"}
	n.Prepare(PrepareReq{Ballot: first})

	if !n.Ping(PingReq{Ballot: first}).Follows {
		t.Error("acceptor does not follow the ballot it just promised to")
	}

	second := Ballot{2, "s0n1"}
	n.Prepare(PrepareReq{Ballot: second})

	if n.Ping(PingReq{Ballot: first}).Follows {
		t.Error("acceptor still claims to follow a superseded ballot")
	}
	if !n.Ping(PingReq{Ballot: second}).Follows {
		t.Error("acceptor does not follow the newer ballot")
	}
}

// A prepared transfer holds its keys and an abort puts back exactly what the
// prepare took.
func TestAbortReversesExactlyWhatPrepareApplied(t *testing.T) {
	n := NewNode("s0n0", 0)
	ballot := Ballot{1, "s0n0"}
	n.Prepare(PrepareReq{Ballot: ballot})

	apply := func(slot uint64, cmd Command) {
		entry := Entry{Slot: slot, Ballot: ballot, Cmd: cmd}
		n.Accept(AcceptReq{Ballot: ballot, Entry: entry})
		n.Commit(CommitReq{Ballot: ballot, Entry: entry})
	}

	apply(1, Command{Type: CmdPut, Key: "alice", Value: 100})
	apply(2, Command{Type: CmdPrepare, TxID: "tx1", Key: "alice", Value: -30})

	if value, _ := n.Value("alice"); value != 70 {
		t.Errorf("alice = %d after prepare, want 70", value)
	}
	if tx, locked := n.Locked("alice"); !locked || tx != "tx1" {
		t.Errorf("alice locked by %q (locked=%v), want tx1", tx, locked)
	}

	apply(3, Command{Type: CmdAbort, TxID: "tx1"})

	if value, _ := n.Value("alice"); value != 100 {
		t.Errorf("alice = %d after abort, want 100", value)
	}
	if _, locked := n.Locked("alice"); locked {
		t.Error("alice is still locked after the abort")
	}
}

func TestCommitKeepsTheDeltaAndReleasesTheLock(t *testing.T) {
	n := NewNode("s0n0", 0)
	ballot := Ballot{1, "s0n0"}
	n.Prepare(PrepareReq{Ballot: ballot})

	apply := func(slot uint64, cmd Command) {
		entry := Entry{Slot: slot, Ballot: ballot, Cmd: cmd}
		n.Accept(AcceptReq{Ballot: ballot, Entry: entry})
		n.Commit(CommitReq{Ballot: ballot, Entry: entry})
	}

	apply(1, Command{Type: CmdPut, Key: "alice", Value: 100})
	apply(2, Command{Type: CmdPrepare, TxID: "tx1", Key: "alice", Value: -30})
	apply(3, Command{Type: CmdCommit, TxID: "tx1"})

	if value, _ := n.Value("alice"); value != 70 {
		t.Errorf("alice = %d after commit, want 70", value)
	}
	if _, locked := n.Locked("alice"); locked {
		t.Error("alice is still locked after the commit")
	}
}
