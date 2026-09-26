package transport

import (
	"context"
	"errors"
	"testing"
)

func TestKilledNodeIsUnreachableBothWays(t *testing.T) {
	f := NewFaults()
	f.Kill("s0n1")

	if f.Reachable("s0n0", "s0n1") {
		t.Error("a message reached a dead node")
	}
	if f.Reachable("s0n1", "s0n0") {
		t.Error("a dead node sent a message")
	}
	if f.Reachable("s0n0", "s0n2") != true {
		t.Error("killing one node cut an unrelated link")
	}
}

func TestReviveRestoresReachability(t *testing.T) {
	f := NewFaults()

	if !f.Kill("s0n1") {
		t.Error("Kill reported no change on a live node")
	}
	if f.Kill("s0n1") {
		t.Error("Kill reported a change on an already-dead node")
	}
	if !f.Revive("s0n1") {
		t.Error("Revive reported no change on a dead node")
	}
	if f.Revive("s0n1") {
		t.Error("Revive reported a change on an already-live node")
	}
	if !f.Reachable("s0n0", "s0n1") {
		t.Error("a revived node is still unreachable")
	}
}

func TestPartitionBlocksAcrossGroupsOnly(t *testing.T) {
	f := NewFaults()
	f.Partition([][]string{{"s0n0", "s0n1"}, {"s0n2"}})

	if !f.Reachable("s0n0", "s0n1") {
		t.Error("nodes in the same group cannot reach each other")
	}
	if f.Reachable("s0n0", "s0n2") {
		t.Error("a message crossed a partition")
	}
	if f.Reachable("s0n2", "s0n0") {
		t.Error("a message crossed a partition in the other direction")
	}
	if !f.Reachable("s0n2", "s0n2") {
		t.Error("a node cannot reach itself")
	}
}

// A node left out of a partition must not quietly bridge the named groups.
// Treating it as reachable-from-everywhere would make "partition a | b" on a
// three-node shard a no-op, since c would still connect the two sides.
func TestUnnamedNodesCannotBridgeNamedGroups(t *testing.T) {
	f := NewFaults()
	f.Partition([][]string{{"s0n0"}, {"s0n1"}})

	if f.Reachable("s0n2", "s0n0") {
		t.Error("an unnamed node bridged into a named group")
	}
	if f.Reachable("s0n0", "s0n2") {
		t.Error("a named group reached an unnamed node")
	}
	if !f.Reachable("s0n2", "s0n2") {
		t.Error("an unnamed node cannot reach itself")
	}
}

// Unnamed nodes stay connected to each other. Partitioning one shard of a
// multi-shard cluster must not silently shred every shard nobody mentioned.
func TestUnnamedNodesRemainConnectedToEachOther(t *testing.T) {
	f := NewFaults()
	// Split shard 0 only.
	f.Partition([][]string{{"s0n0", "s0n1"}, {"s0n2"}})

	for _, pair := range [][2]string{
		{"s1n0", "s1n1"},
		{"s1n1", "s1n2"},
		{"s2n0", "s2n2"},
		{"s1n0", "s2n0"},
	} {
		if !f.Reachable(pair[0], pair[1]) {
			t.Errorf("%s cannot reach %s: partitioning shard 0 broke an untouched shard", pair[0], pair[1])
		}
	}

	// And shard 0 really is split.
	if f.Reachable("s0n0", "s0n2") {
		t.Error("the named split did not take effect")
	}
	if !f.Reachable("s0n0", "s0n1") {
		t.Error("nodes inside a named group cannot reach each other")
	}
}

func TestHealRestoresEverything(t *testing.T) {
	f := NewFaults()
	f.Partition([][]string{{"s0n0"}, {"s0n1", "s0n2"}})

	if !f.Partitioned() {
		t.Error("Partitioned reported a whole network")
	}
	if !f.Heal() {
		t.Error("Heal reported no change on a partitioned network")
	}
	if f.Heal() {
		t.Error("Heal reported a change on a whole network")
	}
	if !f.Reachable("s0n0", "s0n1") {
		t.Error("healing did not restore the link")
	}
}

// Healing a partition does not resurrect the dead: the two failure modes are
// independent.
func TestHealDoesNotReviveDeadNodes(t *testing.T) {
	f := NewFaults()
	f.Kill("s0n1")
	f.Partition([][]string{{"s0n0"}, {"s0n1", "s0n2"}})
	f.Heal()

	if f.Reachable("s0n0", "s0n1") {
		t.Error("heal made a dead node reachable")
	}
	if !f.Reachable("s0n0", "s0n2") {
		t.Error("heal did not restore the link between live nodes")
	}
}

func TestGroupsIsACopy(t *testing.T) {
	f := NewFaults()
	f.Partition([][]string{{"s0n0"}, {"s0n1"}})

	groups := f.Groups()
	groups[0][0] = "tampered"

	if f.Reachable("s0n0", "s0n1") {
		t.Error("mutating the returned groups changed the live partition")
	}
}

func TestInProcDeliversAndRespectsFaults(t *testing.T) {
	ctx := context.Background()
	tp := NewInProc(nil)
	defer tp.Close()

	tp.Register("a", func(_ context.Context, m Method, req any) (any, error) {
		return "pong:" + string(m), nil
	})
	tp.Register("b", func(_ context.Context, m Method, req any) (any, error) {
		return "pong:" + string(m), nil
	})

	reply, err := tp.Call(ctx, "b", "a", MethodPing, nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if reply != "pong:Ping" {
		t.Errorf("reply = %v", reply)
	}

	tp.Faults().Kill("a")
	if _, err := tp.Call(ctx, "b", "a", MethodPing, nil); !errors.Is(err, ErrUnreachable) {
		t.Errorf("call to a dead node = %v, want unreachable", err)
	}

	tp.Faults().Revive("a")
	tp.Faults().Partition([][]string{{"a"}, {"b"}})
	if _, err := tp.Call(ctx, "b", "a", MethodPing, nil); !errors.Is(err, ErrUnreachable) {
		t.Errorf("call across a partition = %v, want unreachable", err)
	}
}

func TestInProcRejectsUnknownNodeAndClosedTransport(t *testing.T) {
	ctx := context.Background()
	tp := NewInProc(nil)

	if _, err := tp.Call(ctx, "a", "nobody", MethodPing, nil); !errors.Is(err, ErrNoHandler) {
		t.Errorf("call to an unregistered node = %v, want no handler", err)
	}

	tp.Close()
	if _, err := tp.Call(ctx, "a", "b", MethodPing, nil); !errors.Is(err, ErrClosed) {
		t.Errorf("call on a closed transport = %v, want closed", err)
	}
}
