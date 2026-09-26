package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/tusharpanthri/distributed-banking-system/internal/paxos"
)

// These are the guarantees the v1 lab's suite pinned down, re-pointed at the
// Multi-Paxos engine, plus the ones v1 could not express because it had no
// leader election and no way to partition a network.

func newCluster(t *testing.T, shards, nodes int) *PaxosEngine {
	t.Helper()

	e, err := NewPaxos(Config{Shards: shards, NodesPerShard: nodes}, nil)
	if err != nil {
		t.Fatalf("NewPaxos: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	e.Bootstrap(context.Background())
	return e
}

// keysOnDistinctShards finds two keys that hash to different shards, so a test
// does not have to hard-code the output of the hash.
func keysOnDistinctShards(t *testing.T, shards int) (string, string) {
	t.Helper()
	first := "k0"
	for i := 1; i < 500; i++ {
		candidate := fmt.Sprintf("k%d", i)
		if ShardFor(candidate, shards) != ShardFor(first, shards) {
			return first, candidate
		}
	}
	t.Fatal("could not find two keys on different shards")
	return "", ""
}

// keysOnSameShard finds two keys that hash to the same shard.
func keysOnSameShard(t *testing.T, shards int) (string, string) {
	t.Helper()
	first := "k0"
	for i := 1; i < 500; i++ {
		candidate := fmt.Sprintf("k%d", i)
		if ShardFor(candidate, shards) == ShardFor(first, shards) {
			return first, candidate
		}
	}
	t.Fatal("could not find two keys on the same shard")
	return "", ""
}

func mustPut(t *testing.T, e *PaxosEngine, key string, value int64) {
	t.Helper()
	if err := e.Put(context.Background(), key, value); err != nil {
		t.Fatalf("put %s=%d: %v", key, value, err)
	}
}

func TestBootstrapElectsALeaderPerShard(t *testing.T) {
	e := newCluster(t, 3, 3)

	st, err := e.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	for _, shard := range st.Shards {
		if shard.Leader == "" {
			t.Errorf("shard s%d has no leader after bootstrap", shard.Shard)
		}
	}
}

func TestCommitWithFullClusterReplicatesToAll(t *testing.T) {
	e := newCluster(t, 1, 3)
	mustPut(t, e, "alice", 100)

	for _, node := range e.shards[0].Nodes() {
		value, ok := node.Value("alice")
		if !ok {
			t.Errorf("%s never applied the write", node.ID())
			continue
		}
		if value != 100 {
			t.Errorf("%s has alice=%d, want 100", node.ID(), value)
		}
	}
}

func TestCommitProceedsWithMinorityDown(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 1, 3)

	if err := e.Kill(ctx, "s0n2"); err != nil {
		t.Fatalf("kill: %v", err)
	}
	if err := e.Put(ctx, "alice", 42); err != nil {
		t.Fatalf("a 2-of-3 majority should still commit, got %v", err)
	}

	value, err := e.Get(ctx, "alice")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value != 42 {
		t.Errorf("alice = %d, want 42", value)
	}
}

// The survivor must refuse rather than commit alone. This is the guarantee the
// v1 lab violated by counting the CSV's list of active servers instead of
// counting replies.
func TestRefusesToCommitWithoutQuorum(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 1, 3)
	mustPut(t, e, "alice", 100)

	if err := e.Kill(ctx, "s0n1"); err != nil {
		t.Fatalf("kill: %v", err)
	}
	if err := e.Kill(ctx, "s0n2"); err != nil {
		t.Fatalf("kill: %v", err)
	}

	err := e.Put(ctx, "alice", 999)
	if err == nil {
		t.Fatal("a lone survivor committed without a quorum")
	}
	if !errors.Is(err, ErrNoQuorum) && !errors.Is(err, ErrNoLeader) {
		t.Errorf("error = %v, want no quorum or no leader", err)
	}

	// The value must be untouched on the replica that is still up.
	if value, ok := e.shards[0].Node("s0n0").Value("alice"); ok && value != 100 {
		t.Errorf("alice = %d on the survivor, want it unchanged at 100", value)
	}
}

func TestKilledLeaderIsReplaced(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 1, 3)

	before, _ := e.shards[0].Leader()
	if before == "" {
		t.Fatal("no leader after bootstrap")
	}
	if err := e.Kill(ctx, before); err != nil {
		t.Fatalf("kill: %v", err)
	}

	after, _ := e.shards[0].Leader()
	if after == "" {
		t.Fatal("shard did not re-elect after its leader was killed")
	}
	if after == before {
		t.Errorf("leader is still %s after being killed", before)
	}
	if err := e.Put(ctx, "alice", 1); err != nil {
		t.Errorf("the new leader should accept writes, got %v", err)
	}
}

func TestNoMoneyCreatedOrDestroyed(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 3, 3)

	keys := []string{}
	var total int64
	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("acct%d", i)
		keys = append(keys, key)
		mustPut(t, e, key, 100)
		total += 100
	}

	// Transfers in every direction, including ones that must abort.
	for i := 0; i < len(keys); i++ {
		for j := 0; j < len(keys); j++ {
			if i == j {
				continue
			}
			_ = e.Transfer(ctx, keys[i], keys[j], int64(7*(i+1)))
		}
	}

	var sum int64
	for _, key := range keys {
		value, err := e.Get(ctx, key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		sum += value
	}
	if sum != total {
		t.Errorf("total is %d after the transfers, want %d: money was created or destroyed", sum, total)
	}
}

func TestReplicasConvergeOnIdenticalLogs(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 2, 3)

	a, b := keysOnDistinctShards(t, 2)
	mustPut(t, e, a, 100)
	mustPut(t, e, b, 100)
	if err := e.Transfer(ctx, a, b, 30); err != nil {
		t.Fatalf("transfer: %v", err)
	}

	for _, shard := range e.shards {
		var reference []paxos.Entry
		var referenceID string
		for _, node := range shard.Nodes() {
			snap := node.Snapshot()
			if reference == nil {
				reference, referenceID = snap.Committed, node.ID()
				continue
			}
			if len(snap.Committed) != len(reference) {
				t.Errorf("s%d: %s has %d committed entries, %s has %d",
					shard.ID(), node.ID(), len(snap.Committed), referenceID, len(reference))
				continue
			}
			for i := range reference {
				if snap.Committed[i].Slot != reference[i].Slot ||
					snap.Committed[i].Cmd != reference[i].Cmd {
					t.Errorf("s%d slot %d: %s has %v, %s has %v",
						shard.ID(), i, node.ID(), snap.Committed[i].Cmd,
						referenceID, reference[i].Cmd)
				}
			}
		}
	}
}

// A replica that missed writes while dead must not stay behind forever.
func TestRecoveredReplicaCatchesUp(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 1, 3)

	mustPut(t, e, "alice", 1)
	if err := e.Kill(ctx, "s0n2"); err != nil {
		t.Fatalf("kill: %v", err)
	}

	// Writes it will miss entirely.
	mustPut(t, e, "alice", 2)
	mustPut(t, e, "alice", 3)

	behind := e.shards[0].Node("s0n2")
	if value, _ := behind.Value("alice"); value != 1 {
		t.Fatalf("the dead replica somehow saw a write: alice=%d", value)
	}

	if err := e.Revive(ctx, "s0n2"); err != nil {
		t.Fatalf("revive: %v", err)
	}
	// One more round, which the revived replica now participates in.
	mustPut(t, e, "alice", 4)

	if value, ok := behind.Value("alice"); !ok || value != 4 {
		t.Errorf("recovered replica has alice=%d (present=%v), want 4", value, ok)
	}
}

func TestCrossShardTransferCommitsOnBothShards(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 3, 3)

	from, to := keysOnDistinctShards(t, 3)
	mustPut(t, e, from, 100)
	mustPut(t, e, to, 10)

	if err := e.Transfer(ctx, from, to, 25); err != nil {
		t.Fatalf("transfer: %v", err)
	}

	if value, _ := e.Get(ctx, from); value != 75 {
		t.Errorf("%s = %d, want 75", from, value)
	}
	if value, _ := e.Get(ctx, to); value != 35 {
		t.Errorf("%s = %d, want 35", to, value)
	}
}

// An aborted cross-shard transfer must leave no money moved and no key locked.
// A stuck lock is worse than a failed transfer: it poisons every later command
// touching that key.
func TestCrossShardAbortLeavesNothingBehind(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 3, 3)

	from, to := keysOnDistinctShards(t, 3)
	mustPut(t, e, from, 10)
	mustPut(t, e, to, 10)

	err := e.Transfer(ctx, from, to, 500)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("transfer error = %v, want insufficient funds", err)
	}

	if value, _ := e.Get(ctx, from); value != 10 {
		t.Errorf("%s = %d, want it unchanged at 10", from, value)
	}
	if value, _ := e.Get(ctx, to); value != 10 {
		t.Errorf("%s = %d, want it unchanged at 10", to, value)
	}

	for _, shard := range e.shards {
		for _, node := range shard.Nodes() {
			for _, key := range []string{from, to} {
				if tx, locked := node.Locked(key); locked {
					t.Errorf("%s still holds %s locked by %s after the abort", node.ID(), key, tx)
				}
			}
		}
	}

	// And the accounts must still be usable.
	if err := e.Transfer(ctx, from, to, 5); err != nil {
		t.Errorf("a later transfer should work after an abort, got %v", err)
	}
}

func TestSameShardTransferUsesOneRound(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 3, 3)

	from, to := keysOnSameShard(t, 3)
	mustPut(t, e, from, 100)
	mustPut(t, e, to, 0)

	if err := e.Transfer(ctx, from, to, 40); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if value, _ := e.Get(ctx, from); value != 60 {
		t.Errorf("%s = %d, want 60", from, value)
	}
	if value, _ := e.Get(ctx, to); value != 40 {
		t.Errorf("%s = %d, want 40", to, value)
	}
}

// A minority partition cannot write; the majority side still can.
func TestPartitionLeavesTheMajoritySideWritable(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 1, 3)
	mustPut(t, e, "alice", 100)

	if err := e.Partition(ctx, [][]string{{"s0n0"}, {"s0n1", "s0n2"}}); err != nil {
		t.Fatalf("partition: %v", err)
	}

	if err := e.Put(ctx, "alice", 200); err != nil {
		t.Fatalf("the 2-node side holds a quorum and should commit, got %v", err)
	}

	leader, _ := e.shards[0].Leader()
	if leader == "s0n0" {
		t.Errorf("leader is %s, which is on the minority side of the partition", leader)
	}
}

// Split every replica apart and no group holds a majority, so the shard is
// leaderless even though every node is alive. Liveness is not the same as
// reachability, and this is the case a dead-node count cannot express.
func TestFullyPartitionedShardIsLeaderless(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 1, 3)
	mustPut(t, e, "alice", 100)

	if err := e.Partition(ctx, [][]string{{"s0n0"}, {"s0n1"}, {"s0n2"}}); err != nil {
		t.Fatalf("partition: %v", err)
	}

	if leader, _ := e.shards[0].Leader(); leader != "" {
		t.Errorf("shard has leader %s despite no group holding a majority", leader)
	}
	if err := e.Put(ctx, "alice", 200); err == nil {
		t.Error("a fully partitioned shard accepted a write")
	}

	// A read must fail too: answering from a replica that cannot confirm its
	// leadership is exactly the stale read linearizability forbids.
	if _, err := e.Get(ctx, "alice"); err == nil {
		t.Error("a fully partitioned shard served a read")
	}

	if err := e.Heal(ctx); err != nil {
		t.Fatalf("heal: %v", err)
	}
	if err := e.Put(ctx, "alice", 300); err != nil {
		t.Errorf("after heal the shard should accept writes, got %v", err)
	}
	if value, _ := e.Get(ctx, "alice"); value != 300 {
		t.Errorf("alice = %d after heal, want 300", value)
	}
}

// A transfer whose destination shard has no quorum must abort, and must not
// leave the source shard debited.
func TestCrossShardAbortsWhenOneShardLosesQuorum(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 3, 3)

	from, to := keysOnDistinctShards(t, 3)
	mustPut(t, e, from, 100)
	mustPut(t, e, to, 10)

	dst := ShardFor(to, 3)
	if err := e.Kill(ctx, NodeID(dst, 0)); err != nil {
		t.Fatalf("kill: %v", err)
	}
	if err := e.Kill(ctx, NodeID(dst, 1)); err != nil {
		t.Fatalf("kill: %v", err)
	}

	if err := e.Transfer(ctx, from, to, 25); err == nil {
		t.Fatal("transfer succeeded with the destination shard below quorum")
	}

	if value, _ := e.Get(ctx, from); value != 100 {
		t.Errorf("%s = %d, want it unchanged at 100: the source was debited for an aborted transfer", from, value)
	}
	for _, node := range e.shards[ShardFor(from, 3)].Nodes() {
		if tx, locked := node.Locked(from); locked {
			t.Errorf("%s left %s locked by %s", node.ID(), from, tx)
		}
	}
}

func TestUnknownNodeIsRejected(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 2, 3)

	if err := e.Kill(ctx, "s9n9"); !errors.Is(err, ErrUnknownNode) {
		t.Errorf("kill s9n9 = %v, want unknown node", err)
	}
	if err := e.Revive(ctx, "nonsense"); !errors.Is(err, ErrUnknownNode) {
		t.Errorf("revive nonsense = %v, want unknown node", err)
	}
	if err := e.Partition(ctx, [][]string{{"s0n0"}, {"s9n9"}}); !errors.Is(err, ErrUnknownNode) {
		t.Errorf("partition with an unknown node = %v, want unknown node", err)
	}
}

func TestGetRejectsUnknownKey(t *testing.T) {
	e := newCluster(t, 2, 3)
	if _, err := e.Get(context.Background(), "nope"); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("get of an unwritten key = %v, want no such key", err)
	}
}

func TestTransferRejectsUnknownDestination(t *testing.T) {
	ctx := context.Background()
	e := newCluster(t, 3, 3)
	mustPut(t, e, "alice", 100)

	if err := e.Transfer(ctx, "alice", "typo", 10); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("transfer to an unwritten key = %v, want no such key", err)
	}
	if value, _ := e.Get(ctx, "alice"); value != 100 {
		t.Errorf("alice = %d, want it unchanged at 100", value)
	}
}

func TestQuorumIsAStrictMajority(t *testing.T) {
	// (n+1)/2 is a majority only for odd n. At n=4 it gives 2, which lets two
	// disjoint halves each believe they hold a quorum.
	for n := 1; n <= 9; n++ {
		cfg := Config{Shards: 1, NodesPerShard: n}
		q := cfg.Quorum()
		if 2*q <= n {
			t.Errorf("quorum for %d nodes is %d, which is not a strict majority", n, q)
		}
	}
}

func TestShardForCoversEveryShard(t *testing.T) {
	for shards := 1; shards <= 8; shards++ {
		seen := map[int]bool{}
		for i := 0; i < 2000; i++ {
			s := ShardFor(fmt.Sprintf("key%d", i), shards)
			if s < 0 || s >= shards {
				t.Fatalf("ShardFor returned %d for a %d-shard cluster", s, shards)
			}
			seen[s] = true
		}
		if len(seen) != shards {
			t.Errorf("%d shards configured but only %d ever used", shards, len(seen))
		}
	}
}
