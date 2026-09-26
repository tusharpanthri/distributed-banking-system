package cluster

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/tusharpanthri/distributed-banking-system/internal/paxos"
)

// Randomised soak testing: thousands of operations interleaved with kills and
// partitions, then a check that the guarantees still hold.
//
// The individual tests in cluster_test.go each pin one property under one
// hand-built scenario. This does the opposite — it makes no attempt to be
// clever about what it does, and instead leans on the invariants being checkable
// after the fact. It is the only test here that can find a bug nobody thought
// to look for.
//
// Every run prints its seed. A failure is reproduced with:
//
//	go test ./internal/cluster -run TestChaos -chaos.seed=<n>

var (
	chaosSeed = flag.Int64("chaos.seed", 0, "seed for the chaos test; 0 picks one")
	chaosOps  = flag.Int("chaos.ops", 400, "operations per chaos run")
	chaosRuns = flag.Int("chaos.runs", 6, "independent chaos runs")
)

// model is what the cluster ought to hold, maintained alongside it. It is only
// updated when an operation reports success.
type model struct {
	balances map[string]int64
}

func (m *model) total() int64 {
	var sum int64
	for _, v := range m.balances {
		sum += v
	}
	return sum
}

func TestChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos soak is slow; skipped under -short")
	}

	for run := 0; run < *chaosRuns; run++ {
		seed := *chaosSeed
		if seed == 0 {
			seed = time.Now().UnixNano() + int64(run)*7919
		} else if run > 0 {
			// An explicit seed reproduces one run, not six.
			break
		}

		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runChaos(t, seed, *chaosOps)
		})
	}
}

func runChaos(t *testing.T, seed int64, ops int) {
	t.Helper()
	t.Logf("chaos seed %d, %d operations (reproduce with -chaos.seed=%d)", seed, ops, seed)

	rng := rand.New(rand.NewSource(seed))
	ctx := context.Background()

	const (
		shards = 3
		nodes  = 3
		keys   = 8
	)

	e, err := NewPaxos(Config{Shards: shards, NodesPerShard: nodes}, nil)
	if err != nil {
		t.Fatalf("NewPaxos: %v", err)
	}
	defer e.Close()
	e.Bootstrap(ctx)

	// Seed every account with a known balance, on a healthy cluster, so the
	// starting total is not in doubt.
	m := &model{balances: map[string]int64{}}
	names := make([]string, keys)
	for i := range names {
		names[i] = fmt.Sprintf("acct%d", i)
		if err := e.Put(ctx, names[i], 1000); err != nil {
			t.Fatalf("seeding %s: %v", names[i], err)
		}
		m.balances[names[i]] = 1000
	}
	startTotal := m.total()

	allNodes := []string{}
	for s := 0; s < shards; s++ {
		for n := 0; n < nodes; n++ {
			allNodes = append(allNodes, NodeID(s, n))
		}
	}

	transfersOK, transfersFailed := 0, 0

	// A transfer that reported failure may still commit afterwards: its round
	// can have been accepted by a minority, reported as failed because no
	// quorum could confirm it either way, and then adopted and chosen by a
	// later leader during recovery. No system can promise "failed means it did
	// not happen" without the caller consulting an outcome registry, so these
	// are recorded and the final state has to be explainable by them -- not
	// required to be unaffected by them.
	type indeterminate struct {
		from, to string
		amount   int64
	}
	var unresolved []indeterminate

	for i := 0; i < ops; i++ {
		switch pick(rng) {
		case opTransfer:
			from := names[rng.Intn(len(names))]
			to := names[rng.Intn(len(names))]
			if from == to {
				continue
			}
			amount := int64(rng.Intn(120) + 1)

			err := e.Transfer(ctx, from, to, amount)
			if err == nil {
				m.balances[from] -= amount
				m.balances[to] += amount
				transfersOK++
			} else {
				transfersFailed++
				assertExpected(t, err, "transfer")
				unresolved = append(unresolved, indeterminate{from, to, amount})
			}

		case opKill:
			node := allNodes[rng.Intn(len(allNodes))]
			_ = e.Kill(ctx, node)

		case opRevive:
			node := allNodes[rng.Intn(len(allNodes))]
			_ = e.Revive(ctx, node)

		case opPartition:
			// Split one shard's replicas at random. Partitioning the whole
			// cluster every time would mean almost nothing ever commits.
			shard := rng.Intn(shards)
			var a, b []string
			for n := 0; n < nodes; n++ {
				id := NodeID(shard, n)
				if rng.Intn(2) == 0 {
					a = append(a, id)
				} else {
					b = append(b, id)
				}
			}
			if len(a) == 0 || len(b) == 0 {
				continue
			}
			_ = e.Partition(ctx, [][]string{a, b})

		case opHeal:
			_ = e.Heal(ctx)
		}
	}

	// Put the cluster back together before checking anything. A partitioned
	// cluster cannot answer a read, and the invariants are about what the
	// system settles on, not what it can serve mid-failure.
	if err := e.Heal(ctx); err != nil {
		t.Fatalf("final heal: %v", err)
	}
	for _, node := range allNodes {
		_ = e.Revive(ctx, node)
	}
	e.Bootstrap(ctx)

	t.Logf("%d transfers committed, %d refused", transfersOK, transfersFailed)

	checkNoStuckLocks(t, e)
	checkReplicasConverged(t, e)
	checkTotalPreserved(t, ctx, e, names, startTotal)
	checkNoNegativeBalances(t, ctx, e, names)

	// What is deliberately NOT asserted: that a transfer reporting failure had
	// no effect. A round accepted by a minority, reported as failed because no
	// quorum could confirm it either way, can be adopted and chosen by a later
	// leader during recovery. No system can promise otherwise without the caller
	// consulting an outcome registry, so asserting it would be asserting
	// something stronger than the design claims.
	//
	// What is asserted instead is a sound interval: every account must lie
	// between the model minus everything indeterminate that could have left it,
	// and the model plus everything indeterminate that could have arrived.
	outflow := map[string]int64{}
	inflow := map[string]int64{}
	for _, u := range unresolved {
		outflow[u.from] += u.amount
		inflow[u.to] += u.amount
	}
	for _, key := range names {
		got, err := e.Get(ctx, key)
		if err != nil {
			t.Fatalf("reading %s after healing: %v", key, err)
		}
		lo := m.balances[key] - outflow[key]
		hi := m.balances[key] + inflow[key]
		if got < lo || got > hi {
			t.Errorf("%s = %d, outside [%d, %d]: the model says %d and no combination "+
				"of unconfirmed transfers reaches %d", key, got, lo, hi, m.balances[key], got)
		}
	}

	checkStateMatchesItsLog(t, e)
	t.Logf("%d of %d transfers had an outcome that could not be confirmed",
		len(unresolved), transfersOK+transfersFailed)
}

type op int

const (
	opTransfer op = iota
	opKill
	opRevive
	opPartition
	opHeal
)

// pick weights the operation mix. Transfers dominate: faults are the
// interesting part, but a cluster that spends all its time broken never gets
// far enough to expose anything.
func pick(rng *rand.Rand) op {
	switch n := rng.Intn(100); {
	case n < 60:
		return opTransfer
	case n < 72:
		return opKill
	case n < 84:
		return opRevive
	case n < 93:
		return opPartition
	default:
		return opHeal
	}
}

// assertExpected fails on an error that is not one of the refusals this system
// is allowed to produce. An unexpected error type means something broke in a
// way the design does not account for.
func assertExpected(t *testing.T, err error, what string) {
	t.Helper()
	switch {
	case errors.Is(err, ErrNoQuorum),
		errors.Is(err, ErrNoLeader),
		errors.Is(err, ErrLocked),
		errors.Is(err, ErrInsufficientFunds),
		errors.Is(err, ErrNoSuchKey):
		return
	}
	t.Errorf("%s failed with an unexpected error: %v", what, err)
}

// checkNoStuckLocks asserts no key is still held once everything is back up.
//
// A lock outliving its transaction is worse than a failed transfer: it poisons
// every later operation touching that key, and nothing in the system will ever
// release it.
func checkNoStuckLocks(t *testing.T, e *PaxosEngine) {
	t.Helper()
	for _, shard := range e.shards {
		for _, node := range shard.Nodes() {
			for key, tx := range node.Snapshot().Locks {
				t.Errorf("%s still holds %s locked by %s after the cluster healed",
					node.ID(), key, tx)
			}
		}
	}
}

// checkReplicasConverged asserts every replica in a shard holds the same
// committed entries, in the same order.
func checkReplicasConverged(t *testing.T, e *PaxosEngine) {
	t.Helper()
	for _, shard := range e.shards {
		var ref []paxos.Entry
		var refID string
		for _, node := range shard.Nodes() {
			snap := node.Snapshot()
			if ref == nil {
				ref, refID = snap.Committed, node.ID()
				continue
			}
			n := len(snap.Committed)
			if n > len(ref) {
				n = len(ref)
			}
			// A replica may legitimately be short if it has not caught up yet,
			// but wherever both hold a slot they must agree on it. Two replicas
			// disagreeing at the same slot is a safety violation.
			for i := 0; i < n; i++ {
				if snap.Committed[i].Slot != ref[i].Slot || snap.Committed[i].Cmd != ref[i].Cmd {
					t.Errorf("s%d divergence at index %d: %s has {%d %v}, %s has {%d %v}",
						shard.ID(), i,
						node.ID(), snap.Committed[i].Slot, snap.Committed[i].Cmd,
						refID, ref[i].Slot, ref[i].Cmd)
					break
				}
			}
		}
	}
}

// checkNoNegativeBalances asserts the funds check was never bypassed. A
// negative balance means a debit went through that nothing authorised.
func checkNoNegativeBalances(t *testing.T, ctx context.Context, e *PaxosEngine, names []string) {
	t.Helper()
	for _, key := range names {
		v, err := e.Get(ctx, key)
		if err != nil {
			t.Errorf("reading %s: %v", key, err)
			continue
		}
		if v < 0 {
			t.Errorf("%s = %d: a debit went through without sufficient funds", key, v)
		}
	}
}

// checkTotalPreserved is the money-conservation check, stated independently of
// the model so that a bug in the model cannot hide a bug in the cluster.
func checkTotalPreserved(t *testing.T, ctx context.Context, e *PaxosEngine, names []string, want int64) {
	t.Helper()
	var sum int64
	for _, key := range names {
		v, err := e.Get(ctx, key)
		if err != nil {
			t.Errorf("reading %s: %v", key, err)
			return
		}
		sum += v
	}
	if sum != want {
		t.Errorf("balances total %d, started at %d: %d was created or destroyed",
			sum, want, sum-want)
	}
}

// checkStateMatchesItsLog replays each shard's committed log into a fresh
// replica and asserts it lands on the same state the live one holds.
//
// A replica's state is supposed to be a pure function of its log. It stops being
// one the moment an entry is applied and then something else is committed at
// that slot -- which is precisely the failure that put two different values at
// slot 115 during development. Replaying is the cheapest way to keep catching
// that: the live state and a from-scratch replay of the same entries can only
// disagree if something reached the state machine that the log does not account
// for.
func checkStateMatchesItsLog(t *testing.T, e *PaxosEngine) {
	t.Helper()

	for _, shard := range e.shards {
		leaderID, _ := shard.Leader()
		if leaderID == "" {
			continue
		}
		live := shard.Node(leaderID)
		snap := live.Snapshot()

		replay := paxos.NewNode("replay", shard.ID())
		replay.Learn(paxos.LearnReq{Entries: snap.Committed})

		for key, want := range snap.State {
			got, _ := replay.Value(key)
			if got != want {
				t.Errorf("s%d: %s holds %s=%d, but replaying its own committed log gives %d",
					shard.ID(), leaderID, key, want, got)
			}
		}
	}
}
