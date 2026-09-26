// Package cluster defines what the gateway is allowed to ask of a running
// cluster. The gateway codes against Engine; the real Multi-Paxos + 2PC
// implementation and the step-1 stub both satisfy it.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/tusharpanthri/distributed-banking-system/internal/paxos"
)

// Config describes the topology to build.
type Config struct {
	Shards         int // number of shards
	NodesPerShard  int // replicas in each shard
	MaxDistinctKey int // cap on distinct keys, 0 for unlimited
}

// Validate reports whether the topology is buildable.
func (c Config) Validate() error {
	if c.Shards < 1 {
		return errors.New("shards must be at least 1")
	}
	if c.NodesPerShard < 1 {
		return errors.New("nodes per shard must be at least 1")
	}
	return nil
}

// Quorum is the number of nodes that must agree in a shard: a strict majority.
//
// Not (n+1)/2, which is a majority only for odd n — at n=4 it returns 2 and two
// disjoint halves can each believe they hold a quorum. This is the same bug the
// v1 lab shipped with; see the root README.
func (c Config) Quorum() int { return c.NodesPerShard/2 + 1 }

// NodeID names a replica as s{shard}n{index}, both zero-based.
func NodeID(shard, index int) string { return fmt.Sprintf("s%dn%d", shard, index) }

// ShardTag is the log tag for a shard, e.g. "[s0]".
func ShardTag(shard int) string { return fmt.Sprintf("[s%d]", shard) }

// ShardFor maps a key to its shard. Callers and the frontend both need this to
// agree, so it is deliberately a plain deterministic hash with no salt.
func ShardFor(key string, shards int) int {
	if shards <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(shards))
}

// NodeStatus is one replica's liveness.
type NodeStatus struct {
	ID    string
	Alive bool
}

// ShardStatus summarises one shard. Leader is empty when the shard has no
// leader — because it lost quorum, or because an election is in flight.
type ShardStatus struct {
	Shard   int
	Leader  string
	Term    uint64
	Alive   int
	Total   int
	Quorum  int
	Nodes   []NodeStatus
	LogSize int
}

// HasQuorum reports whether enough replicas are alive to make progress. It does
// not imply a leader has been elected yet.
func (s ShardStatus) HasQuorum() bool { return s.Alive >= s.Quorum }

// AccountStatus is one key and where it lives. LockedBy names the transaction
// holding it, if any: a key left locked by an unresolved 2PC is exactly what an
// operator needs to see.
type AccountStatus struct {
	Key      string
	Shard    int
	Value    int64
	LockedBy string
}

// Status is the whole-cluster snapshot behind the `status` command.
type Status struct {
	Shards   []ShardStatus
	Accounts []AccountStatus
	// Partitions holds the current network split as groups of node ids, or nil
	// when the network is whole.
	Partitions [][]string
}

// Engine is the cluster as the gateway sees it. Every method is driven by one
// operator command.
//
// Implementations are used by a single session goroutine at a time, but must
// tolerate Close arriving while a command is in flight.
type Engine interface {
	Config() Config

	// Bootstrap elects an initial leader for every shard. A cluster that has
	// never run a round has no leader and cannot serve anything, so this is
	// what a real deployment does on startup rather than making the first
	// unlucky request pay for an election.
	Bootstrap(ctx context.Context)

	Status(ctx context.Context) (Status, error)
	// Datastore renders every replica's committed log, one line per replica, so
	// divergence between replicas is visible rather than merely asserted.
	Datastore() []string
	Put(ctx context.Context, key string, value int64) error
	Get(ctx context.Context, key string) (int64, error)
	Transfer(ctx context.Context, from, to string, amount int64) error

	Kill(ctx context.Context, node string) error
	Revive(ctx context.Context, node string) error
	Partition(ctx context.Context, groups [][]string) error
	Heal(ctx context.Context) error

	Close() error
}

// Errors the gateway renders as ABORT results rather than internal failures.
// These are expected outcomes of a demo, not bugs.
var (
	// The consensus layer already raises these, with a message that explains
	// the specific case. Sharing the sentinel rather than wrapping it lets
	// errors.Is work through untouched and keeps the gateway from printing
	// "no quorum (1/3 alive, needs 2): no quorum".

	// ErrNoQuorum means the shard cannot make progress: too few live replicas.
	ErrNoQuorum = paxos.ErrNoQuorum
	// ErrNoLeader means the shard has live replicas but no elected leader yet.
	ErrNoLeader = paxos.ErrNoLeader
	// ErrLocked means a key is held by an in-flight transaction.
	ErrLocked = paxos.ErrLocked
	// ErrNoSuchKey means a read or transfer named a key that was never written.
	ErrNoSuchKey = errors.New("no such key")
	// ErrInsufficientFunds aborts a transfer whose source cannot cover it.
	ErrInsufficientFunds = errors.New("insufficient funds")
	// ErrUnknownNode names a node id that is not in the topology.
	ErrUnknownNode = errors.New("unknown node")
	// ErrTooManyKeys means the per-session key cap was hit.
	ErrTooManyKeys = errors.New("too many distinct keys")
)
