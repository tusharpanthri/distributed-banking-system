package transport

import (
	"sort"
	"strings"
	"sync"
)

// Faults decides which messages are allowed through. It is the whole of the
// failure model: a node is either dead or alive, and the live nodes are split
// into reachability groups.
//
// Nothing here knows what a leader is. A leader that ends up on the wrong side
// of a partition simply stops being able to reach a quorum, and the consensus
// layer draws its own conclusion — which is how the real thing behaves.
type Faults struct {
	mu     sync.RWMutex
	dead   map[string]bool
	groups [][]string
}

// NewFaults returns a fault injector with every node alive and the network
// whole.
func NewFaults() *Faults {
	return &Faults{dead: map[string]bool{}}
}

// Kill marks a node dead. It reports whether the state changed.
func (f *Faults) Kill(node string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dead[node] {
		return false
	}
	f.dead[node] = true
	return true
}

// Revive brings a node back. It reports whether the state changed.
//
// The node keeps whatever log it had when it died: killing a node stops its
// messages, it does not wipe its state. Recovery then has to reconcile a
// replica that is genuinely behind, rather than one that conveniently forgot
// everything.
func (f *Faults) Revive(node string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dead[node] {
		return false
	}
	delete(f.dead, node)
	return true
}

// Alive reports whether a node is running.
func (f *Faults) Alive(node string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return !f.dead[node]
}

// Dead lists the dead nodes, sorted.
func (f *Faults) Dead() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]string, 0, len(f.dead))
	for id := range f.dead {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Partition splits the network into groups. Messages cross only within a group.
//
// Nodes named in no group form one implicit group of their own: they can reach
// each other but not any named group. Both alternatives are worse. Treating
// them as reachable from everywhere makes "partition a | b" on a three-node
// shard a no-op, because c would still bridge the two sides. Treating each as
// its own island means partitioning one shard of a nine-node cluster silently
// shreds the other two, which is not what anybody typing that command meant.
//
// With one implicit group both cases come out right: "partition a | b" leaves c
// unable to reach either side, so that shard has no majority anywhere, while
// the shards nobody named keep talking among themselves.
func (f *Faults) Partition(groups [][]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	clone := make([][]string, len(groups))
	for i, g := range groups {
		clone[i] = append([]string(nil), g...)
	}
	f.groups = clone
}

// Heal removes every partition. It reports whether anything changed.
func (f *Faults) Heal() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.groups) == 0 {
		return false
	}
	f.groups = nil
	return true
}

// Partitioned reports whether any partition is in force.
func (f *Faults) Partitioned() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.groups) > 0
}

// Groups returns the current partition, or nil when the network is whole.
func (f *Faults) Groups() [][]string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.groups) == 0 {
		return nil
	}
	out := make([][]string, len(f.groups))
	for i, g := range f.groups {
		out[i] = append([]string(nil), g...)
	}
	return out
}

// groupOf returns the index of the group holding node, or -1 when the network
// is whole. Every unnamed node shares one index past the end of the named
// groups: they are together, and apart from everyone named.
func (f *Faults) groupOf(node string) int {
	if len(f.groups) == 0 {
		return -1
	}
	for i, group := range f.groups {
		for _, member := range group {
			if member == node {
				return i
			}
		}
	}
	return len(f.groups)
}

// Reachable reports whether a message from one node to another is delivered.
func (f *Faults) Reachable(from, to string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.dead[from] || f.dead[to] {
		return false
	}
	if from == to {
		return true
	}
	if len(f.groups) == 0 {
		return true
	}
	return f.groupOf(from) == f.groupOf(to)
}

// Describe renders the current partition for display, e.g. "{a b} | {c}".
func (f *Faults) Describe() string {
	groups := f.Groups()
	if len(groups) == 0 {
		return ""
	}
	parts := make([]string, len(groups))
	for i, g := range groups {
		parts[i] = "{" + strings.Join(g, " ") + "}"
	}
	return strings.Join(parts, " | ")
}
