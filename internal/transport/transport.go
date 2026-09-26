// Package transport carries messages between nodes and is the single place
// where failure is injected.
//
// Putting kill, partition and heal here rather than inside the consensus code
// is the design decision the whole project rests on. Consensus never learns
// that a node was "killed" — it sees a call that does not come back, which is
// all a real replica ever sees. That means the same fault injection works
// unchanged against an in-process transport and a networked one, and it means
// the demo can run as a single container on a free tier while still exercising
// real failure paths.
package transport

import (
	"context"
	"errors"
)

// Method names one request kind. Kept as a string rather than an enum so that a
// future gRPC transport can map it straight onto a service method.
type Method string

const (
	// MethodPrepare is the Paxos prepare, which doubles as the election.
	MethodPrepare Method = "Prepare"
	// MethodAccept asks an acceptor to accept a value at a slot.
	MethodAccept Method = "Accept"
	// MethodCommit tells a replica a slot was chosen.
	MethodCommit Method = "Commit"
	// MethodPing asks an acceptor whether it still follows the caller's ballot.
	// A leader uses it to confirm leadership before serving a read.
	MethodPing Method = "Ping"
	// MethodLearn backfills a replica that fell behind while it was dead or
	// partitioned away.
	MethodLearn Method = "Learn"
)

// Handler processes an inbound request on one node.
type Handler func(ctx context.Context, method Method, req any) (any, error)

// Transport delivers a request to a node and brings back its reply.
//
// Request and reply travel as `any`. That is deliberate for the in-process
// implementation, where the values are passed directly. A gRPC implementation
// would add a codec at this boundary; nothing above the interface changes,
// because callers already treat every call as fallible and bounded by ctx.
type Transport interface {
	// Register attaches a handler for a node. Calling it twice for the same
	// node replaces the handler.
	Register(node string, h Handler)

	// Call sends a request from one node to another and waits for the reply.
	// It returns ErrUnreachable when the fault layer refuses delivery.
	Call(ctx context.Context, from, to string, method Method, req any) (any, error)

	// Faults exposes the fault injector driving this transport.
	Faults() *Faults

	// Close releases resources. Calls after Close return ErrClosed.
	Close() error
}

var (
	// ErrUnreachable means the fault layer refused delivery: the sender is
	// dead, the receiver is dead, or a partition separates them. Consensus
	// treats it exactly like a timeout, which is the point.
	ErrUnreachable = errors.New("unreachable")

	// ErrNoHandler means the node id is not registered.
	ErrNoHandler = errors.New("no handler registered")

	// ErrClosed means the transport has been shut down.
	ErrClosed = errors.New("transport closed")
)
