package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tusharpanthri/distributed-banking-system/internal/logstream"
)

// callLatency is the delay applied to every delivered message. It is small and
// real: without it every round completes in the same instant and the log reads
// as one dump. It is not a stand-in for network latency and nothing depends on
// its value for correctness.
const callLatency = 2 * time.Millisecond

// InProc is a Transport whose nodes are goroutines in this process. It is the
// default and the one that gets deployed: a whole cluster in one container, so
// the demo fits on a free tier with no orchestration.
//
// Calls run on the caller's goroutine into the callee's handler, so a handler
// must not block: the consensus layer holds no locks across a Call for exactly
// this reason.
type InProc struct {
	faults *Faults
	log    *logstream.Emitter

	mu       sync.RWMutex
	handlers map[string]Handler
	closed   bool
}

// NewInProc returns an in-process transport. sink may be nil.
func NewInProc(sink logstream.Sink) *InProc {
	return &InProc{
		faults:   NewFaults(),
		log:      logstream.NewEmitter(sink, "[net]"),
		handlers: map[string]Handler{},
	}
}

func (t *InProc) Faults() *Faults { return t.faults }

func (t *InProc) Register(node string, h Handler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[node] = h
}

func (t *InProc) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	t.handlers = map[string]Handler{}
	return nil
}

func (t *InProc) Call(ctx context.Context, from, to string, method Method, req any) (any, error) {
	t.mu.RLock()
	closed := t.closed
	handler, ok := t.handlers[to]
	t.mu.RUnlock()

	if closed {
		return nil, ErrClosed
	}

	// The fault check happens before the handler is ever reached, so a dropped
	// message is indistinguishable from a message that was never answered.
	// Consensus sees a failed call and nothing more.
	if !t.faults.Reachable(from, to) {
		t.logDrop(from, to, method)
		return nil, fmt.Errorf("%s -> %s: %w", from, to, ErrUnreachable)
	}
	if !ok {
		return nil, fmt.Errorf("%s: %w", to, ErrNoHandler)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(callLatency):
	}

	reply, err := handler(ctx, method, req)
	if err != nil {
		return nil, err
	}

	// The reply travels back over the same link, so a partition that forms
	// mid-round loses it exactly as it would lose the request.
	if !t.faults.Reachable(to, from) {
		t.logDrop(to, from, method)
		return nil, fmt.Errorf("%s -> %s reply: %w", to, from, ErrUnreachable)
	}
	return reply, nil
}

// logDrop reports a dropped message, but only when a partition caused it. A
// dead node drops every message aimed at it, and narrating each one would bury
// the round that matters under noise the operator already knows about.
func (t *InProc) logDrop(from, to string, method Method) {
	if !t.faults.Alive(from) || !t.faults.Alive(to) {
		return
	}
	t.log.Net(fmt.Sprintf("dropped %s %s -> %s (partitioned)", method, from, to))
}
