package cluster

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/tusharpanthri/distributed-banking-system/internal/logstream"
	"github.com/tusharpanthri/distributed-banking-system/internal/paxos"
	"github.com/tusharpanthri/distributed-banking-system/internal/transport"
)

// PaxosEngine is the real cluster: one Multi-Paxos shard per key range, with a
// two-phase commit coordinator for transfers that cross shards.
//
// Commands are serialised by a single mutex. Each one may run several rounds
// across several shards, and interleaving two of them would let a transfer
// observe a half-applied prepare from another. The operator types one command
// at a time, so there is nothing to gain from concurrency here and a good deal
// of reasoning to lose.
type PaxosEngine struct {
	cfg    Config
	tp     transport.Transport
	shards []*paxos.Shard
	log    *logstream.Emitter

	mu      sync.Mutex
	nextTx  uint64
	nextCmd uint64
	keys    map[string]bool
	closed  bool
	// pending holds 2PC decisions that have been made but not yet recorded on
	// every shard involved. See decide.
	pending []*pendingDecision
}

// NewPaxos builds a cluster of cfg.Shards shards over an in-process transport.
func NewPaxos(cfg Config, sink logstream.Sink) (*PaxosEngine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if sink == nil {
		sink = logstream.Discard
	}

	tp := transport.NewInProc(sink)
	e := &PaxosEngine{
		cfg:  cfg,
		tp:   tp,
		log:  logstream.NewEmitter(sink, "[2PC]"),
		keys: map[string]bool{},
	}
	for i := 0; i < cfg.Shards; i++ {
		e.shards = append(e.shards, paxos.NewShard(i, cfg.NodesPerShard, tp, sink))
	}
	return e, nil
}

func (e *PaxosEngine) Config() Config { return e.cfg }

func (e *PaxosEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return e.tp.Close()
}

// Bootstrap elects a leader for each shard, in parallel. Shards are independent
// replication groups and nothing about one election informs another.
func (e *PaxosEngine) Bootstrap(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var wg sync.WaitGroup
	for _, shard := range e.shards {
		wg.Add(1)
		go func(shard *paxos.Shard) {
			defer wg.Done()
			e.reelect(ctx, shard)
		}(shard)
	}
	wg.Wait()

	e.resolvePending(ctx)
}

// cmdID mints an identity for one logical command. Re-proposals of that command
// reuse it, which is what lets the state machine ignore a duplicate.
// Callers hold e.mu.
func (e *PaxosEngine) cmdID() string {
	e.nextCmd++
	return "c" + strconv.FormatUint(e.nextCmd, 10)
}

func (e *PaxosEngine) shardFor(key string) *paxos.Shard {
	return e.shards[ShardFor(key, e.cfg.Shards)]
}

// ---------------------------------------------------------------- operations

func (e *PaxosEngine) Put(ctx context.Context, key string, value int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.resolvePending(ctx)

	if !e.keys[key] && e.cfg.MaxDistinctKey > 0 && len(e.keys) >= e.cfg.MaxDistinctKey {
		return fmt.Errorf("%w (limit %d)", ErrTooManyKeys, e.cfg.MaxDistinctKey)
	}

	shard := e.shardFor(key)
	err := shard.Propose(ctx, paxos.Command{ID: e.cmdID(), Type: paxos.CmdPut, Key: key, Value: value},
		requireUnlocked(key))
	if err != nil {
		return err
	}

	e.keys[key] = true
	return nil
}

func (e *PaxosEngine) Get(ctx context.Context, key string) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.resolvePending(ctx)

	shard := e.shardFor(key)
	leaderID, err := shard.ConfirmedLeader(ctx)
	if err != nil {
		return 0, err
	}

	value, ok := shard.Node(leaderID).Value(key)
	if !ok {
		return 0, fmt.Errorf("%q: %w", key, ErrNoSuchKey)
	}
	return value, nil
}

func (e *PaxosEngine) Transfer(ctx context.Context, from, to string, amount int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.resolvePending(ctx)

	srcShard := e.shardFor(from)
	dstShard := e.shardFor(to)

	if srcShard.ID() == dstShard.ID() {
		// Both keys live in the same replicated log, so one entry moves the
		// money atomically. There is nothing for 2PC to coordinate.
		e.log.TwoPC(fmt.Sprintf("%s and %s are both on s%d: single Paxos round, no 2PC",
			from, to, srcShard.ID()))
		err := srcShard.Propose(ctx,
			paxos.Command{ID: e.cmdID(), Type: paxos.CmdTransfer, Key: from, Dest: to, Value: amount},
			bothSidesReady(from, to, amount))
		return err
	}

	return e.twoPhaseCommit(ctx, srcShard, dstShard, from, to, amount)
}

// twoPhaseCommit moves value between two shards.
//
// Each shard runs its own Paxos round to durably record a prepared state. That
// round reaching quorum is that shard's yes vote: the shard has agreed, by
// majority, that it can hold up its end. Anything else is a no.
func (e *PaxosEngine) twoPhaseCommit(ctx context.Context, src, dst *paxos.Shard, from, to string, amount int64) error {
	e.nextTx++
	txID := "tx" + strconv.FormatUint(e.nextTx, 10)

	e.log.TwoPC(fmt.Sprintf("BEGIN %s: %s(s%d) -> %s(s%d) amount %d",
		txID, from, src.ID(), to, dst.ID(), amount))

	// Both commands are minted here, before anything runs in parallel. cmdID
	// advances a counter on the engine, and two goroutines calling it at once
	// can hand the same id to two different commands -- at which point the
	// state machine's duplicate suppression discards one of them as an echo of
	// the other, and half a transfer disappears.
	srcPrepare := paxos.Command{ID: e.cmdID(), Type: paxos.CmdPrepare, TxID: txID, Key: from, Value: -amount}
	dstPrepare := paxos.Command{ID: e.cmdID(), Type: paxos.CmdPrepare, TxID: txID, Key: to, Value: amount}

	// Both sides prepare in parallel. Each writes its own entry through its own
	// shard, so neither can observe the other's half.
	var (
		wg             sync.WaitGroup
		srcErr, dstErr error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		srcErr = src.Propose(ctx, srcPrepare, requireFunds(from, amount))
	}()
	go func() {
		defer wg.Done()
		dstErr = dst.Propose(ctx, dstPrepare, requireExists(to))
	}()
	wg.Wait()

	e.logVote(src.ID(), srcErr)
	e.logVote(dst.ID(), dstErr)

	if srcErr == nil && dstErr == nil {
		// Unanimity. The decision is made here and broadcast to both shards.
		e.log.TwoPC(fmt.Sprintf("COMMIT %s: both shards voted yes", txID))
		srcCommit := paxos.Command{ID: e.cmdID(), Type: paxos.CmdCommit, TxID: txID}
		dstCommit := paxos.Command{ID: e.cmdID(), Type: paxos.CmdCommit, TxID: txID}
		e.decide(ctx, txID, "commit", src, dst, srcCommit, dstCommit)
		return nil
	}

	// Any no vote aborts, and the abort goes to both shards regardless of how
	// either voted.
	//
	// A shard that reported failure may still have prepared: if its accept
	// round was chosen but the replies were lost, the leader sees an error
	// while the shard holds the debit. Aborting only the shards that said yes
	// leaves that one debited with nobody coming back for it. An abort for a
	// transaction a shard never prepared finds nothing held and does nothing,
	// so sending it everywhere costs a round and removes the whole class of
	// failure.
	e.log.TwoPC(fmt.Sprintf("ABORT %s: not unanimous", txID))
	srcCmd := paxos.Command{ID: e.cmdID(), Type: paxos.CmdAbort, TxID: txID}
	dstCmd := paxos.Command{ID: e.cmdID(), Type: paxos.CmdAbort, TxID: txID}
	e.decide(ctx, txID, "abort", src, dst, srcCmd, dstCmd)

	if srcErr != nil {
		return srcErr
	}
	return dstErr
}

func (e *PaxosEngine) logVote(shard int, err error) {
	if err == nil {
		e.log.TwoPC(fmt.Sprintf("s%d votes YES", shard))
		return
	}
	e.log.TwoPC(fmt.Sprintf("s%d votes NO: %s", shard, err.Error()))
}

// pendingDecision is a 2PC outcome that has been decided but not yet recorded
// on every shard that took part.
type pendingDecision struct {
	txID string
	kind string // "commit" or "abort"
	// parts holds the shards still owed the decision. A shard is dropped from
	// the slice once it has durably recorded the outcome.
	parts []decisionPart
}

type decisionPart struct {
	shard *paxos.Shard
	cmd   paxos.Command
}

// decide records the 2PC outcome and then delivers it to each shard that
// prepared.
//
// The decision is written down *before* any attempt to deliver it, which is the
// whole point. Once both shards have voted, the outcome is determined; a shard
// that cannot be reached at that instant is owed the decision, not released from
// it. Recording first and retrying later is what stops a shard sitting on a
// prepared transaction that nobody will ever resolve.
//
// The store here is process memory, which is honest about what it survives: a
// node dying, a network splitting, a leader losing its shard mid-decision — all
// of which the chaos test produces in quantity. It does not survive the
// coordinator process itself dying. Making it do so means writing the record
// somewhere outside the process, which an in-memory demo has nowhere to put.
func (e *PaxosEngine) decide(ctx context.Context, txID, kind string, src, dst *paxos.Shard, srcCmd, dstCmd paxos.Command) {
	decision := &pendingDecision{txID: txID, kind: kind}
	if srcCmd.Type != "" {
		decision.parts = append(decision.parts, decisionPart{shard: src, cmd: srcCmd})
	}
	if dstCmd.Type != "" {
		decision.parts = append(decision.parts, decisionPart{shard: dst, cmd: dstCmd})
	}
	if len(decision.parts) == 0 {
		return
	}

	e.pending = append(e.pending, decision)
	e.deliver(ctx, decision, false)
	e.prunePending()
}

// deliver attempts the outstanding parts of one decision, dropping each part
// that lands. It reports whether anything was delivered.
func (e *PaxosEngine) deliver(ctx context.Context, d *pendingDecision, retry bool) bool {
	type outcome struct {
		index int
		err   error
	}

	results := make([]outcome, len(d.parts))
	var wg sync.WaitGroup
	for i, part := range d.parts {
		wg.Add(1)
		go func(i int, part decisionPart) {
			defer wg.Done()
			results[i] = outcome{index: i, err: part.shard.Propose(ctx, part.cmd, nil)}
		}(i, part)
	}
	wg.Wait()

	var remaining []decisionPart
	delivered := false
	for i, part := range d.parts {
		if results[i].err == nil {
			delivered = true
			if retry {
				e.log.TwoPC(fmt.Sprintf("recovered: s%d recorded the %s of %s",
					part.shard.ID(), d.kind, d.txID))
			}
			continue
		}
		if !retry {
			e.log.Error(fmt.Sprintf(
				"s%d could not record the %s of %s: %v. it is held and will be retried.",
				part.shard.ID(), d.kind, d.txID, results[i].err))
		}
		remaining = append(remaining, part)
	}
	d.parts = remaining
	return delivered
}

// prunePending drops decisions that every shard has now recorded.
func (e *PaxosEngine) prunePending() {
	var open []*pendingDecision
	for _, d := range e.pending {
		if len(d.parts) > 0 {
			open = append(open, d)
		}
	}
	e.pending = open
}

// resolvePending retries decisions that could not be delivered when they were
// made. It runs at the start of every operation, so a cluster that regains a
// quorum finishes its outstanding transactions before doing anything new,
// rather than carrying the locks forward.
//
// Retrying once per operation rather than in a background loop keeps this
// bounded and keeps the log quiet: a cluster with nothing outstanding does no
// work here at all.
func (e *PaxosEngine) resolvePending(ctx context.Context) {
	if len(e.pending) == 0 {
		return
	}
	for _, d := range e.pending {
		e.deliver(ctx, d, true)
	}
	e.prunePending()
}

// Outstanding reports how many 2PC decisions are still owed to some shard. It
// is what `status` uses to show that a transaction is mid-flight rather than
// finished.
func (e *PaxosEngine) Outstanding() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.pending)
}

// ------------------------------------------------------------ fault commands

func (e *PaxosEngine) Kill(ctx context.Context, node string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	shard, err := e.shardOfNode(node)
	if err != nil {
		return err
	}
	if !e.tp.Faults().Kill(node) {
		return fmt.Errorf("%s is already down", node)
	}

	leader, _ := shard.Leader()
	if leader == node {
		shard.StepDown("the node was killed")
		// Re-elect immediately so the operator sees the consequence of the kill
		// rather than discovering it on their next unrelated command.
		e.reelect(ctx, shard)
	}
	return nil
}

func (e *PaxosEngine) Revive(ctx context.Context, node string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	shard, err := e.shardOfNode(node)
	if err != nil {
		return err
	}
	e.resolvePending(ctx)

	if !e.tp.Faults().Revive(node) {
		return fmt.Errorf("%s is already up", node)
	}

	// A revived node keeps the log it died with. It catches up the next time a
	// leader includes it in a round, or when it wins one and recovers state
	// from the quorum's promises.
	if leader, _ := shard.Leader(); leader == "" {
		e.reelect(ctx, shard)
	}
	// A replica coming back may be the quorum an outstanding decision needed.
	e.resolvePending(ctx)
	return nil
}

func (e *PaxosEngine) Partition(ctx context.Context, groups [][]string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, group := range groups {
		for _, id := range group {
			if _, err := e.shardOfNode(id); err != nil {
				return err
			}
		}
	}

	e.tp.Faults().Partition(groups)

	// Every shard re-establishes leadership from scratch. A leader on the wrong
	// side of the split cannot reach a quorum and therefore cannot win; a
	// shard with no majority group finds no leader at all.
	for _, shard := range e.shards {
		shard.StepDown("the network was partitioned")
		e.reelect(ctx, shard)
	}
	return nil
}

func (e *PaxosEngine) Heal(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Resolve before the early return: a cluster with nothing to heal may still
	// be owed a decision from a failure that has since cleared.
	e.resolvePending(ctx)

	if !e.tp.Faults().Heal() {
		return nil
	}
	for _, shard := range e.shards {
		if leader, _ := shard.Leader(); leader == "" {
			e.reelect(ctx, shard)
		}
	}
	e.resolvePending(ctx)
	return nil
}

// reelect attempts an election and reports failure as a normal state rather
// than an error: a shard with no majority is supposed to end up leaderless.
func (e *PaxosEngine) reelect(ctx context.Context, shard *paxos.Shard) {
	if _, _, err := shard.Elect(ctx); err != nil {
		e.log.WithTag(ShardTag(shard.ID())).Leader(
			fmt.Sprintf("s%d has no leader: %s", shard.ID(), err.Error()))
	}
}

func (e *PaxosEngine) shardOfNode(id string) (*paxos.Shard, error) {
	for _, shard := range e.shards {
		for _, node := range shard.NodeIDs() {
			if node == id {
				return shard, nil
			}
		}
	}
	return nil, fmt.Errorf("%w: %s", ErrUnknownNode, id)
}

// ------------------------------------------------------------------- status

func (e *PaxosEngine) Status(ctx context.Context) (Status, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Status should report what the cluster settles on, not a transient state
	// it has the means to leave.
	e.resolvePending(ctx)

	faults := e.tp.Faults()
	st := Status{Partitions: faults.Groups()}

	values := map[string]int64{}
	locks := map[string]string{}

	for _, shard := range e.shards {
		leader, ballot := shard.Leader()
		if leader != "" && !faults.Alive(leader) {
			leader = ""
		}

		ss := ShardStatus{
			Shard:  shard.ID(),
			Leader: leader,
			Term:   ballot.Number,
			Alive:  shard.AliveCount(),
			Total:  e.cfg.NodesPerShard,
			Quorum: shard.Quorum(),
		}

		// Read state from the most advanced live replica. The leader is the
		// obvious choice when there is one; when there is not, showing the
		// furthest-along survivor beats showing nothing.
		var best *paxos.Snapshot
		for _, node := range shard.Nodes() {
			alive := faults.Alive(node.ID())
			ss.Nodes = append(ss.Nodes, NodeStatus{ID: node.ID(), Alive: alive})
			if !alive {
				continue
			}
			snap := node.Snapshot()
			if best == nil || snap.Applied > best.Applied {
				copied := snap
				best = &copied
			}
		}
		if best != nil {
			ss.LogSize = best.LogSize
			for k, v := range best.State {
				values[k] = v
			}
			for k, tx := range best.Locks {
				locks[k] = tx
			}
		}
		st.Shards = append(st.Shards, ss)
	}

	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		st.Accounts = append(st.Accounts, AccountStatus{
			Key:      k,
			Shard:    ShardFor(k, e.cfg.Shards),
			Value:    values[k],
			LockedBy: locks[k],
		})
	}
	return st, nil
}

// Datastore renders every replica's committed log, so two replicas that
// disagree are visible rather than merely suspected.
func (e *PaxosEngine) Datastore() []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	var out []string
	for _, shard := range e.shards {
		for _, node := range shard.Nodes() {
			snap := node.Snapshot()
			parts := make([]string, 0, len(snap.Committed))
			for _, entry := range snap.Committed {
				parts = append(parts, fmt.Sprintf("%s%s", entry.Ballot, entry.Cmd))
			}
			line := node.ID() + ": "
			if len(parts) == 0 {
				line += "(empty)"
			} else {
				line += strings.Join(parts, " -> ")
			}
			out = append(out, line)
		}
	}
	return out
}

// ------------------------------------------------------------- preconditions

// requireUnlocked refuses a write to a key held by an in-flight transaction.
func requireUnlocked(key string) func(*paxos.Node) error {
	return func(n *paxos.Node) error {
		if tx, locked := n.Locked(key); locked {
			return fmt.Errorf("%s is held by %s: %w", key, tx, paxos.ErrLocked)
		}
		return nil
	}
}

// requireExists refuses to credit a key that was never created, so a typo in a
// destination does not silently mint an account.
func requireExists(key string) func(*paxos.Node) error {
	return func(n *paxos.Node) error {
		if tx, locked := n.Locked(key); locked {
			return fmt.Errorf("%s is held by %s: %w", key, tx, paxos.ErrLocked)
		}
		if _, ok := n.Value(key); !ok {
			return fmt.Errorf("%q: %w", key, ErrNoSuchKey)
		}
		return nil
	}
}

// requireFunds is the source side's precondition.
func requireFunds(key string, amount int64) func(*paxos.Node) error {
	return func(n *paxos.Node) error {
		if tx, locked := n.Locked(key); locked {
			return fmt.Errorf("%s is held by %s: %w", key, tx, paxos.ErrLocked)
		}
		value, ok := n.Value(key)
		if !ok {
			return fmt.Errorf("%q: %w", key, ErrNoSuchKey)
		}
		if value < amount {
			return fmt.Errorf("%s holds %d and needs %d: %w", key, value, amount, ErrInsufficientFunds)
		}
		return nil
	}
}

// bothSidesReady is the single-shard transfer precondition: both keys are in
// the same log, so both can be checked against the same leader.
func bothSidesReady(from, to string, amount int64) func(*paxos.Node) error {
	return func(n *paxos.Node) error {
		if err := requireFunds(from, amount)(n); err != nil {
			return err
		}
		return requireExists(to)(n)
	}
}
