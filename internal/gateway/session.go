package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/tusharpanthri/distributed-banking-system/internal/cluster"
	"github.com/tusharpanthri/distributed-banking-system/internal/logstream"
)

// outboxSize bounds how many log frames may be queued for one browser. A
// consumer that stops reading must not be able to stall the cluster, so past
// this point events are dropped rather than blocking the emitting goroutine.
const outboxSize = 512

// session is one WebSocket connection and the private cluster built for it.
//
// One cluster per connection is what makes "reset on every visit" true: the
// cluster is constructed when the socket opens and destroyed when it closes, so
// a visitor who leaves the network partitioned cannot hand that state to the
// next visitor.
type session struct {
	conn   *websocket.Conn
	engine cluster.Engine
	pace   time.Duration
	logger *slog.Logger

	// out carries both log and result frames so that ordering between them is
	// the channel's ordering. Everything emitted before a command's result is
	// queued ahead of it, which is what gives the protocol its guarantee of
	// zero or more logs followed by exactly one result.
	out     chan any
	dropped int
}

func newSession(conn *websocket.Conn, cfg cluster.Config, pace time.Duration, logger *slog.Logger) (*session, error) {
	s := &session{
		conn:   conn,
		pace:   pace,
		logger: logger,
		out:    make(chan any, outboxSize),
	}

	engine, err := cluster.NewPaxos(cfg, logstream.SinkFunc(s.emit))
	if err != nil {
		return nil, err
	}
	s.engine = engine
	return s, nil
}

// emit queues one cluster event. It never blocks: a browser that has stopped
// reading loses log lines, which is strictly better than wedging consensus.
func (s *session) emit(e logstream.Event) {
	select {
	case s.out <- newLogFrame(e):
	default:
		s.dropped++
	}
}

// result queues the terminating frame for a command.
func (s *session) result(msg string) {
	select {
	case s.out <- newResultFrame(msg):
	case <-time.After(time.Second):
		// The result frame is what re-enables the caller's prompt, so it is
		// worth waiting for room rather than dropping it outright.
		s.logger.Warn("dropped result frame, outbox full", "msg", msg)
	}
}

// greet brings the cluster up and narrates it.
//
// The banner ends with a result frame, exactly as a command would, so the
// frontend can hold its prompt disabled until the startup elections finish
// instead of inviting commands at a cluster that has no leaders yet. Connecting
// is, in effect, an implicit command.
func (s *session) greet(ctx context.Context) {
	cfg := s.engine.Config()
	s.emit(logstream.Event{
		Level: logstream.Info,
		Tag:   "[gw]",
		Msg: fmt.Sprintf("fresh cluster: %d shards x %d nodes, quorum %d",
			cfg.Shards, cfg.NodesPerShard, cfg.Quorum()),
	})
	s.emit(logstream.Event{
		Level: logstream.Info,
		Tag:   "[gw]",
		Msg:   "this cluster is yours alone and resets when you disconnect. type help",
	})

	s.engine.Bootstrap(ctx)

	st, err := s.engine.Status(ctx)
	if err != nil {
		s.result("ERROR " + err.Error())
		return
	}
	ready := 0
	for _, shard := range st.Shards {
		if shard.Leader != "" {
			ready++
		}
	}
	s.result(fmt.Sprintf("OK cluster up, %d/%d shards led", ready, len(st.Shards)))
}

// run drives the connection until the client disconnects or ctx is cancelled.
func (s *session) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() { _ = s.engine.Close() }()

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		s.writeLoop(ctx)
	}()

	err := s.readLoop(ctx)
	cancel()
	<-writerDone
	return err
}

// writeLoop is the only goroutine that writes to the socket. Frames leave in
// queue order, separated by the display pace.
func (s *session) writeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case frame := <-s.out:
			// Pacing is presentation only. It lives here, at the edge, and
			// never inside the consensus path: a log stream that arrives as one
			// instant dump is unreadable, but a Paxos round that sleeps is a
			// lie about how fast the system is.
			if _, isLog := frame.(logFrame); isLog && s.pace > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(s.pace):
				}
			}

			payload, err := json.Marshal(frame)
			if err != nil {
				s.logger.Error("marshalling frame", "err", err)
				continue
			}
			writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = s.conn.Write(writeCtx, websocket.MessageText, payload)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

// readLoop reads one command at a time and runs it to completion before reading
// the next, so a connection never has two commands in flight.
func (s *session) readLoop(ctx context.Context) error {
	limiter := newRateLimiter(commandsPerSecond, commandBurst)

	for {
		readCtx, cancel := context.WithTimeout(ctx, idleTimeout)
		_, data, err := s.conn.Read(readCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, context.DeadlineExceeded) {
				s.result("ABORT idle for too long, closing the connection")
				s.drain()
				return s.conn.Close(websocket.StatusNormalClosure, "idle timeout")
			}
			return nil // the client hung up
		}

		var msg inbound
		if err := json.Unmarshal(data, &msg); err != nil {
			s.emit(logstream.Event{Level: logstream.Error, Tag: "[gw]", Msg: "frame was not valid JSON"})
			s.result("ERROR expected a JSON object of the form {\"cmd\": \"...\"}")
			continue
		}

		if !limiter.allow() {
			s.result("ERROR slow down: too many commands per second")
			continue
		}

		s.dispatch(ctx, msg.Cmd)
	}
}

// drain gives the writer a moment to flush anything queued before the socket
// closes, so a final message is not lost to the close handshake.
func (s *session) drain() {
	deadline := time.After(2 * time.Second)
	for {
		if len(s.out) == 0 {
			return
		}
		select {
		case <-deadline:
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// dispatch parses one line, runs it, and guarantees exactly one result frame.
func (s *session) dispatch(ctx context.Context, line string) {
	cmd, err := parse(line)
	if errors.Is(err, errEmpty) {
		// A bare newline gets no answer at all. Answering it would mean the
		// frontend prints a complaint every time someone hits enter.
		s.result("")
		return
	}
	if err != nil {
		s.emit(logstream.Event{Level: logstream.Error, Tag: "[gw]", Msg: err.Error()})
		s.result("ERROR " + err.Error())
		return
	}

	s.emit(logstream.Event{Level: logstream.Client, Tag: "[you]", Msg: strings.TrimSpace(line)})

	switch cmd.Verb {
	case verbHelp:
		for _, l := range strings.Split(helpText, "\n") {
			s.emit(logstream.Event{Level: logstream.Info, Tag: "", Msg: l})
		}
		s.result("OK")

	case verbClear, verbDemo:
		// Client-side only. Accepted rather than rejected so that a client which
		// forwards every line verbatim does not get an error for a command its
		// own UI handles.
		s.result("")

	case verbStatus:
		s.runStatus(ctx)

	case verbDatastore:
		lines := s.engine.Datastore()
		for _, l := range lines {
			s.emit(logstream.Event{Level: logstream.Paxos, Tag: "[log]", Msg: l})
		}
		s.result(fmt.Sprintf("OK %d replicas", len(lines)))

	case verbPut:
		if err := s.engine.Put(ctx, cmd.Key, cmd.Value); err != nil {
			s.fail(err)
			return
		}
		s.result(fmt.Sprintf("OK %s=%d on s%d", cmd.Key, cmd.Value,
			cluster.ShardFor(cmd.Key, s.engine.Config().Shards)))

	case verbGet:
		value, err := s.engine.Get(ctx, cmd.Key)
		if err != nil {
			s.fail(err)
			return
		}
		s.result(fmt.Sprintf("OK %s=%d", cmd.Key, value))

	case verbTransfer:
		if err := s.engine.Transfer(ctx, cmd.From, cmd.To, cmd.Value); err != nil {
			s.fail(err)
			return
		}
		s.result(fmt.Sprintf("OK moved %d from %s to %s", cmd.Value, cmd.From, cmd.To))

	case verbKill:
		if err := s.engine.Kill(ctx, cmd.Node); err != nil {
			s.fail(err)
			return
		}
		s.result("OK " + cmd.Node + " is down")

	case verbRevive:
		if err := s.engine.Revive(ctx, cmd.Node); err != nil {
			s.fail(err)
			return
		}
		s.result("OK " + cmd.Node + " is up")

	case verbPartition:
		if err := s.engine.Partition(ctx, cmd.Groups); err != nil {
			s.fail(err)
			return
		}
		s.result(fmt.Sprintf("OK network split into %d groups", len(cmd.Groups)))

	case verbHeal:
		if err := s.engine.Heal(ctx); err != nil {
			s.fail(err)
			return
		}
		s.result("OK partitions removed")

	default:
		s.result("ERROR unknown command")
	}
}

// fail renders an engine error. Losing quorum or running out of funds is an
// expected outcome of this demo rather than a malfunction, so those read as
// ABORT; anything else is a genuine error.
func (s *session) fail(err error) {
	expected := errors.Is(err, cluster.ErrNoQuorum) ||
		errors.Is(err, cluster.ErrNoLeader) ||
		errors.Is(err, cluster.ErrNoSuchKey) ||
		errors.Is(err, cluster.ErrLocked) ||
		errors.Is(err, cluster.ErrInsufficientFunds) ||
		errors.Is(err, cluster.ErrTooManyKeys)

	if expected {
		// The result line carries the reason already; emitting it as a log
		// frame too just prints the same sentence twice, one line apart.
		s.result("ABORT " + err.Error())
		return
	}
	s.emit(logstream.Event{Level: logstream.Error, Tag: "[gw]", Msg: err.Error()})
	s.result("ERROR " + err.Error())
}

func (s *session) runStatus(ctx context.Context) {
	st, err := s.engine.Status(ctx)
	if err != nil {
		s.fail(err)
		return
	}

	// Count shards that can actually take a write. Live-node count alone is not
	// that number: a shard can have every replica up and still be unwritable if
	// a partition leaves no group holding a majority, which is precisely the
	// state this demo exists to show.
	writable := 0
	for _, shard := range st.Shards {
		leader := shard.Leader
		if leader == "" {
			leader = "none"
		}
		if shard.Leader != "" {
			writable++
		}

		members := make([]string, 0, len(shard.Nodes))
		for _, n := range shard.Nodes {
			state := "up"
			if !n.Alive {
				state = "DOWN"
			}
			marker := ""
			if n.ID == shard.Leader {
				marker = "*"
			}
			members = append(members, fmt.Sprintf("%s%s:%s", marker, n.ID, state))
		}

		s.emit(logstream.Event{
			Level: logstream.Leader,
			Tag:   cluster.ShardTag(shard.Shard),
			Msg: fmt.Sprintf("leader=%s term=%d alive=%d/%d needs=%d  %s",
				leader, shard.Term, shard.Alive, shard.Total, shard.Quorum,
				strings.Join(members, " ")),
		})
	}

	if len(st.Partitions) > 0 {
		rendered := make([]string, len(st.Partitions))
		for i, group := range st.Partitions {
			rendered[i] = "{" + strings.Join(group, " ") + "}"
		}
		s.emit(logstream.Event{
			Level: logstream.Net,
			Tag:   "[net]",
			Msg:   "partitioned: " + strings.Join(rendered, " | "),
		})
	}

	if len(st.Accounts) == 0 {
		s.emit(logstream.Event{Level: logstream.Info, Tag: "[keys]", Msg: "no keys yet, try: put tushar 100"})
	}
	for _, acct := range st.Accounts {
		s.emit(logstream.Event{
			Level: logstream.Info,
			Tag:   cluster.ShardTag(acct.Shard),
			Msg:   fmt.Sprintf("%s = %d", acct.Key, acct.Value),
		})
	}

	s.result(fmt.Sprintf("OK %d shards, %d writable, %d keys",
		len(st.Shards), writable, len(st.Accounts)))
}
