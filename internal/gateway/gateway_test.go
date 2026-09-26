package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/tusharpanthri/distributed-banking-system/internal/cluster"
	"github.com/tusharpanthri/distributed-banking-system/internal/logstream"
)

// frame is the union of what the server may send, decoded loosely so a test can
// assert on the wire shape rather than on Go types.
type frame struct {
	Type  string `json:"type"`
	Level string `json:"level"`
	Tag   string `json:"tag"`
	Msg   string `json:"msg"`
}

type client struct {
	t    *testing.T
	conn *websocket.Conn
	ctx  context.Context
}

// newTestServer starts a real HTTP server with pacing disabled.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	gw, err := New(Options{
		Cluster: cluster.Config{Shards: 3, NodesPerShard: 3},
		Pace:    0,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func dial(t *testing.T, srv *httptest.Server) *client {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })

	c := &client{t: t, conn: conn, ctx: ctx}
	c.drainGreeting()
	return c
}

func (c *client) read() frame {
	c.t.Helper()
	readCtx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()

	_, data, err := c.conn.Read(readCtx)
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	var f frame
	if err := json.Unmarshal(data, &f); err != nil {
		c.t.Fatalf("unmarshal %q: %v", data, err)
	}
	return f
}

// drainGreeting consumes the connect banner, which runs the startup elections
// and terminates in a result frame like any command.
func (c *client) drainGreeting() {
	c.t.Helper()
	for i := 0; i < 500; i++ {
		f := c.read()
		if f.Type == "result" {
			if !strings.HasPrefix(f.Msg, "OK cluster up") {
				c.t.Fatalf("greeting ended with %q", f.Msg)
			}
			return
		}
	}
	c.t.Fatal("greeting never produced a result frame")
}

// send issues a command and returns every log frame plus the result. It fails
// the test if a second result arrives or if the stream ends without one.
func (c *client) send(cmd string) (logs []frame, result frame) {
	c.t.Helper()

	payload, err := json.Marshal(inbound{Cmd: cmd})
	if err != nil {
		c.t.Fatalf("marshal: %v", err)
	}
	writeCtx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	if err := c.conn.Write(writeCtx, websocket.MessageText, payload); err != nil {
		c.t.Fatalf("write: %v", err)
	}

	for i := 0; i < 200; i++ {
		f := c.read()
		switch f.Type {
		case "log":
			if !logstream.Valid(logstream.Level(f.Level)) {
				c.t.Errorf("command %q produced level %q, which is not in the protocol", cmd, f.Level)
			}
			logs = append(logs, f)
		case "result":
			return logs, f
		default:
			c.t.Fatalf("command %q produced frame type %q", cmd, f.Type)
		}
	}
	c.t.Fatalf("command %q produced no result frame", cmd)
	return nil, frame{}
}

// sendRaw writes bytes that are not necessarily a valid command frame.
func (c *client) sendRaw(payload string) frame {
	c.t.Helper()
	writeCtx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()
	if err := c.conn.Write(writeCtx, websocket.MessageText, []byte(payload)); err != nil {
		c.t.Fatalf("write: %v", err)
	}
	for i := 0; i < 200; i++ {
		if f := c.read(); f.Type == "result" {
			return f
		}
	}
	c.t.Fatal("no result frame")
	return frame{}
}

func TestHealthz(t *testing.T) {
	srv := newTestServer(t)

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

// The protocol's core promise: zero or more logs, then exactly one result.
func TestEveryCommandEndsWithExactlyOneResult(t *testing.T) {
	c := dial(t, newTestServer(t))

	for _, cmd := range []string{
		"status", "help", "clear",
		"put alice 100", "get alice", "put bob 50",
		"kill s0n1", "revive s0n1",
		"partition s0n0 | s0n1 s0n2", "heal",
		"nonsense", "put",
	} {
		// send itself fails the test on a missing or duplicate result.
		if _, result := c.send(cmd); result.Type != "result" {
			t.Errorf("command %q: result type = %q", cmd, result.Type)
		}
	}
}

func TestPutGetRoundTrips(t *testing.T) {
	c := dial(t, newTestServer(t))

	if _, result := c.send("put alice 100"); !strings.HasPrefix(result.Msg, "OK") {
		t.Fatalf("put: %q", result.Msg)
	}
	_, result := c.send("get alice")
	if result.Msg != "OK alice=100" {
		t.Errorf("get alice = %q, want %q", result.Msg, "OK alice=100")
	}
}

func TestTransferMovesValue(t *testing.T) {
	c := dial(t, newTestServer(t))

	c.send("put alice 100")
	c.send("put bob 10")
	if _, result := c.send("transfer alice bob 25"); !strings.HasPrefix(result.Msg, "OK") {
		t.Fatalf("transfer: %q", result.Msg)
	}

	if _, result := c.send("get alice"); result.Msg != "OK alice=75" {
		t.Errorf("alice = %q, want OK alice=75", result.Msg)
	}
	if _, result := c.send("get bob"); result.Msg != "OK bob=35" {
		t.Errorf("bob = %q, want OK bob=35", result.Msg)
	}
}

func TestTransferAbortsOnInsufficientFunds(t *testing.T) {
	c := dial(t, newTestServer(t))

	c.send("put alice 10")
	c.send("put bob 10")
	_, result := c.send("transfer alice bob 500")
	if !strings.HasPrefix(result.Msg, "ABORT") {
		t.Errorf("result = %q, want an ABORT", result.Msg)
	}

	// An aborted transfer must not have moved anything.
	if _, r := c.send("get alice"); r.Msg != "OK alice=10" {
		t.Errorf("alice = %q, want unchanged at 10", r.Msg)
	}
	if _, r := c.send("get bob"); r.Msg != "OK bob=10" {
		t.Errorf("bob = %q, want unchanged at 10", r.Msg)
	}
}

// Losing quorum is the point of the demo, so it must read as a clean ABORT
// rather than an internal error.
func TestWritesAbortWhenShardLosesQuorum(t *testing.T) {
	c := dial(t, newTestServer(t))

	// "alice" is placed by the same hash the server uses, so ask rather than
	// assume which shard to take down.
	c.send("put alice 100")
	logs, _ := c.send("status")
	shard := shardOfKey(t, logs, "alice")

	c.send("kill " + cluster.NodeID(shard, 0))
	c.send("kill " + cluster.NodeID(shard, 1))

	_, result := c.send("put alice 999")
	if !strings.HasPrefix(result.Msg, "ABORT") {
		t.Errorf("put with 1/3 alive = %q, want an ABORT", result.Msg)
	}
	if !strings.Contains(result.Msg, "quorum") {
		t.Errorf("result %q should explain that quorum was lost", result.Msg)
	}
}

// A leader isolated from the majority must step down, leaving the shard to the
// group that still holds a quorum.
func TestPartitionedMinorityCannotWrite(t *testing.T) {
	c := dial(t, newTestServer(t))

	c.send("put alice 100")
	logs, _ := c.send("status")
	shard := shardOfKey(t, logs, "alice")

	// Split the shard 1 | 2. The lone node cannot reach a majority.
	c.send("partition " + cluster.NodeID(shard, 0) + " | " + cluster.NodeID(shard, 1) + " " + cluster.NodeID(shard, 2))

	// The majority side still holds the shard, so writes succeed.
	if _, result := c.send("put alice 200"); !strings.HasPrefix(result.Msg, "OK") {
		t.Errorf("majority side should still accept writes, got %q", result.Msg)
	}

	// Now split every node of the shard apart: no group holds a majority.
	c.send("partition " + cluster.NodeID(shard, 0) + " | " + cluster.NodeID(shard, 1) + " | " + cluster.NodeID(shard, 2))
	_, result := c.send("put alice 300")
	if !strings.HasPrefix(result.Msg, "ABORT") {
		t.Errorf("fully split shard = %q, want an ABORT", result.Msg)
	}

	if _, result := c.send("heal"); !strings.HasPrefix(result.Msg, "OK") {
		t.Errorf("heal = %q", result.Msg)
	}
	if _, result := c.send("put alice 400"); !strings.HasPrefix(result.Msg, "OK") {
		t.Errorf("after heal, writes should work again, got %q", result.Msg)
	}
}

func TestMalformedFramesGetErrorResults(t *testing.T) {
	c := dial(t, newTestServer(t))

	if f := c.sendRaw("this is not json"); !strings.HasPrefix(f.Msg, "ERROR") {
		t.Errorf("non-JSON frame = %q, want an ERROR", f.Msg)
	}
	if f := c.sendRaw(`{"cmd":"frobnicate"}`); !strings.HasPrefix(f.Msg, "ERROR") {
		t.Errorf("unknown command = %q, want an ERROR", f.Msg)
	}
	// A connection must survive bad input rather than being closed on it.
	if _, result := c.send("status"); !strings.HasPrefix(result.Msg, "OK") {
		t.Errorf("session should still work after bad frames, got %q", result.Msg)
	}
}

// Each connection gets its own cluster, so damage does not leak between
// visitors. This is the "reset on every visit" guarantee.
func TestEachConnectionGetsAFreshCluster(t *testing.T) {
	srv := newTestServer(t)

	first := dial(t, srv)
	first.send("put alice 100")
	first.send("kill s0n0")
	first.send("partition s1n0 | s1n1 s1n2")

	second := dial(t, srv)
	if _, result := second.send("get alice"); !strings.HasPrefix(result.Msg, "ABORT") {
		t.Errorf("a new connection should not see the first one's keys, got %q", result.Msg)
	}
	logs, result := second.send("status")
	if !strings.Contains(result.Msg, "0 keys") {
		t.Errorf("new session status = %q, want 0 keys", result.Msg)
	}
	for _, l := range logs {
		if strings.Contains(l.Msg, "DOWN") {
			t.Errorf("new session sees a dead node from another session: %q", l.Msg)
		}
		if strings.Contains(l.Msg, "partitioned") {
			t.Errorf("new session sees another session's partition: %q", l.Msg)
		}
	}
}

func TestStatusReportsLeaderAndLiveness(t *testing.T) {
	c := dial(t, newTestServer(t))

	logs, result := c.send("status")
	if !strings.HasPrefix(result.Msg, "OK 3 shards, 3 writable") {
		t.Errorf("status result = %q", result.Msg)
	}

	shardLines := 0
	for _, l := range logs {
		if l.Level == string(logstream.Leader) {
			shardLines++
			if !strings.Contains(l.Msg, "leader=") || !strings.Contains(l.Msg, "alive=3/3") {
				t.Errorf("shard line %q should report leader and liveness", l.Msg)
			}
		}
	}
	if shardLines != 3 {
		t.Errorf("status reported %d shards, want 3", shardLines)
	}
}

func TestUnknownNodeIsRejected(t *testing.T) {
	c := dial(t, newTestServer(t))

	for _, cmd := range []string{"kill s9n9", "revive s9n9", "partition s9n9 | s0n0"} {
		if _, result := c.send(cmd); strings.HasPrefix(result.Msg, "OK") {
			t.Errorf("%q should have been rejected, got %q", cmd, result.Msg)
		}
	}
}

// shardOfKey reads the shard a key landed on out of a status listing, so tests
// do not have to duplicate the hash.
func shardOfKey(t *testing.T, logs []frame, key string) int {
	t.Helper()
	for _, l := range logs {
		if !strings.HasPrefix(l.Msg, key+" = ") {
			continue
		}
		digits := strings.TrimSuffix(strings.TrimPrefix(l.Tag, "[s"), "]")
		shard, err := strconv.Atoi(digits)
		if err != nil {
			t.Fatalf("status tagged %q for key %q, which is not a shard tag", l.Tag, key)
		}
		return shard
	}
	t.Fatalf("key %q not found in status output", key)
	return -1
}
