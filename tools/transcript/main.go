// Command transcript records a scripted session against a real gateway and
// writes every frame to a JSONL file, for rendering into the README's
// animation and screenshots.
//
// It starts the gateway in-process and drives it over a genuine WebSocket, so
// what it records is byte-for-byte what a browser receives. Nothing here
// fabricates output: if the animation shows a shard losing quorum, the shard
// lost quorum.
//
//	go run ./tools/transcript -out docs/screenshots/transcript.jsonl
//
// The script below mirrors the TOUR array in frontend/index.html. The two are
// kept in step by hand; they are the same seventeen lines of data, and a shared
// file would mean the static frontend had to fetch it at runtime.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/tusharpanthri/distributed-banking-system/internal/cluster"
	"github.com/tusharpanthri/distributed-banking-system/internal/gateway"
)

type step struct {
	Say string
	Cmd string
}

var script = []step{
	{Say: "a fresh cluster: 3 shards, 3 replicas each. a majority is 2.", Cmd: "status"},
	{Say: "every write is one paxos round. the leader proposes, a majority accepts, then it commits.", Cmd: "put tushar 100"},
	{Cmd: "put ram 50"},
	{Say: "tushar and ram hashed to the same shard, so one log entry moves the money. no 2pc needed.", Cmd: "transfer tushar ram 30"},
	{Say: "varun hashed to a different shard.", Cmd: "put varun 40"},
	{Say: "so this transfer has to cross shards. each side runs its own paxos round, and reaching quorum IS that shard's vote. unanimous means commit.", Cmd: "transfer tushar varun 25"},
	{Say: "every replica holds the same entries, in the same order, under the same ballots.", Cmd: "datastore"},
	{Say: "now break it. kill the leader of tushar's shard.", Cmd: "kill s2n0"},
	{Say: "a new leader was elected at a higher ballot. there is no separate election protocol here - electing IS the paxos prepare phase.", Cmd: "transfer tushar varun 10"},
	{Say: "two of three is still a majority, so that worked. kill a second replica.", Cmd: "kill s2n1"},
	{Say: "one replica left out of three. it holds all the data and it still refuses, because committing alone is how you lose money.", Cmd: "put tushar 999"},
	{Say: "bring them back. a revived replica returns with the log it died with.", Cmd: "revive s2n0"},
	{Cmd: "revive s2n1"},
	{Say: "now split the network instead of killing anything. all three replicas stay alive - they just cannot reach each other.", Cmd: "partition s2n0 | s2n1 | s2n2"},
	{Say: "3 of 3 alive, and no leader. liveness is not reachability, and a dead-node count cannot express this.", Cmd: "status"},
	{Say: "heal the split and the shard elects again on its own.", Cmd: "heal"},
	{Say: "that's the tour. the cluster is yours - break it however you like.", Cmd: "status"},
}

// record is one line of the output file. "narr" and "cmd" are added by this
// tool to mark what the browser would be showing locally; "log" and "result"
// come off the wire untouched.
type record struct {
	Type  string `json:"type"`
	Level string `json:"level,omitempty"`
	Tag   string `json:"tag,omitempty"`
	Msg   string `json:"msg"`
}

func main() {
	out := flag.String("out", "docs/screenshots/transcript.jsonl", "output file")
	flag.Parse()

	if err := run(*out); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(path string) error {
	gw, err := gateway.New(gateway.Options{
		Cluster: cluster.Config{Shards: 3, NodesPerShard: 3},
		Pace:    0, // timing is the renderer's business, not the server's
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		return err
	}

	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/ws", nil)
	if err != nil {
		return err
	}
	defer conn.CloseNow()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)

	// readUntilResult drains frames up to and including the terminating result.
	readUntilResult := func() error {
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return err
			}
			var rec record
			if err := json.Unmarshal(data, &rec); err != nil {
				return err
			}
			if err := enc.Encode(rec); err != nil {
				return err
			}
			if rec.Type == "result" {
				return nil
			}
		}
	}

	// The connect banner is itself a command-shaped exchange.
	if err := readUntilResult(); err != nil {
		return err
	}

	for _, s := range script {
		if s.Say != "" {
			if err := enc.Encode(record{Type: "narr", Msg: s.Say}); err != nil {
				return err
			}
		}
		if err := enc.Encode(record{Type: "cmd", Msg: s.Cmd}); err != nil {
			return err
		}

		payload, err := json.Marshal(map[string]string{"cmd": s.Cmd})
		if err != nil {
			return err
		}
		if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
			return err
		}
		if err := readUntilResult(); err != nil {
			return err
		}
	}

	fmt.Fprintf(os.Stderr, "wrote %s\n", path)
	return conn.Close(websocket.StatusNormalClosure, "")
}
