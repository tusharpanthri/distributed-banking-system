// Command cluster runs a sharded transaction cluster and the WebSocket gateway
// a browser drives it through.
//
//	go run ./cmd/cluster
//
// then connect a WebSocket to ws://localhost:8080/ws. See docs/protocol.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/tusharpanthri/distributed-banking-system/internal/cluster"
	"github.com/tusharpanthri/distributed-banking-system/internal/gateway"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		addr      = flag.String("addr", "", "listen address (default :$PORT, or :8080)")
		shards    = flag.Int("shards", 3, "number of shards")
		nodes     = flag.Int("nodes", 3, "replicas per shard")
		transport = flag.String("transport", "inproc", "inter-node transport: inproc|grpc")
		pace      = flag.Duration("pace", 120*time.Millisecond, "delay between log frames, presentation only (0 disables)")
		sessions  = flag.Int("max-sessions", 32, "maximum concurrent connections")
		verbose   = flag.Bool("v", false, "debug logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	switch *transport {
	case "inproc":
	case "grpc":
		// Deliberately explicit rather than silently falling back: a deploy
		// that thinks it is running multi-process should fail loudly.
		return errors.New("the grpc transport is not implemented yet; run with -transport=inproc")
	default:
		return fmt.Errorf("unknown transport %q, want inproc or grpc", *transport)
	}

	cfg := cluster.Config{Shards: *shards, NodesPerShard: *nodes}
	if err := cfg.Validate(); err != nil {
		return err
	}

	gw, err := gateway.New(gateway.Options{
		Cluster:     cfg,
		Pace:        *pace,
		MaxSessions: *sessions,
		Logger:      logger,
	})
	if err != nil {
		return err
	}

	listen := resolveAddr(*addr)
	logger.Info("starting",
		"addr", listen,
		"shards", cfg.Shards,
		"nodes_per_shard", cfg.NodesPerShard,
		"quorum", cfg.Quorum(),
		"transport", *transport,
		"pace", *pace,
	)
	logger.Info("connect a websocket", "url", "ws://localhost"+listen+"/ws")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := gw.Serve(ctx, listen); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	logger.Info("stopped")
	return nil
}

// resolveAddr honours -addr, then $PORT, then :8080. Render and Fly both inject
// PORT and expect the process to bind it.
func resolveAddr(addr string) string {
	if addr != "" {
		return addr
	}
	if port := os.Getenv("PORT"); port != "" {
		if _, err := strconv.Atoi(port); err == nil {
			return ":" + port
		}
		return port
	}
	return ":8080"
}
