// Package gateway exposes a running cluster to a browser over WebSocket. It is
// the only package that knows a browser exists: the cluster emits structured
// events through logstream, and the gateway turns those into wire frames.
//
// The wire contract lives in docs/protocol.md.
package gateway

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/tusharpanthri/distributed-banking-system/internal/cluster"
)

// Options configure the gateway.
type Options struct {
	// Cluster is the topology built for each connection.
	Cluster cluster.Config
	// Pace is the delay inserted between log frames. Presentation only; 0
	// disables it. See docs/protocol.md.
	Pace time.Duration
	// MaxSessions bounds concurrent connections. 0 uses the default.
	MaxSessions int
	// Logger receives server-side diagnostics, not cluster events.
	Logger *slog.Logger
}

// Gateway serves /ws and /healthz.
type Gateway struct {
	opts     Options
	logger   *slog.Logger
	sessions atomic.Int64
}

// New returns a Gateway. It does not start listening; use Handler with an
// http.Server.
func New(opts Options) (*Gateway, error) {
	if err := opts.Cluster.Validate(); err != nil {
		return nil, err
	}
	if opts.MaxSessions <= 0 {
		opts.MaxSessions = maxSessions
	}
	if opts.Cluster.MaxDistinctKey <= 0 {
		opts.Cluster.MaxDistinctKey = maxDistinctKeys
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Gateway{opts: opts, logger: opts.Logger}, nil
}

// Handler returns the HTTP routes.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", g.handleHealth)
	mux.HandleFunc("GET /ws", g.handleWS)
	mux.HandleFunc("GET /", g.handleRoot)
	return mux
}

func (g *Gateway) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleRoot answers anything that is not a known route with a short pointer,
// because a bare visit to the backend URL is a thing people will do.
func (g *Gateway) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("distributed transaction control plane\n\n" +
		"this is the backend. connect a websocket to /ws\n" +
		"health check: /healthz\n"))
}

func (g *Gateway) handleWS(w http.ResponseWriter, r *http.Request) {
	if n := g.sessions.Add(1); int(n) > g.opts.MaxSessions {
		g.sessions.Add(-1)
		http.Error(w, "too many active sessions, try again shortly", http.StatusServiceUnavailable)
		return
	}
	defer g.sessions.Add(-1)

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// The frontend is static-hosted on a different origin than the backend,
		// and this endpoint exposes a throwaway in-memory cluster with no user
		// data and no credentials. There is nothing for a cross-origin caller
		// to steal, so any origin is accepted deliberately.
		InsecureSkipVerify: true,
		CompressionMode:    websocket.CompressionDisabled,
	})
	if err != nil {
		g.logger.Warn("websocket handshake failed", "err", err, "remote", r.RemoteAddr)
		return
	}
	conn.SetReadLimit(maxFrameBytes)
	defer func() { _ = conn.CloseNow() }()

	sess, err := newSession(conn, g.opts.Cluster, g.opts.Pace, g.logger)
	if err != nil {
		g.logger.Error("building session cluster", "err", err)
		_ = conn.Close(websocket.StatusInternalError, "could not build cluster")
		return
	}

	g.logger.Info("session opened", "remote", r.RemoteAddr, "active", g.sessions.Load())
	sess.greet(r.Context())
	if err := sess.run(r.Context()); err != nil {
		g.logger.Warn("session ended with error", "err", err)
	}
	g.logger.Info("session closed", "remote", r.RemoteAddr, "dropped_frames", sess.dropped)

	_ = conn.Close(websocket.StatusNormalClosure, "")
}

// Serve runs an HTTP server on addr until ctx is cancelled.
func (g *Gateway) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           g.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: a WebSocket session is long-lived by design and a
		// write deadline would kill it mid-demo. Idle sessions are bounded by
		// idleTimeout in the read loop instead.
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
