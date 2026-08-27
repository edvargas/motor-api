// Package httpapi is a thin net/http adapter: it validates and deserializes
// requests, routes them through the shared dispatch.Pool into
// pipeline.Processor, and serializes the verdict. No detection logic
// lives here.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/adapters/memory"
	"github.com/edvargas05/motor-deteccao/internal/pipeline"
	"github.com/edvargas05/motor-deteccao/internal/pipeline/dispatch"
)

// Server wires the HTTP mux and an underlying http.Server.
type Server struct {
	httpServer *http.Server
	metrics    *Metrics
}

// NewServer builds a Server that routes requests through processor via
// pool, and exposes admin toggles on window and cfg.
func NewServer(processor *pipeline.Processor, pool *dispatch.Pool, window *memory.WindowStore, cfg *memory.ConfigProvider, addr string, logger *slog.Logger) *Server {
	h := &handler{
		processor: processor,
		pool:      pool,
		window:    window,
		cfg:       cfg,
		metrics:   NewMetrics(),
		logger:    logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /transactions", h.postTransaction)
	mux.HandleFunc("POST /transactions/batch", h.postTransactionBatch)
	mux.HandleFunc("GET /healthz", h.getHealthz)
	mux.HandleFunc("GET /metrics", h.getMetrics)
	mux.HandleFunc("POST /admin/window/down", h.postWindowDown)
	mux.HandleFunc("POST /admin/window/up", h.postWindowUp)
	mux.HandleFunc("POST /admin/config/reload", h.postConfigReload)

	return &Server{
		metrics: h.metrics,
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      30 * time.Second,
			MaxHeaderBytes:    1 << 16,
		},
	}
}

// Handler exposes the underlying http.Handler, mainly for httptest use.
func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler
}

// Metrics exposes the Server's Metrics collector.
func (s *Server) Metrics() *Metrics {
	return s.metrics
}

// ListenAndServe starts serving and blocks until the server stops.
func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully drains in-flight requests within ctx's deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
