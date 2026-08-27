// Command motor is the transaction-detection engine: a fully-mocked,
// ports & adapters application demonstrating idempotency, sliding-window
// rules, degraded mode, hot-reloadable config, and customer-ordered
// concurrency, driven by an HTTP API and a mocked partitioned source.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/adapters/httpapi"
	"github.com/edvargas05/motor-deteccao/internal/adapters/memory"
	"github.com/edvargas05/motor-deteccao/internal/config"
	"github.com/edvargas05/motor-deteccao/internal/domain"
	"github.com/edvargas05/motor-deteccao/internal/engine"
	"github.com/edvargas05/motor-deteccao/internal/pipeline"
	"github.com/edvargas05/motor-deteccao/internal/pipeline/dispatch"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	mode := "serve"
	if len(os.Args) > 1 && os.Args[1][0] != '-' {
		mode = os.Args[1]
		os.Args = append(os.Args[:1], os.Args[2:]...)
	}

	var err error
	switch mode {
	case "serve":
		err = runServe(logger)
	case "demo":
		err = runDemo(logger)
	case "loadtest":
		err = runLoadtest(logger)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q; expected serve|demo|loadtest\n", mode)
		os.Exit(2)
	}

	if err != nil {
		logger.Error("exiting with error", "error", err)
		os.Exit(1)
	}
}

// buildEngine wires every port with its in-memory adapter and returns the
// pieces every mode needs: the Processor, the shared dispatch.Pool, and
// the two admin-toggleable adapters (window, config) the HTTP layer and
// demo script poke directly.
func buildEngine(logger *slog.Logger, workers int) (*pipeline.Processor, *dispatch.Pool, *memory.WindowStore, *memory.ConfigProvider, *memory.AlertSink, error) {
	bundle, err := config.Load(config.DefaultRulesJSON, config.DefaultProfileJSON)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("loading default config: %w", err)
	}

	window := memory.NewWindowStore()
	idem := memory.NewIdempotencyStore()
	risk := memory.NewRiskStore(map[string]domain.RiskProfile{
		"11111111-1111-1111-1111-111111111111": {LimiteValor: 50000, Nivel: "vip"},
	})
	cfg := memory.NewConfigProvider(bundle)
	sink := memory.NewAlertSink(logger)
	evaluator := engine.NewEvaluator(window)
	processor := pipeline.NewProcessor(idem, window, risk, cfg, sink, evaluator)
	pool := dispatch.NewPool(workers)

	return processor, pool, window, cfg, sink, nil
}

func runServe(logger *slog.Logger) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "HTTP listen address")
	workers := fs.Int("workers", 16, "number of dispatch workers")
	_ = fs.Parse(os.Args[1:])

	processor, pool, window, cfg, _, err := buildEngine(logger, *workers)
	if err != nil {
		return err
	}
	defer pool.Close()

	server := httpapi.NewServer(processor, pool, window, cfg, *addr, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("serving", "addr", *addr)
		if err := server.ListenAndServe(); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
