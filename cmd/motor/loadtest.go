package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/adapters/httpapi"
	"github.com/edvargas05/motor-deteccao/internal/domain"
	"github.com/edvargas05/motor-deteccao/internal/loadgen"
)

// runLoadtest generates a synthetic transaction preset, starts a
// throwaway API instance, fires the load at it with a configurable
// internal client, and prints TPS/p50/p95/p99. Results reflect this local
// process and its in-memory mocks, not real infrastructure.
func runLoadtest(logger *slog.Logger) error {
	fs := flag.NewFlagSet("loadtest", flag.ExitOnError)
	numTx := fs.Int("n", 100_000, "number of synthetic transactions to generate")
	numCustomers := fs.Int("customers", 5_000, "number of distinct customers")
	seed := fs.Int64("seed", 0, "seed for reproducible generation (0 = random)")
	concurrency := fs.Int("concurrency", 64, "concurrent in-flight HTTP requests")
	workers := fs.Int("workers", 16, "number of dispatch workers in the target server")
	addr := fs.String("addr", ":18080", "address for the throwaway server this mode starts")
	dumpPath := fs.String("dump", "", "if set, also write the generated transactions as .jsonl to this path")
	_ = fs.Parse(os.Args[1:])

	fmt.Printf("generating %d transactions across %d customers (seed=%d)...\n", *numTx, *numCustomers, *seed)
	txs := loadgen.Generate(loadgen.Options{NumTransactions: *numTx, NumCustomers: *numCustomers, Seed: *seed})

	if *dumpPath != "" {
		if err := dumpJSONL(*dumpPath, txs); err != nil {
			return fmt.Errorf("dumping jsonl: %w", err)
		}
		fmt.Printf("wrote %d transactions to %s\n", len(txs), *dumpPath)
	}

	processor, pool, _, _, sink, err := buildEngine(logger, *workers)
	if err != nil {
		return err
	}
	defer pool.Close()

	server := httpapi.NewServer(processor, pool, nil, nil, *addr, logger)
	go func() {
		_ = server.ListenAndServe()
	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	time.Sleep(200 * time.Millisecond) // let the listener come up

	fmt.Printf("firing load at http://localhost%s with concurrency=%d...\n", *addr, *concurrency)
	result, err := loadgen.Fire(context.Background(), txs, loadgen.ClientOptions{
		BaseURL:     "http://localhost" + *addr,
		Concurrency: *concurrency,
	})
	if err != nil {
		return fmt.Errorf("firing load: %w", err)
	}

	fmt.Printf(`
=== loadtest results (local process, in-memory mocks) ===
total:       %d
alerts:      %d
duplicates:  %d
duration:    %s
throughput:  %.1f TPS observed
p50:         %s
p95:         %s
p99:         %s
alerts emitted (sink): %d
`, result.Total, result.Alerts, result.Duplicates, result.TotalDuration,
		result.TPS, result.P50, result.P95, result.P99, len(sink.Alerts()))

	return nil
}

func dumpJSONL(path string, txs []domain.Transaction) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, tx := range txs {
		if err := enc.Encode(tx); err != nil {
			return err
		}
	}
	return nil
}
