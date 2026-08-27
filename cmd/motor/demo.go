package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/config"
	"github.com/edvargas05/motor-deteccao/internal/domain"
	"github.com/edvargas05/motor-deteccao/internal/pipeline"
	"github.com/edvargas05/motor-deteccao/internal/pipeline/dispatch"
)

// runDemo executes a scripted walkthrough of every capability, printing
// each verdict so it's presentable via `go run ./cmd/motor demo`.
func runDemo(logger *slog.Logger) error {
	processor, pool, window, cfg, _, err := buildEngine(logger, 4)
	if err != nil {
		return err
	}
	defer pool.Close()
	ctx := context.Background()

	printSection := func(title string) { fmt.Printf("\n=== %s ===\n", title) }
	// show routes each transaction through the shared dispatch.Pool, like
	// every other inbound path — the demo must never call Processor with
	// its own ad-hoc goroutine scheme.
	show := func(label string, tx domain.Transaction) {
		v, err := processViaPool(ctx, pool, processor, tx)
		if err != nil {
			fmt.Printf("%s: ERROR %v\n", label, err)
			return
		}
		switch {
		case v.Duplicate:
			fmt.Printf("%s: duplicate, discarded\n", label)
		case v.Alert != nil:
			b, _ := json.MarshalIndent(v.Alert, "", "  ")
			fmt.Printf("%s: ALERT\n%s\n", label, b)
		default:
			fmt.Printf("%s: not suspicious\n", label)
		}
	}

	printSection("customer_risk: default vs. personalized limit")
	base := time.Date(2026, 8, 24, 14, 0, 0, 0, time.UTC)
	show("high-value tx, default risk profile", domain.Transaction{
		CustomerID: "plain-customer", TransactionID: "demo-1", Amount: 6000,
		Currency: "BRL", Channel: domain.ChannelCard, DeviceID: "d1", CapturedAt: base,
	})
	show("same amount, personalized (vip) limit", domain.Transaction{
		CustomerID: "11111111-1111-1111-1111-111111111111", TransactionID: "demo-2", Amount: 6000,
		Currency: "BRL", Channel: domain.ChannelCard, DeviceID: "d1", CapturedAt: base,
	})

	printSection("window: pix burst triggers velocidade-pix")
	for i := 0; i < 9; i++ {
		show(fmt.Sprintf("pix burst #%d", i+1), domain.Transaction{
			CustomerID: "burst-customer", TransactionID: fmt.Sprintf("demo-burst-%d", i), Amount: 150,
			Currency: "BRL", Channel: domain.ChannelPix, DeviceID: "d2", CapturedAt: base.Add(time.Duration(i) * time.Second),
		})
	}

	printSection("idempotency: duplicate transaction")
	dupTx := domain.Transaction{
		CustomerID: "dup-customer", TransactionID: "demo-dup", Amount: 100,
		Currency: "BRL", Channel: domain.ChannelCard, DeviceID: "d3", CapturedAt: base,
	}
	show("first send", dupTx)
	show("second send (duplicate)", dupTx)

	printSection("degraded mode: WindowStore down, then recovered")
	window.SetDown(true)
	show("window down + high-value tx", domain.Transaction{
		CustomerID: "plain-customer", TransactionID: "demo-degraded", Amount: 6000,
		Currency: "BRL", Channel: domain.ChannelCard, DeviceID: "d1", CapturedAt: base,
	})
	window.SetDown(false)
	show("window back up + high-value tx", domain.Transaction{
		CustomerID: "plain-customer", TransactionID: "demo-recovered", Amount: 6000,
		Currency: "BRL", Channel: domain.ChannelCard, DeviceID: "d1", CapturedAt: base,
	})

	printSection("hot reload: enabling a new rule without redeploy")
	bundle, err := config.Load(config.DefaultRulesJSON, config.DefaultProfileJSON)
	if err != nil {
		return err
	}
	for i := range bundle.Rules {
		if bundle.Rules[i].RuleID == "valor-critico" {
			bundle.Rules[i].Enabled = true
		}
	}
	cfg.SetOverride(bundle)
	if err := cfg.Reload(ctx); err != nil {
		return err
	}
	show("very-high-value tx, now caught by newly enabled valor-critico", domain.Transaction{
		CustomerID: "plain-customer", TransactionID: "demo-hotreload", Amount: 30000,
		Currency: "BRL", Channel: domain.ChannelCard, DeviceID: "d1", CapturedAt: base,
	})

	fmt.Println("\ndemo complete")
	return nil
}

// processViaPool submits tx to pool and waits synchronously for the
// result — the same shape httpapi's processOne uses for its single-item
// HTTP path, reused here so the demo genuinely goes through the shared
// dispatcher instead of calling Processor directly.
func processViaPool(ctx context.Context, pool *dispatch.Pool, processor *pipeline.Processor, tx domain.Transaction) (pipeline.Verdict, error) {
	type outcome struct {
		verdict pipeline.Verdict
		err     error
	}
	done := make(chan outcome, 1)

	enqueued := pool.Submit(ctx, dispatch.Job{
		CustomerID: tx.CustomerID,
		Run: func(jobCtx context.Context) {
			v, err := processor.Process(ctx, tx)
			done <- outcome{verdict: v, err: err}
		},
	})
	if !enqueued {
		return pipeline.Verdict{}, ctx.Err()
	}

	select {
	case o := <-done:
		return o.verdict, o.err
	case <-ctx.Done():
		return pipeline.Verdict{}, ctx.Err()
	}
}
