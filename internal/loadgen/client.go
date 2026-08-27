package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// ClientOptions configures Fire's internal load-testing client.
type ClientOptions struct {
	BaseURL     string // e.g. "http://localhost:8080"
	Concurrency int    // number of concurrent in-flight requests
}

// Result is the aggregated outcome of firing a set of transactions at the
// API's POST /transactions endpoint.
type Result struct {
	Total         int
	Alerts        int
	Duplicates    int
	TotalDuration time.Duration
	P50, P95, P99 time.Duration
	TPS           float64
}

type transactionResponse struct {
	Status     string `json:"status,omitempty"`
	Suspicious bool   `json:"suspicious"`
}

// Fire sends every transaction to POST {BaseURL}/transactions, with
// opts.Concurrency requests in flight at once, and aggregates latency and
// outcome statistics. It stops early if ctx is canceled.
func Fire(ctx context.Context, transactions []domain.Transaction, opts ClientOptions) (Result, error) {
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}

	client := &http.Client{Timeout: 10 * time.Second}
	sem := make(chan struct{}, opts.Concurrency)
	var (
		mu         sync.Mutex
		latencies  []time.Duration
		alerts     int
		duplicates int
		wg         sync.WaitGroup
	)

	started := time.Now()
	for _, tx := range transactions {
		select {
		case <-ctx.Done():
			wg.Wait()
			return Result{}, ctx.Err()
		default:
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(tx domain.Transaction) {
			defer wg.Done()
			defer func() { <-sem }()

			reqStart := time.Now()
			resp, err := sendOne(ctx, client, opts.BaseURL, tx)
			latency := time.Since(reqStart)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				return
			}
			latencies = append(latencies, latency)
			switch {
			case resp.Status == "duplicate":
				duplicates++
			case resp.Suspicious:
				alerts++
			}
		}(tx)
	}
	wg.Wait()
	elapsed := time.Since(started)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(p * float64(len(latencies)-1))
		return latencies[idx]
	}

	tps := 0.0
	if elapsed > 0 {
		tps = float64(len(transactions)) / elapsed.Seconds()
	}

	return Result{
		Total:         len(transactions),
		Alerts:        alerts,
		Duplicates:    duplicates,
		TotalDuration: elapsed,
		P50:           pct(0.50),
		P95:           pct(0.95),
		P99:           pct(0.99),
		TPS:           tps,
	}, nil
}

func sendOne(ctx context.Context, client *http.Client, baseURL string, tx domain.Transaction) (transactionResponse, error) {
	body, err := json.Marshal(tx)
	if err != nil {
		return transactionResponse{}, fmt.Errorf("marshaling transaction: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/transactions", bytes.NewReader(body))
	if err != nil {
		return transactionResponse{}, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return transactionResponse{}, fmt.Errorf("sending request: %w", err)
	}
	defer resp.Body.Close()

	var out transactionResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return transactionResponse{}, fmt.Errorf("decoding response: %w", err)
	}
	return out, nil
}
