package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/adapters/memory"
	"github.com/edvargas05/motor-deteccao/internal/config"
	"github.com/edvargas05/motor-deteccao/internal/domain"
	"github.com/edvargas05/motor-deteccao/internal/pipeline"
	"github.com/edvargas05/motor-deteccao/internal/pipeline/dispatch"
)

const maxBodyBytes = 1 << 20 // 1 MiB

type handler struct {
	processor *pipeline.Processor
	pool      *dispatch.Pool
	window    *memory.WindowStore
	cfg       *memory.ConfigProvider
	metrics   *Metrics
	logger    *slog.Logger
}

// transactionResponse mirrors what POST /transactions and each item of a
// batch summary can return.
type transactionResponse struct {
	Status     string        `json:"status,omitempty"` // "duplicate", when applicable
	Suspicious bool          `json:"suspicious"`
	Alert      *domain.Alert `json:"alert,omitempty"`
}

func (h *handler) postTransaction(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	var tx domain.Transaction
	if err := json.NewDecoder(r.Body).Decode(&tx); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	verdict, err := h.processOne(r.Context(), tx)
	if err != nil {
		writeProcessError(w, r, err)
		return
	}

	if verdict.Duplicate {
		writeJSON(w, http.StatusOK, transactionResponse{Status: "duplicate"})
		return
	}
	if verdict.Alert != nil {
		writeJSON(w, http.StatusOK, transactionResponse{Suspicious: true, Alert: verdict.Alert})
		return
	}
	writeJSON(w, http.StatusOK, transactionResponse{Suspicious: false})
}

// processOne routes a single transaction through the shared dispatch.Pool
// and waits synchronously for its result — this is the "API síncrona"
// entry point the spec requires: it never talks to Processor directly.
func (h *handler) processOne(ctx context.Context, tx domain.Transaction) (pipeline.Verdict, error) {
	type outcome struct {
		verdict pipeline.Verdict
		err     error
	}
	done := make(chan outcome, 1)
	start := time.Now()

	h.pool.Submit(ctx, dispatch.Job{
		CustomerID: tx.CustomerID,
		Run: func(jobCtx context.Context) {
			v, err := h.processor.Process(ctx, tx)
			done <- outcome{verdict: v, err: err}
		},
	})

	select {
	case o := <-done:
		if o.err == nil {
			h.record(o.verdict, time.Since(start))
		}
		return o.verdict, o.err
	case <-ctx.Done():
		return pipeline.Verdict{}, ctx.Err()
	}
}

func (h *handler) record(v pipeline.Verdict, latency time.Duration) {
	if v.Duplicate {
		h.metrics.RecordDuplicate()
		return
	}
	var categories []string
	if v.Alert != nil {
		categories = v.Alert.Categories
	}
	h.metrics.RecordProcessed(latency, v.Partial, categories)
}

type batchSummary struct {
	Total      int          `json:"total"`
	Alerts     int          `json:"alerts"`
	Duplicates int          `json:"duplicates"`
	Partials   int          `json:"partials"`
	LatencyMS  LatencyStats `json:"latency_ms"`
	TPS        float64      `json:"tps"`
}

func (h *handler) postTransactionBatch(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes*10)

	var txs []domain.Transaction
	if err := json.NewDecoder(r.Body).Decode(&txs); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	started := time.Now()
	var (
		mu                            sync.Mutex
		total, alerts, dups, partials int
		latencies                     []time.Duration
		wg                            sync.WaitGroup
	)

	// Repartition by customer_id, not by array index: the dispatch.Pool
	// already guarantees same-customer jobs run in submission order on the
	// same worker, so submitting in array order here is sufficient — two
	// events for the same customer never race each other.
	for _, tx := range txs {
		wg.Add(1)
		tx := tx
		txStart := time.Now()

		enqueued := h.pool.Submit(r.Context(), dispatch.Job{
			CustomerID: tx.CustomerID,
			Run: func(jobCtx context.Context) {
				defer wg.Done()
				v, err := h.processor.Process(r.Context(), tx)
				if err != nil {
					h.logger.ErrorContext(r.Context(), "batch item processing failed",
						"customer_id", tx.CustomerID,
						"transaction_id", tx.TransactionID,
						"error", err,
					)
					return
				}
				itemLatency := time.Since(txStart)
				h.record(v, itemLatency) // batch traffic must also count toward the server-wide /metrics snapshot

				mu.Lock()
				defer mu.Unlock()
				total++
				latencies = append(latencies, itemLatency)
				if v.Duplicate {
					dups++
					return
				}
				if v.Partial {
					partials++
				}
				if v.Alert != nil {
					alerts++
				}
			},
		})

		// Submit's return value is ground truth: true means the job was
		// enqueued and its Run closure (above) will call wg.Done() itself
		// once it finishes. false means the job was dropped and will never
		// run, so we must call wg.Done() here ourselves or wg.Wait() below
		// would hang forever.
		if !enqueued {
			wg.Done()
		}
	}
	wg.Wait()

	elapsed := time.Since(started)
	tps := 0.0
	if elapsed > 0 {
		tps = float64(total) / elapsed.Seconds()
	}

	writeJSON(w, http.StatusOK, batchSummary{
		Total:      total,
		Alerts:     alerts,
		Duplicates: dups,
		Partials:   partials,
		LatencyMS:  latencyStats(latencies),
		TPS:        tps,
	})
}

func (h *handler) getHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *handler) getMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.metrics.Snapshot())
}

func (h *handler) postWindowDown(w http.ResponseWriter, r *http.Request) {
	if h.window == nil {
		writeError(w, http.StatusServiceUnavailable, "window admin toggle not available in this mode")
		return
	}
	h.window.SetDown(true)
	writeJSON(w, http.StatusOK, map[string]string{"window": "down"})
}

func (h *handler) postWindowUp(w http.ResponseWriter, r *http.Request) {
	if h.window == nil {
		writeError(w, http.StatusServiceUnavailable, "window admin toggle not available in this mode")
		return
	}
	h.window.SetDown(false)
	writeJSON(w, http.StatusOK, map[string]string{"window": "up"})
}

// reloadRequest is the optional body for POST /admin/config/reload: new
// rules plus an optional profile (defaulting to the currently active one
// when omitted, so a caller can push just a rule change).
type reloadRequest struct {
	Rules   []domain.RuleDef           `json:"rules"`
	Profile *domain.OperationalProfile `json:"profile,omitempty"`
}

func (h *handler) postConfigReload(w http.ResponseWriter, r *http.Request) {
	if h.cfg == nil {
		writeError(w, http.StatusServiceUnavailable, "config admin toggle not available in this mode")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if r.ContentLength != 0 {
		var req reloadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON payload")
			return
		}
		profile := h.cfg.Current().Profile
		if req.Profile != nil {
			profile = *req.Profile
		}
		bundle, err := config.Load(mustMarshalJSON(req.Rules), mustMarshalJSON(profile))
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		h.cfg.SetOverride(bundle)
	}
	// With no body, this reloads whatever was already staged (e.g. by the
	// demo script's SetOverride) — a no-op if nothing is staged.

	if err := h.cfg.Reload(r.Context()); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"version": h.cfg.Current().Version})
}

func mustMarshalJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("marshal %T: %v", v, err)) // in-memory struct, never fails
	}
	return b
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// writeProcessError maps a Process error to the right HTTP status: a
// structurally invalid transaction is the caller's fault (400); a client
// that disconnected/canceled gets no useful response to write; anything
// else (a store error, a publish failure) is this server's fault (500),
// not "your payload is bad."
func writeProcessError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalidTransaction):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// Client disconnected or the request deadline passed — nobody is
		// listening for a response, and net/http will surface the broken
		// connection on its own; avoid writing a misleading 500.
	default:
		writeError(w, http.StatusInternalServerError, "internal error processing transaction")
	}
}
