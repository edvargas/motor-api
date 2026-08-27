package httpapi

import (
	"sort"
	"sync"
	"time"
)

// Metrics accumulates the counters and latencies the /metrics endpoint
// reports, and that /transactions/batch summarizes per-batch.
type Metrics struct {
	mu         sync.Mutex
	processed  int
	duplicates int
	alerts     int
	partials   int
	byCategory map[string]int
	latencies  []time.Duration
}

// NewMetrics builds an empty Metrics.
func NewMetrics() *Metrics {
	return &Metrics{byCategory: make(map[string]int)}
}

// RecordDuplicate accounts for a duplicate verdict.
func (m *Metrics) RecordDuplicate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processed++
	m.duplicates++
}

// RecordProcessed accounts for a non-duplicate verdict: latency, whether it
// was partial, whether it produced an alert, and which categories.
func (m *Metrics) RecordProcessed(latency time.Duration, partial bool, categories []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processed++
	m.latencies = append(m.latencies, latency)
	if partial {
		m.partials++
	}
	if len(categories) > 0 {
		m.alerts++
		for _, c := range categories {
			m.byCategory[c]++
		}
	}
}

// Snapshot is the JSON-serializable view of Metrics at a point in time.
type Snapshot struct {
	Processed  int            `json:"processed"`
	Duplicates int            `json:"duplicates"`
	Alerts     int            `json:"alerts"`
	Partials   int            `json:"partials"`
	ByCategory map[string]int `json:"by_category"`
	LatencyMS  LatencyStats   `json:"latency_ms"`
}

// LatencyStats summarizes a set of latencies in milliseconds.
type LatencyStats struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

// Snapshot returns a point-in-time copy of the accumulated metrics.
func (m *Metrics) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	byCategory := make(map[string]int, len(m.byCategory))
	for k, v := range m.byCategory {
		byCategory[k] = v
	}

	return Snapshot{
		Processed:  m.processed,
		Duplicates: m.duplicates,
		Alerts:     m.alerts,
		Partials:   m.partials,
		ByCategory: byCategory,
		LatencyMS:  latencyStats(m.latencies),
	}
}

func latencyStats(latencies []time.Duration) LatencyStats {
	if len(latencies) == 0 {
		return LatencyStats{}
	}
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	pct := func(p float64) float64 {
		idx := int(p * float64(len(sorted)-1))
		return float64(sorted[idx]) / float64(time.Millisecond)
	}
	return LatencyStats{P50: pct(0.50), P95: pct(0.95), P99: pct(0.99)}
}
