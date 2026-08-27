package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/edvargas05/motor-deteccao/internal/adapters/memory"
	"github.com/edvargas05/motor-deteccao/internal/config"
	"github.com/edvargas05/motor-deteccao/internal/engine"
	"github.com/edvargas05/motor-deteccao/internal/pipeline"
	"github.com/edvargas05/motor-deteccao/internal/pipeline/dispatch"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	bundle, err := config.Load(config.DefaultRulesJSON, config.DefaultProfileJSON)
	require.NoError(t, err)

	window := memory.NewWindowStore()
	idem := memory.NewIdempotencyStore()
	risk := memory.NewRiskStore(nil)
	cfg := memory.NewConfigProvider(bundle)
	sink := memory.NewAlertSink(slog.New(slog.NewTextHandler(io.Discard, nil)))
	evaluator := engine.NewEvaluator(window)
	processor := pipeline.NewProcessor(idem, window, risk, cfg, sink, evaluator, slog.New(slog.NewTextHandler(io.Discard, nil)))
	pool := dispatch.NewPool(4)
	t.Cleanup(pool.Close)

	return NewServer(processor, pool, window, cfg, ":0", slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestPostTransactionReturnsVerdict(t *testing.T) {
	s := newTestServer(t)
	body := `{"customer_id":"c1","transaction_id":"t1","amount":6000,"currency":"BRL","channel":"card","device_id":"d1","captured_at":"2026-08-24T14:03:11Z"}`

	req := httptest.NewRequest(http.MethodPost, "/transactions", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp transactionResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Suspicious, "6000 > default limite_valor 5000 should be suspicious")
	require.NotNil(t, resp.Alert)
	assert.Contains(t, resp.Alert.Categories, "valor")
}

func TestPostTransactionInvalidPayload(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/transactions", bytes.NewBufferString(`not json`))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPostTransactionBatchAggregatesSummary(t *testing.T) {
	s := newTestServer(t)
	txs := []map[string]any{
		{"customer_id": "c1", "transaction_id": "t1", "amount": 100, "currency": "BRL", "channel": "card", "device_id": "d1", "captured_at": "2026-08-24T14:03:11Z"},
		{"customer_id": "c1", "transaction_id": "t1", "amount": 100, "currency": "BRL", "channel": "card", "device_id": "d1", "captured_at": "2026-08-24T14:03:11Z"},  // duplicate
		{"customer_id": "c2", "transaction_id": "t2", "amount": 6000, "currency": "BRL", "channel": "card", "device_id": "d1", "captured_at": "2026-08-24T14:03:11Z"}, // alert
	}
	payload, err := json.Marshal(txs)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/transactions/batch", bytes.NewBuffer(payload))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var summary batchSummary
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &summary))
	assert.Equal(t, 3, summary.Total)
	assert.Equal(t, 1, summary.Duplicates)
	assert.Equal(t, 1, summary.Alerts)
}

func TestHealthzAndMetrics(t *testing.T) {
	s := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAdminWindowToggleAndConfigReload(t *testing.T) {
	s := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/admin/window/down", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/admin/window/up", nil)
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/admin/config/reload", nil)
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}
