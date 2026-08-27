package pipeline

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/edvargas05/motor-deteccao/internal/adapters/memory"
	"github.com/edvargas05/motor-deteccao/internal/config"
	"github.com/edvargas05/motor-deteccao/internal/domain"
	"github.com/edvargas05/motor-deteccao/internal/engine"
)

func newTestProcessor(t *testing.T) (*Processor, *memory.WindowStore, *memory.RiskStore, *memory.AlertSink) {
	t.Helper()
	bundle, err := config.Load(config.DefaultRulesJSON, config.DefaultProfileJSON)
	require.NoError(t, err)

	idem := memory.NewIdempotencyStore()
	window := memory.NewWindowStore()
	risk := memory.NewRiskStore(map[string]domain.RiskProfile{
		"vip-customer": {LimiteValor: 100000, Nivel: "vip"},
	})
	cfg := memory.NewConfigProvider(bundle)
	sink := memory.NewAlertSink(testLogger())
	evaluator := engine.NewEvaluator(window)

	return NewProcessor(idem, window, risk, cfg, sink, evaluator, testLogger()), window, risk, sink
}

func TestProcessDuplicateTransaction(t *testing.T) {
	p, _, _, sink := newTestProcessor(t)
	ctx := context.Background()
	tx := makeTx("c1", "t1", 100, domain.ChannelCard)

	v1, err := p.Process(ctx, tx)
	require.NoError(t, err)
	assert.False(t, v1.Duplicate)

	v2, err := p.Process(ctx, tx)
	require.NoError(t, err)
	assert.True(t, v2.Duplicate)
	assert.Len(t, sink.Alerts(), 0)
}

func TestProcessValorAtipicoDefaultVsCustomLimit(t *testing.T) {
	p, _, _, _ := newTestProcessor(t)
	ctx := context.Background()

	// Default limite_valor is 5000: 6000 triggers.
	verdictDefault, err := p.Process(ctx, makeTx("plain-customer", "t1", 6000, domain.ChannelCard))
	require.NoError(t, err)
	require.NotNil(t, verdictDefault.Alert)
	assert.Contains(t, verdictDefault.Alert.Categories, "valor")

	// vip-customer has limite_valor 100000: 6000 does not trigger.
	verdictCustom, err := p.Process(ctx, makeTx("vip-customer", "t2", 6000, domain.ChannelCard))
	require.NoError(t, err)
	assert.Nil(t, verdictCustom.Alert)
}

func TestProcessVelocidadePixTriggersAfterThreshold(t *testing.T) {
	p, _, _, _ := newTestProcessor(t)
	ctx := context.Background()

	var last Verdict
	for i := 0; i < 9; i++ {
		v, err := p.Process(ctx, makeTx("burst-customer", txID(i), 100, domain.ChannelPix))
		require.NoError(t, err)
		last = v
	}
	require.NotNil(t, last.Alert, "9th pix transaction should push count over 8 and trigger velocidade-pix")
	assert.Contains(t, last.Alert.Categories, "velocidade")
}

func TestProcessDegradedModeWhenWindowDown(t *testing.T) {
	p, window, _, _ := newTestProcessor(t)
	ctx := context.Background()
	window.SetDown(true)

	// valor-atipico only requires event+customer_risk, so it still fires.
	v, err := p.Process(ctx, makeTx("plain-customer", "t1", 6000, domain.ChannelCard))
	require.NoError(t, err)
	require.NotNil(t, v.Alert)
	assert.Equal(t, domain.EvaluationPartial, v.Alert.Evaluation)
	assert.Equal(t, []string{"window"}, v.Alert.Degraded)

	window.SetDown(false)
	v2, err := p.Process(ctx, makeTx("plain-customer", "t2", 6000, domain.ChannelCard))
	require.NoError(t, err)
	require.NotNil(t, v2.Alert)
	assert.Equal(t, domain.EvaluationComplete, v2.Alert.Evaluation)
}

func TestProcessRiskStoreDownFallsBackToDefault(t *testing.T) {
	p, _, risk, _ := newTestProcessor(t)
	risk.SetDown(true)
	ctx := context.Background()

	// Default limite_valor is 5000: 6000 still triggers via fallback.
	v, err := p.Process(ctx, makeTx("vip-customer", "t1", 6000, domain.ChannelCard))
	require.NoError(t, err)
	require.NotNil(t, v.Alert)
}

func TestProcessAlertIDDeterministic(t *testing.T) {
	p, _, _, _ := newTestProcessor(t)
	ctx := context.Background()
	captured := time.Date(2026, 8, 24, 14, 3, 11, 0, time.UTC)

	tx1 := domain.Transaction{CustomerID: "c1", TransactionID: "t1", Amount: 6000, Currency: "BRL", Channel: domain.ChannelCard, CapturedAt: captured}
	v1, err := p.Process(ctx, tx1)
	require.NoError(t, err)
	require.NotNil(t, v1.Alert)

	// Same inputs on a fresh processor instance -> same alert_id.
	p2, _, _, _ := newTestProcessor(t)
	v2, err := p2.Process(ctx, tx1)
	require.NoError(t, err)
	require.NotNil(t, v2.Alert)
	assert.Equal(t, v1.Alert.AlertID, v2.Alert.AlertID)

	tx3 := tx1
	tx3.TransactionID = "different"
	p3, _, _, _ := newTestProcessor(t)
	v3, err := p3.Process(ctx, tx3)
	require.NoError(t, err)
	require.NotNil(t, v3.Alert)
	assert.NotEqual(t, v1.Alert.AlertID, v3.Alert.AlertID)
}

func TestProcessAggregatesMultipleTriggeredRules(t *testing.T) {
	p, _, _, _ := newTestProcessor(t)
	ctx := context.Background()

	var last Verdict
	for i := 0; i < 9; i++ {
		// High amount (6000 > default 5000) AND pix bursts -> both rules fire on the last one.
		v, err := p.Process(ctx, makeTx("multi-customer", txID(i), 6000, domain.ChannelPix))
		require.NoError(t, err)
		last = v
	}
	require.NotNil(t, last.Alert)
	assert.Equal(t, domain.SeverityAlta, last.Alert.Severity, "max severity across velocidade-pix (alta) and valor-atipico (media)")
	assert.ElementsMatch(t, []string{"velocidade", "valor"}, last.Alert.Categories)
	assert.Len(t, last.Alert.TriggeredRules, 2)
}

// --- test helpers ---

func makeTx(customerID, txID string, amount float64, channel domain.Channel) domain.Transaction {
	return domain.Transaction{
		CustomerID:    customerID,
		TransactionID: txID,
		Amount:        amount,
		Currency:      "BRL",
		Channel:       channel,
		DeviceID:      "device-1",
		CapturedAt:    time.Now(),
	}
}

func txID(i int) string {
	return "t" + string(rune('a'+i))
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
