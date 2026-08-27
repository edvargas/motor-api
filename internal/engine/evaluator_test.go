package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// fakeWindowStore is a minimal in-test stub satisfying ports.WindowStore.
type fakeWindowStore struct {
	counts map[domain.Channel]int
}

func (f *fakeWindowStore) Record(ctx context.Context, customerID string, tx domain.Transaction, ttl time.Duration) error {
	return nil
}

func (f *fakeWindowStore) CountByChannel(ctx context.Context, customerID string, channel domain.Channel, span time.Duration) (int, error) {
	return f.counts[channel], nil
}

func TestEvaluateTriggersVelocidadePix(t *testing.T) {
	rule := domain.RuleDef{
		RuleID: "velocidade-pix", Version: 1, Enabled: true,
		Requires:  []string{"event", "window"},
		Window:    &domain.WindowSpec{SpanSeconds: 300, Type: domain.WindowTypeCount},
		Condition: `channel == "pix" && window.count_channel_pix > 8`,
		Emits:     domain.RuleEmits{Severity: domain.SeverityAlta, Category: "velocidade"},
	}
	store := &fakeWindowStore{counts: map[domain.Channel]int{domain.ChannelPix: 9}}
	ev := NewEvaluator(store)

	tx := domain.Transaction{CustomerID: "c1", TransactionID: "t1", Amount: 100, Currency: "BRL", Channel: domain.ChannelPix, CapturedAt: time.Now()}

	res, err := ev.Evaluate(context.Background(), []domain.RuleDef{rule}, tx, domain.RiskProfile{}, true)
	require.NoError(t, err)
	assert.Len(t, res.TriggeredRules, 1)
	assert.Equal(t, domain.SeverityAlta, res.Severity)
	assert.Contains(t, res.Categories, "velocidade")
}

func TestEvaluateSkipsWindowRuleWhenUnavailable(t *testing.T) {
	rule := domain.RuleDef{
		RuleID: "velocidade-pix", Version: 1, Enabled: true,
		Requires:  []string{"event", "window"},
		Window:    &domain.WindowSpec{SpanSeconds: 300, Type: domain.WindowTypeCount},
		Condition: `channel == "pix" && window.count_channel_pix > 8`,
		Emits:     domain.RuleEmits{Severity: domain.SeverityAlta, Category: "velocidade"},
	}
	store := &fakeWindowStore{counts: map[domain.Channel]int{domain.ChannelPix: 100}}
	ev := NewEvaluator(store)
	tx := domain.Transaction{CustomerID: "c1", TransactionID: "t1", Amount: 100, Currency: "BRL", Channel: domain.ChannelPix, CapturedAt: time.Now()}

	res, err := ev.Evaluate(context.Background(), []domain.RuleDef{rule}, tx, domain.RiskProfile{}, false)
	require.NoError(t, err)
	assert.Empty(t, res.TriggeredRules)
}

func TestEvaluateValorAtipicoDefaultVsCustomLimit(t *testing.T) {
	rule := domain.RuleDef{
		RuleID: "valor-atipico", Version: 1, Enabled: true,
		Requires:  []string{"event", "customer_risk"},
		Condition: "amount > risk.limite_valor",
		Emits:     domain.RuleEmits{Severity: domain.SeverityMedia, Category: "valor"},
	}
	ev := NewEvaluator(&fakeWindowStore{})
	tx := domain.Transaction{CustomerID: "c1", TransactionID: "t1", Amount: 6000, Currency: "BRL", Channel: domain.ChannelCard, CapturedAt: time.Now()}

	resDefault, err := ev.Evaluate(context.Background(), []domain.RuleDef{rule}, tx, domain.RiskProfile{LimiteValor: 5000}, true)
	require.NoError(t, err)
	assert.Len(t, resDefault.TriggeredRules, 1, "6000 > default 5000 should trigger")

	resCustom, err := ev.Evaluate(context.Background(), []domain.RuleDef{rule}, tx, domain.RiskProfile{LimiteValor: 10000}, true)
	require.NoError(t, err)
	assert.Empty(t, resCustom.TriggeredRules, "6000 < custom 10000 should not trigger")
}

func TestScoreAggregatesMaxAndMean(t *testing.T) {
	s := Score([]domain.Severity{domain.SeverityAlta, domain.SeverityMedia})
	// weight(alta)=0.75, mean(0.75,0.5)=0.625 -> 0.75*0.7 + 0.625*0.3 = 0.7125
	assert.InDelta(t, 0.7125, s, 0.0001)
}
