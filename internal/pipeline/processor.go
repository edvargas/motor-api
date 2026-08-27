// Package pipeline hosts the Processor: the single place idempotency,
// window/risk layer resolution, rule evaluation, decision, and alert
// emission happen. Every inbound adapter (HTTP API, mocked source) routes
// through the same Processor via a dispatch.Pool — no detection logic is
// duplicated anywhere else.
package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/domain"
	"github.com/edvargas05/motor-deteccao/internal/engine"
	"github.com/edvargas05/motor-deteccao/internal/ports"
)

const (
	idempotencyTTL    = 24 * time.Hour
	windowTTL         = 10 * time.Minute
	defaultBucketSpan = 60 * time.Second
)

// Verdict is what Process returns for one transaction.
type Verdict struct {
	Duplicate bool
	Alert     *domain.Alert // nil when the transaction was not suspicious
	Partial   bool          // true if any required layer was degraded, alert or not
	Degraded  []string
}

// Processor is the single detection core. Construct one per running engine
// and share it (it is safe for concurrent use, since every port it holds
// is itself safe for concurrent use) — callers are expected to still
// route same-customer transactions through a dispatch.Pool to preserve
// ordering, since Process on its own does not serialize by customer.
type Processor struct {
	idempotency ports.IdempotencyStore
	window      ports.WindowStore
	risk        ports.RiskStore
	cfg         ports.ConfigProvider
	sink        ports.AlertSink
	evaluator   *engine.Evaluator
}

// NewProcessor wires the Processor's dependencies.
func NewProcessor(idempotency ports.IdempotencyStore, window ports.WindowStore, risk ports.RiskStore, cfg ports.ConfigProvider, sink ports.AlertSink, evaluator *engine.Evaluator) *Processor {
	return &Processor{
		idempotency: idempotency,
		window:      window,
		risk:        risk,
		cfg:         cfg,
		sink:        sink,
		evaluator:   evaluator,
	}
}

// Process runs tx through idempotency, layer resolution, rule evaluation,
// decision, and (if suspicious) emission.
func (p *Processor) Process(ctx context.Context, tx domain.Transaction) (Verdict, error) {
	if err := tx.Validate(); err != nil {
		return Verdict{}, err
	}

	key := tx.IdempotencyKey()
	seen, err := p.idempotency.Seen(ctx, key)
	if err != nil {
		return Verdict{}, fmt.Errorf("checking idempotency: %w", err)
	}
	if seen {
		return Verdict{Duplicate: true}, nil
	}

	windowAvailable := true
	if err := p.window.Record(ctx, tx.CustomerID, tx, windowTTL); err != nil {
		if errors.Is(err, domain.ErrEnrichmentUnavailable) {
			windowAvailable = false
		} else {
			return Verdict{}, fmt.Errorf("recording window: %w", err)
		}
	}

	ruleSet := p.cfg.Current()
	risk := p.resolveRisk(ctx, tx.CustomerID, ruleSet.Profile)

	result, err := p.evaluator.Evaluate(ctx, ruleSet.Rules, tx, risk, windowAvailable)
	if err != nil {
		return Verdict{}, fmt.Errorf("evaluating rules: %w", err)
	}

	var degraded []string
	if !windowAvailable {
		degraded = []string{"window"}
	}
	partial := len(degraded) > 0

	if result.Score < ruleSet.Profile.Thresholds.ScoreMinimoAlerta || len(result.TriggeredRules) == 0 {
		// Mark idempotency only after we've fully committed to this
		// no-alert verdict, so a crash mid-evaluation does not
		// silently swallow a retry.
		if err := p.idempotency.Mark(ctx, key, idempotencyTTL); err != nil {
			return Verdict{}, fmt.Errorf("marking idempotency: %w", err)
		}
		return Verdict{Partial: partial, Degraded: degraded}, nil
	}

	evaluation := domain.EvaluationComplete
	if partial {
		evaluation = domain.EvaluationPartial
	}

	alert := domain.Alert{
		AlertID:        alertID(tx, result, ruleSet.Rules),
		CustomerID:     tx.CustomerID,
		TransactionID:  tx.TransactionID,
		Severity:       result.Severity,
		Categories:     result.Categories,
		Score:          result.Score,
		TriggeredRules: result.TriggeredRules,
		Evaluation:     evaluation,
		Degraded:       degraded,
	}

	if err := p.sink.Publish(ctx, alert); err != nil {
		// Do not mark idempotency: publish failed, so a retry of this
		// transaction must re-evaluate and re-attempt publish rather
		// than short-circuiting as a duplicate.
		return Verdict{}, fmt.Errorf("publishing alert: %w", err)
	}

	// Mark idempotency only after the alert has been durably published.
	if err := p.idempotency.Mark(ctx, key, idempotencyTTL); err != nil {
		return Verdict{}, fmt.Errorf("marking idempotency: %w", err)
	}

	return Verdict{Alert: &alert, Partial: partial, Degraded: degraded}, nil
}

// resolveRisk returns the customer's personalized profile, falling back to
// the global default when the store is down or has no profile for them.
func (p *Processor) resolveRisk(ctx context.Context, customerID string, profile domain.OperationalProfile) domain.RiskProfile {
	got, err := p.risk.Get(ctx, customerID)
	if err != nil {
		return profile.DefaultCustomerRisk
	}
	return *got
}

// alertID computes the deterministic SHA-256 alert_id: sha256_hex of
// "v1|customer_id|transaction_id|window_bucket". window_bucket is derived
// from the span of the highest-severity triggered rule that declares a
// window; transactions with no windowed trigger use a fixed default span,
// so alert_id stays fully deterministic either way.
func alertID(tx domain.Transaction, result engine.Result, rules []domain.RuleDef) string {
	span := defaultBucketSpan
	if r, ok := ruleForHighestSeverity(result, rules); ok && r.Window != nil {
		span = time.Duration(r.Window.SpanSeconds) * time.Second
	}

	bucketStart := tx.CapturedAt.Unix() / int64(span.Seconds()) * int64(span.Seconds())
	bucket := time.Unix(bucketStart, 0).UTC().Format(time.RFC3339)

	raw := fmt.Sprintf("v1|%s|%s|%s", tx.CustomerID, tx.TransactionID, bucket)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// ruleForHighestSeverity finds, among the rules that triggered, the
// domain.RuleDef of the one with the highest emitted severity.
func ruleForHighestSeverity(result engine.Result, rules []domain.RuleDef) (domain.RuleDef, bool) {
	byID := make(map[string]domain.RuleDef, len(rules))
	for _, r := range rules {
		byID[r.RuleID] = r
	}

	var best domain.RuleDef
	found := false
	for _, ref := range result.TriggeredRules {
		r, ok := byID[ref.RuleID]
		if !ok {
			continue
		}
		if !found || domain.MaxSeverity(r.Emits.Severity, best.Emits.Severity) == r.Emits.Severity {
			best = r
			found = true
		}
	}
	return best, found
}
