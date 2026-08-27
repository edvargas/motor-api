package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/edvargas05/motor-deteccao/internal/domain"
	"github.com/edvargas05/motor-deteccao/internal/ports"
)

var knownChannels = []domain.Channel{domain.ChannelPix, domain.ChannelTED, domain.ChannelCard}

// Result is the outcome of evaluating a rule set against one transaction.
type Result struct {
	TriggeredRules []domain.RuleRef
	Severity       domain.Severity
	Categories     []string
	Score          float64
}

// Evaluator evaluates config-driven rules against a transaction, its
// resolved window counts, and its resolved risk profile. It never talks to
// IdempotencyStore, ConfigProvider, or AlertSink — those are the
// Processor's job.
type Evaluator struct {
	windowStore ports.WindowStore

	mu       sync.RWMutex
	programs map[string]*vm.Program // keyed by rule_id@version
}

// NewEvaluator builds an Evaluator that queries windowStore for the
// window-derived variables rules may reference.
func NewEvaluator(windowStore ports.WindowStore) *Evaluator {
	return &Evaluator{
		windowStore: windowStore,
		programs:    make(map[string]*vm.Program),
	}
}

// Evaluate runs every enabled rule in rules against tx. Rules requiring
// "window" are skipped when windowAvailable is false. Rules requiring
// "customer_risk" always evaluate against risk (the caller resolves risk
// to the customer's profile or the global default beforehand).
func (e *Evaluator) Evaluate(ctx context.Context, rules []domain.RuleDef, tx domain.Transaction, risk domain.RiskProfile, windowAvailable bool) (Result, error) {
	var (
		triggeredRefs   []domain.RuleRef
		triggeredSevs   []domain.Severity
		categories      = make(map[string]struct{})
		overallSeverity domain.Severity
	)

	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}
		if rule.RequiresLayer("window") && !windowAvailable {
			continue
		}

		env, err := e.buildEnv(ctx, rule, tx, risk)
		if err != nil {
			return Result{}, fmt.Errorf("building env for rule %q: %w", rule.RuleID, err)
		}

		program, err := e.programFor(rule)
		if err != nil {
			return Result{}, fmt.Errorf("compiling rule %q: %w", rule.RuleID, err)
		}

		out, err := expr.Run(program, env)
		if err != nil {
			return Result{}, fmt.Errorf("evaluating rule %q: %w", rule.RuleID, err)
		}
		matched, ok := out.(bool)
		if !ok || !matched {
			continue
		}

		triggeredRefs = append(triggeredRefs, domain.RuleRef{RuleID: rule.RuleID, Version: rule.Version})
		triggeredSevs = append(triggeredSevs, rule.Emits.Severity)
		categories[rule.Emits.Category] = struct{}{}
		if overallSeverity == "" {
			overallSeverity = rule.Emits.Severity
		} else {
			overallSeverity = domain.MaxSeverity(overallSeverity, rule.Emits.Severity)
		}
	}

	catList := make([]string, 0, len(categories))
	for c := range categories {
		catList = append(catList, c)
	}

	return Result{
		TriggeredRules: triggeredRefs,
		Severity:       overallSeverity,
		Categories:     catList,
		Score:          Score(triggeredSevs),
	}, nil
}

func (e *Evaluator) programFor(rule domain.RuleDef) (*vm.Program, error) {
	key := fmt.Sprintf("%s@%d", rule.RuleID, rule.Version)

	e.mu.RLock()
	if p, ok := e.programs[key]; ok {
		e.mu.RUnlock()
		return p, nil
	}
	e.mu.RUnlock()

	program, err := expr.Compile(rule.Condition, expr.AllowUndefinedVariables())
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	e.programs[key] = program
	e.mu.Unlock()

	return program, nil
}

func (e *Evaluator) buildEnv(ctx context.Context, rule domain.RuleDef, tx domain.Transaction, risk domain.RiskProfile) (map[string]any, error) {
	env := map[string]any{
		"amount":      tx.Amount,
		"currency":    tx.Currency,
		"channel":     string(tx.Channel),
		"device_id":   tx.DeviceID,
		"customer_id": tx.CustomerID,
	}

	if rule.RequiresLayer("customer_risk") {
		env["risk"] = map[string]any{
			"limite_valor": risk.LimiteValor,
			"nivel":        risk.Nivel,
		}
	}

	if rule.RequiresLayer("window") {
		span := time.Duration(rule.Window.SpanSeconds) * time.Second
		counts := make(map[string]any, len(knownChannels))
		for _, ch := range knownChannels {
			n, err := e.windowStore.CountByChannel(ctx, tx.CustomerID, ch, span)
			if err != nil {
				return nil, fmt.Errorf("counting channel %q: %w", ch, err)
			}
			counts["count_channel_"+string(ch)] = n
		}
		env["window"] = counts
	}

	return env, nil
}
