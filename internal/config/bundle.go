package config

import (
	"encoding/json"
	"fmt"

	"github.com/expr-lang/expr"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// Bundle is a parsed, validated set of rules and operational parameters.
type Bundle struct {
	Rules   []domain.RuleDef
	Profile domain.OperationalProfile
}

// Load parses and validates rulesJSON and profileJSON into a Bundle.
// It never returns a partially valid Bundle: any validation failure
// returns a wrapped domain.ErrInvalidConfig and a zero Bundle.
func Load(rulesJSON, profileJSON []byte) (Bundle, error) {
	var rules []domain.RuleDef
	if err := json.Unmarshal(rulesJSON, &rules); err != nil {
		return Bundle{}, fmt.Errorf("%w: parsing rules: %v", domain.ErrInvalidConfig, err)
	}

	var profile domain.OperationalProfile
	if err := json.Unmarshal(profileJSON, &profile); err != nil {
		return Bundle{}, fmt.Errorf("%w: parsing profile: %v", domain.ErrInvalidConfig, err)
	}

	if err := validateProfile(profile); err != nil {
		return Bundle{}, err
	}

	seen := make(map[string]bool, len(rules))
	for _, r := range rules {
		if err := validateRule(r, seen); err != nil {
			return Bundle{}, err
		}
	}

	return Bundle{Rules: rules, Profile: profile}, nil
}

func validateProfile(p domain.OperationalProfile) error {
	if p.Thresholds.ScoreMinimoAlerta < 0 || p.Thresholds.ScoreMinimoAlerta > 1 {
		return fmt.Errorf("%w: thresholds.score_minimo_alerta must be in [0,1], got %v", domain.ErrInvalidConfig, p.Thresholds.ScoreMinimoAlerta)
	}
	if p.DefaultCustomerRisk.LimiteValor <= 0 {
		return fmt.Errorf("%w: default_customer_risk.limite_valor must be positive", domain.ErrInvalidConfig)
	}
	return nil
}

func validateRule(r domain.RuleDef, seen map[string]bool) error {
	if r.RuleID == "" {
		return fmt.Errorf("%w: rule with empty rule_id", domain.ErrInvalidConfig)
	}
	if seen[r.RuleID] {
		return fmt.Errorf("%w: duplicate rule_id %q", domain.ErrInvalidConfig, r.RuleID)
	}
	seen[r.RuleID] = true

	if !r.Emits.Severity.Valid() {
		return fmt.Errorf("%w: rule %q has invalid severity %q", domain.ErrInvalidConfig, r.RuleID, r.Emits.Severity)
	}
	if r.Emits.Category == "" {
		return fmt.Errorf("%w: rule %q missing emits.category", domain.ErrInvalidConfig, r.RuleID)
	}
	if r.Condition == "" {
		return fmt.Errorf("%w: rule %q missing condition", domain.ErrInvalidConfig, r.RuleID)
	}
	if r.RequiresLayer("window") {
		if r.Window == nil || r.Window.SpanSeconds <= 0 {
			return fmt.Errorf("%w: rule %q requires window but has no valid window.span_seconds", domain.ErrInvalidConfig, r.RuleID)
		}
	}

	// Validate condition syntax compiles. Undefined variables are allowed
	// here because the real environment is only fully known at evaluation
	// time (it depends on which layers are available for a given tx).
	if _, err := expr.Compile(r.Condition, expr.AllowUndefinedVariables()); err != nil {
		return fmt.Errorf("%w: rule %q has invalid condition: %v", domain.ErrInvalidConfig, r.RuleID, err)
	}

	return nil
}
