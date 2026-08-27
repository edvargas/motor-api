package ports

import (
	"context"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// RuleSet is the active, validated set of rules and operational parameters.
type RuleSet struct {
	Version int
	Rules   []domain.RuleDef
	Profile domain.OperationalProfile
}

// ConfigProvider exposes the active RuleSet and supports hot reload.
type ConfigProvider interface {
	// Current returns the currently active, validated RuleSet.
	Current() RuleSet
	// Reload attempts to load and validate a new RuleSet, swapping it in
	// only on success. On failure the previously active RuleSet is kept
	// and a wrapped domain.ErrInvalidConfig is returned.
	Reload(ctx context.Context) error
}
