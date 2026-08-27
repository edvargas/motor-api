package ports

import (
	"context"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// RiskStore resolves a customer's personalized risk parameters.
// Get returns domain.ErrRiskProfileNotFound when the customer has no
// stored profile, or a wrapped domain.ErrEnrichmentUnavailable when the
// store is toggled down.
type RiskStore interface {
	Get(ctx context.Context, customerID string) (*domain.RiskProfile, error)
}
