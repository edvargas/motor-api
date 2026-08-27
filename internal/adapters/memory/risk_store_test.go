package memory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

func TestRiskStoreGetKnownAndUnknown(t *testing.T) {
	s := NewRiskStore(map[string]domain.RiskProfile{
		"vip": {LimiteValor: 50000, Nivel: "vip"},
	})
	ctx := context.Background()

	p, err := s.Get(ctx, "vip")
	require.NoError(t, err)
	assert.Equal(t, float64(50000), p.LimiteValor)

	_, err = s.Get(ctx, "unknown")
	require.ErrorIs(t, err, domain.ErrRiskProfileNotFound)
}

func TestRiskStoreSetDown(t *testing.T) {
	s := NewRiskStore(nil)
	s.SetDown(true)
	_, err := s.Get(context.Background(), "any")
	require.ErrorIs(t, err, domain.ErrEnrichmentUnavailable)
}
