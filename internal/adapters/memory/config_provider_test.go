package memory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/edvargas05/motor-deteccao/internal/config"
)

func TestConfigProviderReloadSwapsInValidOverride(t *testing.T) {
	base, err := config.Load(config.DefaultRulesJSON, config.DefaultProfileJSON)
	require.NoError(t, err)

	p := NewConfigProvider(base)
	initialVersion := p.Current().Version

	newRulesJSON := []byte(`[{"rule_id":"novo","version":1,"enabled":true,"requires":["event"],"condition":"amount > 0","emits":{"severity":"baixa","category":"teste"}}]`)
	newBundle, err := config.Load(newRulesJSON, config.DefaultProfileJSON)
	require.NoError(t, err)

	p.SetOverride(newBundle)
	require.NoError(t, p.Reload(context.Background()))

	current := p.Current()
	assert.Greater(t, current.Version, initialVersion)
	assert.Len(t, current.Rules, 1)
	assert.Equal(t, "novo", current.Rules[0].RuleID)
}

func TestConfigProviderReloadKeepsLastValidOnBadOverride(t *testing.T) {
	base, err := config.Load(config.DefaultRulesJSON, config.DefaultProfileJSON)
	require.NoError(t, err)
	p := NewConfigProvider(base)
	before := p.Current()

	// Force an invalid bundle in directly (bypassing config.Load) to prove
	// Reload's defensive re-validation catches it.
	p.override = &config.Bundle{Rules: nil, Profile: base.Profile}
	p.override.Profile.DefaultCustomerRisk.LimiteValor = -1 // invalid

	err = p.Reload(context.Background())
	require.Error(t, err)

	after := p.Current()
	assert.Equal(t, before.Version, after.Version, "invalid reload must not change active version")
}
