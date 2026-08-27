package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/edvargas05/motor-deteccao/internal/config"
	"github.com/edvargas05/motor-deteccao/internal/domain"
	"github.com/edvargas05/motor-deteccao/internal/ports"
)

// ConfigProvider is an in-memory ports.ConfigProvider. Reload re-validates
// whatever bundle was last set via SetOverride (or the initial bundle) and
// swaps it in only on success, keeping the previous version otherwise.
type ConfigProvider struct {
	mu       sync.RWMutex
	active   ports.RuleSet
	version  int
	override *config.Bundle
}

// NewConfigProvider builds a ConfigProvider whose initial active RuleSet is
// derived from initial.
func NewConfigProvider(initial config.Bundle) *ConfigProvider {
	return &ConfigProvider{
		active:  ports.RuleSet{Version: 1, Rules: initial.Rules, Profile: initial.Profile},
		version: 1,
	}
}

func (c *ConfigProvider) Current() ports.RuleSet {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.active
}

// SetOverride stages a new bundle to be picked up by the next Reload. This
// is how the demo runner and the /admin/config/reload HTTP endpoint
// simulate "hot reload without redeploy": in a real system this would be a
// file watch or a config-service poll instead.
func (c *ConfigProvider) SetOverride(b config.Bundle) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.override = &b
}

func (c *ConfigProvider) Reload(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.override == nil {
		return nil // nothing staged; keep current
	}

	// Re-validate defensively even though SetOverride's caller is expected
	// to have used config.Load already: Reload is the boundary that must
	// never let an invalid bundle become active.
	b, err := config.Load(mustMarshalRules(c.override.Rules), mustMarshalProfile(c.override.Profile))
	if err != nil {
		return err
	}

	c.version++
	c.active = ports.RuleSet{Version: c.version, Rules: b.Rules, Profile: b.Profile}
	c.override = nil
	return nil
}

func mustMarshalRules(rules []domain.RuleDef) []byte {
	b, err := json.Marshal(rules)
	if err != nil {
		panic(fmt.Sprintf("marshal rules: %v", err)) // in-memory struct, never fails
	}
	return b
}

func mustMarshalProfile(p domain.OperationalProfile) []byte {
	b, err := json.Marshal(p)
	if err != nil {
		panic(fmt.Sprintf("marshal profile: %v", err))
	}
	return b
}
