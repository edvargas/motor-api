package config

import "testing"

func TestLoadDefaultBundleIsValid(t *testing.T) {
	b, err := Load(DefaultRulesJSON, DefaultProfileJSON)
	if err != nil {
		t.Fatalf("expected embedded default config to be valid, got: %v", err)
	}
	if len(b.Rules) == 0 {
		t.Fatal("expected at least one rule in default bundle")
	}
	if b.Profile.DefaultCustomerRisk.LimiteValor != 5000 {
		t.Errorf("expected default limite_valor 5000, got %v", b.Profile.DefaultCustomerRisk.LimiteValor)
	}
}

func TestLoadRejectsInvalidCondition(t *testing.T) {
	rulesJSON := []byte(`[{
		"rule_id": "bad", "version": 1, "enabled": true,
		"requires": ["event"], "condition": "amount >>> 10",
		"emits": {"severity": "alta", "category": "x"}
	}]`)
	_, err := Load(rulesJSON, DefaultProfileJSON)
	if err == nil {
		t.Fatal("expected error for invalid condition syntax")
	}
}

func TestLoadRejectsDuplicateRuleID(t *testing.T) {
	rulesJSON := []byte(`[
		{"rule_id": "dup", "version": 1, "enabled": true, "requires": ["event"], "condition": "amount > 0", "emits": {"severity": "alta", "category": "x"}},
		{"rule_id": "dup", "version": 2, "enabled": true, "requires": ["event"], "condition": "amount > 0", "emits": {"severity": "alta", "category": "x"}}
	]`)
	_, err := Load(rulesJSON, DefaultProfileJSON)
	if err == nil {
		t.Fatal("expected error for duplicate rule_id")
	}
}

func TestLoadRejectsWindowRuleWithoutSpan(t *testing.T) {
	rulesJSON := []byte(`[{
		"rule_id": "needs-window", "version": 1, "enabled": true,
		"requires": ["event", "window"], "condition": "window.count_channel_pix > 1",
		"emits": {"severity": "alta", "category": "x"}
	}]`)
	_, err := Load(rulesJSON, DefaultProfileJSON)
	if err == nil {
		t.Fatal("expected error for window-dependent rule missing window spec")
	}
}
