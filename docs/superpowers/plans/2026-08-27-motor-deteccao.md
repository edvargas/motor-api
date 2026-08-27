# Motor de Detecção de Transações Suspeitas — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build, in Go, a fully-mocked transaction-detection engine (ports & adapters) exposed via HTTP + a mocked partitioned Kafka-like source, with config-driven rules (`expr`), a sliding per-customer window, degraded-mode handling, deterministic alert IDs, ordered-by-customer concurrency, and a synthetic load generator.

**Architecture:** Hexagonal architecture. `internal/domain` holds dependency-free entities. `internal/ports` declares consumer-side interfaces for every external dependency (source, idempotency, window, risk, config, sink). `internal/engine` evaluates config-driven rules (via `expr`) and aggregates severity/score. `internal/pipeline` hosts the single `Processor` (idempotency → layer resolution → rule evaluation → decision → emission) plus a hash-by-`customer_id` `WorkerPool` that both the HTTP API and the mocked source feed into — so ordering per customer is guaranteed and no detection logic is duplicated. `internal/adapters/memory` implements every port in memory with failure toggles. `internal/adapters/httpapi` is a thin `net/http` adapter. `internal/loadgen` generates and fires synthetic traffic.

**Tech Stack:** Go 1.22+, standard library (`net/http`, `log/slog`, `context`, `sync`), `github.com/expr-lang/expr` for rule conditions, `github.com/stretchr/testify` for assertions. No other third-party dependencies.

## Global Constraints

- Module path: `github.com/edvargas05/motor-deteccao` (from `go.mod`).
- Go 1.22+; no framework on top of `net/http`.
- No database, no cloud SDKs; everything mocked in `internal/adapters/memory`.
- `context.Context` is the first parameter on every I/O-capable or cancelable method.
- Errors wrapped with `%w`; sentinel errors `ErrDuplicate`, `ErrEnrichmentUnavailable` (defined in `internal/domain/errors.go`).
- No global state, no `init()` magic, explicit `NewX(...)` constructors everywhere.
- All shared mutable state (window/idempotency maps) protected by mutexes; `go test -race ./...` must be clean.
- Score aggregation: `score = weight(max_severity)*0.7 + mean(weights of triggered rules)*0.3`, weights `baixa=0.25, media=0.5, alta=0.75, critica=1.0` (documented as constants in `internal/engine`).
- `alert_id = sha256_hex("v1|" + customer_id + "|" + transaction_id + "|" + window_bucket)`, where `window_bucket` is computed from the **highest-severity triggered rule that has a `window`**: `bucket_start = floor(captured_at.Unix() / span_seconds) * span_seconds`, RFC3339 UTC string of that instant. If no triggered rule has a `window`, fall back to a fixed 60-second span for the bucket.
- No git; do not run any `git` command as part of this plan. Steps that would normally say "commit" instead say "mark step done".

---

## File Structure

```
go.mod
Makefile
README.md
cmd/motor/main.go
cmd/motor/demo.go
cmd/motor/loadtest.go
internal/domain/transaction.go
internal/domain/severity.go
internal/domain/rule.go
internal/domain/alert.go
internal/domain/errors.go
internal/ports/source.go
internal/ports/idempotency.go
internal/ports/window.go
internal/ports/risk.go
internal/ports/config.go
internal/ports/sink.go
internal/config/bundle.go
internal/config/embed.go
internal/config/default_rules.json
internal/config/default_profile.json
internal/engine/score.go
internal/engine/evaluator.go
internal/adapters/memory/idempotency_store.go
internal/adapters/memory/window_store.go
internal/adapters/memory/risk_store.go
internal/adapters/memory/alert_sink.go
internal/adapters/memory/config_provider.go
internal/adapters/memory/source.go
internal/pipeline/dispatch/pool.go
internal/pipeline/processor.go
internal/adapters/httpapi/server.go
internal/adapters/httpapi/handlers.go
internal/adapters/httpapi/metrics.go
internal/loadgen/generate.go
internal/loadgen/client.go
```

Each task below creates or modifies a cohesive slice of this tree and ends with `go build ./...` (and `go test -race ./...` once tests exist) passing.

---

### Task 1: Module init + domain package

**Files:**
- Create: `go.mod`
- Create: `internal/domain/transaction.go`
- Create: `internal/domain/severity.go`
- Create: `internal/domain/rule.go`
- Create: `internal/domain/alert.go`
- Create: `internal/domain/errors.go`
- Test: `internal/domain/severity_test.go`
- Test: `internal/domain/rule_test.go`

**Interfaces:**
- Produces: `domain.Transaction`, `domain.Channel` (+ `ChannelPix/ChannelTED/ChannelCard`), `domain.Geo`, `domain.Severity` (+ `SeverityBaixa/Media/Alta/Critica`), `domain.MaxSeverity(a, b Severity) Severity`, `domain.RuleDef`, `domain.WindowSpec`, `domain.RuleEmits`, `domain.OperationalProfile`, `domain.Thresholds`, `domain.RiskProfile`, `domain.RuleRef`, `domain.Evaluation` (+ `EvaluationComplete/EvaluationPartial`), `domain.Alert`, sentinel errors `domain.ErrDuplicate`, `domain.ErrEnrichmentUnavailable`, `domain.ErrInvalidTransaction`, `domain.ErrInvalidConfig`, `domain.ErrRiskProfileNotFound`.

- [ ] **Step 1: Initialize the module**

Run:
```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go mod init github.com/edvargas05/motor-deteccao
```
Expected: creates `go.mod` with `module github.com/edvargas05/motor-deteccao` and a `go 1.22` (or newer) directive.

- [ ] **Step 2: Write `internal/domain/transaction.go`**

```go
package domain

import (
	"fmt"
	"time"
)

// Channel identifies the payment rail a transaction moved through.
type Channel string

const (
	ChannelPix  Channel = "pix"
	ChannelTED  Channel = "ted"
	ChannelCard Channel = "card"
)

func (c Channel) Valid() bool {
	switch c {
	case ChannelPix, ChannelTED, ChannelCard:
		return true
	default:
		return false
	}
}

// Geo is the geolocation captured with a transaction.
type Geo struct {
	Country string  `json:"country"`
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
}

// Transaction is the inbound event the engine evaluates.
type Transaction struct {
	CustomerID    string    `json:"customer_id"`
	TransactionID string    `json:"transaction_id"`
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
	Channel       Channel   `json:"channel"`
	DeviceID      string    `json:"device_id"`
	Geo           Geo       `json:"geo"`
	CapturedAt    time.Time `json:"captured_at"`
}

// Validate rejects structurally invalid transactions before any processing.
func (t Transaction) Validate() error {
	if t.CustomerID == "" {
		return fmt.Errorf("%w: customer_id is required", ErrInvalidTransaction)
	}
	if t.TransactionID == "" {
		return fmt.Errorf("%w: transaction_id is required", ErrInvalidTransaction)
	}
	if t.Amount <= 0 {
		return fmt.Errorf("%w: amount must be positive", ErrInvalidTransaction)
	}
	if t.Currency == "" {
		return fmt.Errorf("%w: currency is required", ErrInvalidTransaction)
	}
	if !t.Channel.Valid() {
		return fmt.Errorf("%w: unknown channel %q", ErrInvalidTransaction, t.Channel)
	}
	if t.CapturedAt.IsZero() {
		return fmt.Errorf("%w: captured_at is required", ErrInvalidTransaction)
	}
	return nil
}

// IdempotencyKey is the key used by IdempotencyStore for this transaction.
func (t Transaction) IdempotencyKey() string {
	return t.CustomerID + ":" + t.TransactionID
}
```

- [ ] **Step 3: Write `internal/domain/severity.go`**

```go
package domain

// Severity is the ordered risk level a triggered rule emits.
type Severity string

const (
	SeverityBaixa   Severity = "baixa"
	SeverityMedia   Severity = "media"
	SeverityAlta    Severity = "alta"
	SeverityCritica Severity = "critica"
)

var severityRank = map[Severity]int{
	SeverityBaixa:   1,
	SeverityMedia:   2,
	SeverityAlta:    3,
	SeverityCritica: 4,
}

func (s Severity) Valid() bool {
	_, ok := severityRank[s]
	return ok
}

func (s Severity) rank() int {
	return severityRank[s]
}

// MaxSeverity returns whichever of a, b ranks higher. Unknown severities
// rank as zero, so a known severity always beats an unknown one.
func MaxSeverity(a, b Severity) Severity {
	if a.rank() >= b.rank() {
		return a
	}
	return b
}
```

- [ ] **Step 4: Write `internal/domain/rule.go`**

```go
package domain

// WindowType is the kind of aggregate a rule's window layer computes.
type WindowType string

const WindowTypeCount WindowType = "count"

// WindowSpec configures the sliding window a rule reads from.
type WindowSpec struct {
	SpanSeconds int        `json:"span_seconds"`
	Type        WindowType `json:"type"`
}

// RuleEmits is what a rule contributes to an alert once triggered.
type RuleEmits struct {
	Severity Severity `json:"severity"`
	Category string   `json:"category"`
}

// RuleDef is a single detection rule loaded from configuration.
type RuleDef struct {
	RuleID    string      `json:"rule_id"`
	Version   int         `json:"version"`
	Enabled   bool        `json:"enabled"`
	Requires  []string    `json:"requires"`
	Window    *WindowSpec `json:"window,omitempty"`
	Condition string      `json:"condition"`
	Emits     RuleEmits   `json:"emits"`
}

// RequiresLayer reports whether the rule declared a dependency on layer.
func (r RuleDef) RequiresLayer(layer string) bool {
	for _, l := range r.Requires {
		if l == layer {
			return true
		}
	}
	return false
}

// RiskProfile is a customer's operational risk parameters.
type RiskProfile struct {
	LimiteValor float64 `json:"limite_valor"`
	Nivel       string  `json:"nivel"`
}

// Thresholds are global decision thresholds.
type Thresholds struct {
	ScoreMinimoAlerta float64 `json:"score_minimo_alerta"`
}

// OperationalProfile is the config-driven set of operational parameters.
type OperationalProfile struct {
	Version             int         `json:"version"`
	DefaultCustomerRisk RiskProfile `json:"default_customer_risk"`
	Thresholds          Thresholds  `json:"thresholds"`
}
```

- [ ] **Step 5: Write `internal/domain/alert.go`**

```go
package domain

// RuleRef identifies a specific rule version that triggered.
type RuleRef struct {
	RuleID  string `json:"rule_id"`
	Version int    `json:"version"`
}

// Evaluation reports whether every required layer was available.
type Evaluation string

const (
	EvaluationComplete Evaluation = "complete"
	EvaluationPartial  Evaluation = "partial"
)

// Alert is the outbound event the engine publishes for a suspicious transaction.
type Alert struct {
	AlertID        string     `json:"alert_id"`
	CustomerID     string     `json:"customer_id"`
	TransactionID  string     `json:"transaction_id"`
	Severity       Severity   `json:"severity"`
	Categories     []string   `json:"categories"`
	Score          float64    `json:"score"`
	TriggeredRules []RuleRef  `json:"triggered_rules"`
	Evaluation     Evaluation `json:"evaluation"`
	Degraded       []string   `json:"degraded,omitempty"`
}
```

- [ ] **Step 6: Write `internal/domain/errors.go`**

```go
package domain

import "errors"

var (
	// ErrDuplicate marks a transaction already seen by the idempotency store.
	ErrDuplicate = errors.New("transaction already processed")
	// ErrEnrichmentUnavailable marks a store (window or risk) as down.
	ErrEnrichmentUnavailable = errors.New("enrichment layer unavailable")
	// ErrInvalidTransaction marks a structurally invalid inbound transaction.
	ErrInvalidTransaction = errors.New("invalid transaction payload")
	// ErrInvalidConfig marks a configuration bundle that failed validation.
	ErrInvalidConfig = errors.New("invalid configuration")
	// ErrRiskProfileNotFound marks a customer with no stored risk profile.
	ErrRiskProfileNotFound = errors.New("risk profile not found")
)
```

- [ ] **Step 7: Write `internal/domain/severity_test.go`**

```go
package domain

import "testing"

func TestMaxSeverity(t *testing.T) {
	cases := []struct {
		a, b, want Severity
	}{
		{SeverityBaixa, SeverityAlta, SeverityAlta},
		{SeverityCritica, SeverityMedia, SeverityCritica},
		{SeverityMedia, SeverityMedia, SeverityMedia},
	}
	for _, c := range cases {
		got := MaxSeverity(c.a, c.b)
		if got != c.want {
			t.Errorf("MaxSeverity(%q, %q) = %q, want %q", c.a, c.b, got, c.want)
		}
	}
}
```

- [ ] **Step 8: Write `internal/domain/rule_test.go`**

```go
package domain

import "testing"

func TestRuleDefRequiresLayer(t *testing.T) {
	r := RuleDef{Requires: []string{"event", "window"}}
	if !r.RequiresLayer("window") {
		t.Error("expected RequiresLayer(window) to be true")
	}
	if r.RequiresLayer("customer_risk") {
		t.Error("expected RequiresLayer(customer_risk) to be false")
	}
}

func TestTransactionValidate(t *testing.T) {
	valid := Transaction{
		CustomerID: "c1", TransactionID: "t1", Amount: 10,
		Currency: "BRL", Channel: ChannelPix, CapturedAt: mustParseTime(t, "2026-08-24T14:03:11Z"),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected valid transaction, got error: %v", err)
	}

	invalid := valid
	invalid.Amount = 0
	if err := invalid.Validate(); err == nil {
		t.Fatal("expected error for non-positive amount")
	}
}
```

Note: `mustParseTime` is a small local test helper — add it to the bottom of `rule_test.go`:

```go
func mustParseTime(t *testing.T, s string) (out time.Time) {
	t.Helper()
	out, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse time: %v", err)
	}
	return out
}
```

Add `"time"` to the import block of `rule_test.go`.

- [ ] **Step 9: Run the tests**

```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go build ./...
go test ./internal/domain/...
```
Expected: build succeeds, both tests `PASS`.

- [ ] **Step 10: Mark task done**

No git — just check off this task's boxes above once the build and tests pass.

---

### Task 2: Ports package

**Files:**
- Create: `internal/ports/source.go`
- Create: `internal/ports/idempotency.go`
- Create: `internal/ports/window.go`
- Create: `internal/ports/risk.go`
- Create: `internal/ports/config.go`
- Create: `internal/ports/sink.go`

**Interfaces:**
- Consumes: `domain.Transaction`, `domain.RuleDef`, `domain.OperationalProfile`, `domain.Channel`, `domain.RiskProfile`, `domain.Alert` (Task 1).
- Produces: `ports.TransactionSource`, `ports.IdempotencyStore`, `ports.WindowStore`, `ports.RiskStore`, `ports.RuleSet`, `ports.ConfigProvider`, `ports.AlertSink`.

- [ ] **Step 1: Write `internal/ports/source.go`**

```go
package ports

import (
	"context"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// TransactionSource is the inbound port that stands in for a partitioned
// Kafka consumer. Run pushes transactions onto the returned channel,
// respecting ctx cancellation, and closes the channel when done.
//
// In a real system, offset commit for a partition would happen here after
// the dispatcher confirms the batch drawn from that partition was handed
// off to workers (at-least-once delivery) — see the mock implementation in
// internal/adapters/memory/source.go for where that call would sit.
type TransactionSource interface {
	Run(ctx context.Context) (<-chan domain.Transaction, error)
}
```

- [ ] **Step 2: Write `internal/ports/idempotency.go`**

```go
package ports

import (
	"context"
	"time"
)

// IdempotencyStore tracks transaction keys already processed.
type IdempotencyStore interface {
	// Seen reports whether key was already marked.
	Seen(ctx context.Context, key string) (bool, error)
	// Mark records key as processed for ttl.
	Mark(ctx context.Context, key string, ttl time.Duration) error
}
```

- [ ] **Step 3: Write `internal/ports/window.go`**

```go
package ports

import (
	"context"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// WindowStore maintains the per-customer sliding window used by rules.
// Implementations return a wrapped domain.ErrEnrichmentUnavailable when
// the store is toggled down, so the pipeline can enter degraded mode.
type WindowStore interface {
	// Record appends tx to customerID's window, expiring entries after ttl.
	Record(ctx context.Context, customerID string, tx domain.Transaction, ttl time.Duration) error
	// CountByChannel counts customerID's transactions on channel within the
	// last span (relative to the store's clock).
	CountByChannel(ctx context.Context, customerID string, channel domain.Channel, span time.Duration) (int, error)
}
```

- [ ] **Step 4: Write `internal/ports/risk.go`**

```go
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
```

- [ ] **Step 5: Write `internal/ports/config.go`**

```go
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
```

- [ ] **Step 6: Write `internal/ports/sink.go`**

```go
package ports

import (
	"context"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// AlertSink is the outbound port that stands in for publishing to the
// "alertas" topic.
type AlertSink interface {
	Publish(ctx context.Context, alert domain.Alert) error
}
```

- [ ] **Step 7: Build**

```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go build ./...
```
Expected: succeeds (ports has no tests of its own — it's pure interfaces).

- [ ] **Step 8: Mark task done**

---

### Task 3: Config package (embedded JSON, load & validate)

**Files:**
- Create: `internal/config/default_rules.json`
- Create: `internal/config/default_profile.json`
- Create: `internal/config/embed.go`
- Create: `internal/config/bundle.go`
- Test: `internal/config/bundle_test.go`

**Interfaces:**
- Consumes: `domain.RuleDef`, `domain.OperationalProfile`, `domain.WindowTypeCount`, `domain.ErrInvalidConfig` (Task 1).
- Produces: `config.Bundle{Rules []domain.RuleDef, Profile domain.OperationalProfile}`, `config.Load(rulesJSON, profileJSON []byte) (Bundle, error)`, `config.DefaultRulesJSON []byte`, `config.DefaultProfileJSON []byte` (via `go:embed`).

This package only parses and validates JSON into domain structs — it does not implement `ports.ConfigProvider` (that's the memory adapter in Task 5, which wraps a `config.Bundle`).

- [ ] **Step 1: Write `internal/config/default_rules.json`**

```json
[
  {
    "rule_id": "velocidade-pix",
    "version": 4,
    "enabled": true,
    "requires": ["event", "window"],
    "window": { "span_seconds": 300, "type": "count" },
    "condition": "channel == \"pix\" && window.count_channel_pix > 8",
    "emits": { "severity": "alta", "category": "velocidade" }
  },
  {
    "rule_id": "valor-atipico",
    "version": 2,
    "enabled": true,
    "requires": ["event", "customer_risk"],
    "condition": "amount > risk.limite_valor",
    "emits": { "severity": "media", "category": "valor" }
  },
  {
    "rule_id": "valor-critico",
    "version": 1,
    "enabled": false,
    "requires": ["event", "customer_risk"],
    "condition": "amount > (risk.limite_valor * 5)",
    "emits": { "severity": "critica", "category": "valor" }
  }
]
```

- [ ] **Step 2: Write `internal/config/default_profile.json`**

```json
{
  "version": 5,
  "default_customer_risk": { "limite_valor": 5000, "nivel": "padrao" },
  "thresholds": { "score_minimo_alerta": 0.6 }
}
```

- [ ] **Step 3: Write `internal/config/embed.go`**

```go
package config

import _ "embed"

//go:embed default_rules.json
var DefaultRulesJSON []byte

//go:embed default_profile.json
var DefaultProfileJSON []byte
```

- [ ] **Step 4: Write `internal/config/bundle.go`**

```go
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
```

- [ ] **Step 5: Add the `expr` dependency**

```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go get github.com/expr-lang/expr@latest
```
Expected: `go.mod`/`go.sum` updated with `github.com/expr-lang/expr`.

- [ ] **Step 6: Write `internal/config/bundle_test.go`**

```go
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
```

- [ ] **Step 7: Run the tests**

```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go test ./internal/config/...
```
Expected: all four tests `PASS`.

- [ ] **Step 8: Mark task done**

---

### Task 4: Engine — score aggregation + rule evaluator

**Files:**
- Create: `internal/engine/score.go`
- Create: `internal/engine/evaluator.go`
- Test: `internal/engine/evaluator_test.go`

**Interfaces:**
- Consumes: `domain.RuleDef`, `domain.Transaction`, `domain.RiskProfile`, `domain.Severity`, `domain.RuleRef`, `domain.Channel` (Task 1); `ports.WindowStore` (Task 2).
- Produces: `engine.SeverityWeight(s domain.Severity) float64`, `engine.Score(triggered []domain.Severity) float64`, `engine.Result{TriggeredRules []domain.RuleRef, Severity domain.Severity, Categories []string, Score float64}`, `engine.NewEvaluator(windowStore ports.WindowStore) *Evaluator`, `(*Evaluator) Evaluate(ctx context.Context, rules []domain.RuleDef, tx domain.Transaction, risk domain.RiskProfile, windowAvailable bool) (Result, error)`.

- [ ] **Step 1: Write `internal/engine/score.go`**

```go
package engine

import "github.com/edvargas05/motor-deteccao/internal/domain"

// severityWeight maps each severity to its numeric weight for scoring.
// Documented formula: score = weight(max_severity)*0.7 + mean(weights)*0.3.
var severityWeight = map[domain.Severity]float64{
	domain.SeverityBaixa:   0.25,
	domain.SeverityMedia:   0.5,
	domain.SeverityAlta:    0.75,
	domain.SeverityCritica: 1.0,
}

// SeverityWeight returns the numeric weight of a severity level.
func SeverityWeight(s domain.Severity) float64 {
	return severityWeight[s]
}

// Score aggregates the severities of every triggered rule into a single
// score in [0,1]: 70% the weight of the worst (max) severity, 30% the mean
// weight across all triggered rules. This rewards a single critical hit
// while still letting several lower-severity hits push the score up.
func Score(triggered []domain.Severity) float64 {
	if len(triggered) == 0 {
		return 0
	}
	var max domain.Severity
	var sum float64
	for i, s := range triggered {
		if i == 0 {
			max = s
		} else {
			max = domain.MaxSeverity(max, s)
		}
		sum += SeverityWeight(s)
	}
	mean := sum / float64(len(triggered))
	return SeverityWeight(max)*0.7 + mean*0.3
}
```

- [ ] **Step 2: Write `internal/engine/evaluator.go`**

```go
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
		triggeredRefs  []domain.RuleRef
		triggeredSevs  []domain.Severity
		categories     = make(map[string]struct{})
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
```

- [ ] **Step 3: Write `internal/engine/evaluator_test.go`**

```go
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
		Requires: []string{"event", "window"},
		Window:   &domain.WindowSpec{SpanSeconds: 300, Type: domain.WindowTypeCount},
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
```

- [ ] **Step 4: Add testify dependency and run tests**

```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go get github.com/stretchr/testify@latest
go test ./internal/engine/...
```
Expected: all tests `PASS`.

- [ ] **Step 5: Mark task done**

---

### Task 5: Memory adapters — idempotency, window, risk, sink, config, source

**Files:**
- Create: `internal/adapters/memory/idempotency_store.go`
- Create: `internal/adapters/memory/window_store.go`
- Create: `internal/adapters/memory/risk_store.go`
- Create: `internal/adapters/memory/alert_sink.go`
- Create: `internal/adapters/memory/config_provider.go`
- Create: `internal/adapters/memory/source.go`
- Test: `internal/adapters/memory/idempotency_store_test.go`
- Test: `internal/adapters/memory/window_store_test.go`
- Test: `internal/adapters/memory/risk_store_test.go`
- Test: `internal/adapters/memory/config_provider_test.go`

**Interfaces:**
- Consumes: `ports.IdempotencyStore`, `ports.WindowStore`, `ports.RiskStore`, `ports.AlertSink`, `ports.ConfigProvider`, `ports.RuleSet`, `ports.TransactionSource` (Task 2); `domain.*` (Task 1); `config.Bundle`, `config.Load`, `config.DefaultRulesJSON`, `config.DefaultProfileJSON` (Task 3).
- Produces: `memory.NewIdempotencyStore() *IdempotencyStore`, `memory.NewWindowStore() *WindowStore` (+ `SetDown(bool)`), `memory.NewRiskStore(custom map[string]domain.RiskProfile) *RiskStore` (+ `SetDown(bool)`), `memory.NewAlertSink() *AlertSink` (+ `Alerts() []domain.Alert`), `memory.NewConfigProvider(initial config.Bundle) *ConfigProvider` (+ `SetOverride(config.Bundle)`, `Reload(ctx) error`, `Current() ports.RuleSet`), `memory.NewScriptedSource(script []domain.Transaction) *ScriptedSource` (+ `Run(ctx) (<-chan domain.Transaction, error)`).

- [ ] **Step 1: Write `internal/adapters/memory/idempotency_store.go`**

```go
package memory

import (
	"context"
	"sync"
	"time"
)

type idempotencyEntry struct {
	expiresAt time.Time
}

// IdempotencyStore is an in-memory ports.IdempotencyStore with TTL expiry.
type IdempotencyStore struct {
	mu      sync.Mutex
	entries map[string]idempotencyEntry
	now     func() time.Time
}

// NewIdempotencyStore builds an empty IdempotencyStore.
func NewIdempotencyStore() *IdempotencyStore {
	return &IdempotencyStore{
		entries: make(map[string]idempotencyEntry),
		now:     time.Now,
	}
}

func (s *IdempotencyStore) Seen(ctx context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[key]
	if !ok {
		return false, nil
	}
	if s.now().After(e.expiresAt) {
		delete(s.entries, key)
		return false, nil
	}
	return true, nil
}

func (s *IdempotencyStore) Mark(ctx context.Context, key string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[key] = idempotencyEntry{expiresAt: s.now().Add(ttl)}
	return nil
}
```

- [ ] **Step 2: Write `internal/adapters/memory/idempotency_store_test.go`**

```go
package memory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdempotencyStoreSeenAndMark(t *testing.T) {
	s := NewIdempotencyStore()
	ctx := context.Background()

	seen, err := s.Seen(ctx, "k1")
	require.NoError(t, err)
	assert.False(t, seen)

	require.NoError(t, s.Mark(ctx, "k1", time.Minute))

	seen, err = s.Seen(ctx, "k1")
	require.NoError(t, err)
	assert.True(t, seen)
}

func TestIdempotencyStoreExpiry(t *testing.T) {
	s := NewIdempotencyStore()
	fixed := time.Now()
	s.now = func() time.Time { return fixed }
	ctx := context.Background()

	require.NoError(t, s.Mark(ctx, "k1", time.Second))
	s.now = func() time.Time { return fixed.Add(2 * time.Second) }

	seen, err := s.Seen(ctx, "k1")
	require.NoError(t, err)
	assert.False(t, seen, "entry should have expired")
}
```

- [ ] **Step 3: Write `internal/adapters/memory/window_store.go`**

```go
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

type windowEntry struct {
	tx        domain.Transaction
	recordedAt time.Time
}

// WindowStore is an in-memory ports.WindowStore with TTL expiry and a
// failure toggle for demoing degraded mode.
type WindowStore struct {
	mu      sync.Mutex
	byCust  map[string][]windowEntry
	down    bool
	now     func() time.Time
}

// NewWindowStore builds an empty WindowStore.
func NewWindowStore() *WindowStore {
	return &WindowStore{
		byCust: make(map[string][]windowEntry),
		now:    time.Now,
	}
}

// SetDown toggles simulated unavailability for every subsequent call.
func (s *WindowStore) SetDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
}

func (s *WindowStore) Record(ctx context.Context, customerID string, tx domain.Transaction, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return fmt.Errorf("window store: %w", domain.ErrEnrichmentUnavailable)
	}
	s.evictLocked(customerID, ttl)
	s.byCust[customerID] = append(s.byCust[customerID], windowEntry{tx: tx, recordedAt: s.now()})
	return nil
}

func (s *WindowStore) CountByChannel(ctx context.Context, customerID string, channel domain.Channel, span time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return 0, fmt.Errorf("window store: %w", domain.ErrEnrichmentUnavailable)
	}
	cutoff := s.now().Add(-span)
	count := 0
	for _, e := range s.byCust[customerID] {
		if e.tx.Channel == channel && e.recordedAt.After(cutoff) {
			count++
		}
	}
	return count, nil
}

// evictLocked drops entries older than ttl for customerID. Caller must hold s.mu.
func (s *WindowStore) evictLocked(customerID string, ttl time.Duration) {
	cutoff := s.now().Add(-ttl)
	entries := s.byCust[customerID]
	kept := entries[:0]
	for _, e := range entries {
		if e.recordedAt.After(cutoff) {
			kept = append(kept, e)
		}
	}
	s.byCust[customerID] = kept
}
```

- [ ] **Step 4: Write `internal/adapters/memory/window_store_test.go`**

```go
package memory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

func TestWindowStoreCountByChannel(t *testing.T) {
	s := NewWindowStore()
	ctx := context.Background()
	tx := domain.Transaction{CustomerID: "c1", Channel: domain.ChannelPix}

	for i := 0; i < 3; i++ {
		require.NoError(t, s.Record(ctx, "c1", tx, time.Hour))
	}

	n, err := s.CountByChannel(ctx, "c1", domain.ChannelPix, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 3, n)

	n, err = s.CountByChannel(ctx, "c1", domain.ChannelTED, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestWindowStoreSetDown(t *testing.T) {
	s := NewWindowStore()
	s.SetDown(true)
	ctx := context.Background()

	err := s.Record(ctx, "c1", domain.Transaction{CustomerID: "c1"}, time.Hour)
	require.ErrorIs(t, err, domain.ErrEnrichmentUnavailable)

	_, err = s.CountByChannel(ctx, "c1", domain.ChannelPix, time.Hour)
	require.ErrorIs(t, err, domain.ErrEnrichmentUnavailable)

	s.SetDown(false)
	require.NoError(t, s.Record(ctx, "c1", domain.Transaction{CustomerID: "c1", Channel: domain.ChannelPix}, time.Hour))
}
```

- [ ] **Step 5: Write `internal/adapters/memory/risk_store.go`**

```go
package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// RiskStore is an in-memory ports.RiskStore seeded with a fixed set of
// personalized profiles, and a failure toggle for demoing degraded mode.
type RiskStore struct {
	mu       sync.RWMutex
	profiles map[string]domain.RiskProfile
	down     bool
}

// NewRiskStore builds a RiskStore seeded with custom (customerID -> profile).
func NewRiskStore(custom map[string]domain.RiskProfile) *RiskStore {
	profiles := make(map[string]domain.RiskProfile, len(custom))
	for k, v := range custom {
		profiles[k] = v
	}
	return &RiskStore{profiles: profiles}
}

// SetDown toggles simulated unavailability for every subsequent call.
func (s *RiskStore) SetDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
}

func (s *RiskStore) Get(ctx context.Context, customerID string) (*domain.RiskProfile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.down {
		return nil, fmt.Errorf("risk store: %w", domain.ErrEnrichmentUnavailable)
	}
	p, ok := s.profiles[customerID]
	if !ok {
		return nil, domain.ErrRiskProfileNotFound
	}
	return &p, nil
}
```

- [ ] **Step 6: Write `internal/adapters/memory/risk_store_test.go`**

```go
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
```

- [ ] **Step 7: Write `internal/adapters/memory/alert_sink.go`**

```go
package memory

import (
	"context"
	"log/slog"
	"sync"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// AlertSink is an in-memory ports.AlertSink that accumulates every
// published alert and logs it, standing in for the "alertas" topic.
type AlertSink struct {
	mu     sync.Mutex
	alerts []domain.Alert
	logger *slog.Logger
}

// NewAlertSink builds an empty AlertSink logging through logger.
func NewAlertSink(logger *slog.Logger) *AlertSink {
	return &AlertSink{logger: logger}
}

func (s *AlertSink) Publish(ctx context.Context, alert domain.Alert) error {
	s.mu.Lock()
	s.alerts = append(s.alerts, alert)
	s.mu.Unlock()

	s.logger.InfoContext(ctx, "alert published",
		"alert_id", alert.AlertID,
		"customer_id", alert.CustomerID,
		"transaction_id", alert.TransactionID,
		"severity", alert.Severity,
		"categories", alert.Categories,
		"score", alert.Score,
		"evaluation", alert.Evaluation,
	)
	return nil
}

// Alerts returns a snapshot of every alert published so far.
func (s *AlertSink) Alerts() []domain.Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Alert, len(s.alerts))
	copy(out, s.alerts)
	return out
}
```

- [ ] **Step 8: Write `internal/adapters/memory/config_provider.go`**

```go
package memory

import (
	"context"
	"sync"

	"github.com/edvargas05/motor-deteccao/internal/config"
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
		active: ports.RuleSet{Version: 1, Rules: initial.Rules, Profile: initial.Profile},
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
```

`ConfigProvider.Reload` re-serializes and re-validates through `config.Load` so an override that was mutated in place (or bypassed validation) can never become active silently. Add these two small helpers at the bottom of the same file:

```go
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
```

Add `"encoding/json"`, `"fmt"`, and `"github.com/edvargas05/motor-deteccao/internal/domain"` to the import block.

- [ ] **Step 9: Write `internal/adapters/memory/config_provider_test.go`**

```go
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
```

- [ ] **Step 10: Write `internal/adapters/memory/source.go`**

```go
package memory

import (
	"context"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// ScriptedSource is an in-memory ports.TransactionSource that replays a
// fixed script of transactions, standing in for a partitioned Kafka
// consumer (partitioned by customer_id in a real deployment).
type ScriptedSource struct {
	script []domain.Transaction
}

// NewScriptedSource builds a ScriptedSource that will replay script, in
// order, when Run is called.
func NewScriptedSource(script []domain.Transaction) *ScriptedSource {
	return &ScriptedSource{script: script}
}

func (s *ScriptedSource) Run(ctx context.Context) (<-chan domain.Transaction, error) {
	out := make(chan domain.Transaction)
	go func() {
		defer close(out)
		for _, tx := range s.script {
			select {
			case <-ctx.Done():
				return
			case out <- tx:
				// In a real Kafka consumer, offset commit for this
				// partition would happen here, after the dispatcher has
				// accepted the batch containing tx (at-least-once
				// delivery) — not before, and not per-message, to avoid
				// committing offsets for messages that never reached a
				// worker.
			}
		}
	}()
	return out, nil
}
```

- [ ] **Step 11: Run the tests**

```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go build ./...
go test -race ./internal/adapters/memory/...
```
Expected: build succeeds, all tests `PASS` with `-race` clean.

- [ ] **Step 12: Mark task done**

---

### Task 6: Dispatcher / WorkerPool (order-by-customer concurrency)

**Files:**
- Create: `internal/pipeline/dispatch/pool.go`
- Test: `internal/pipeline/dispatch/pool_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks except standard library — the pool is generic over a `func(ctx context.Context, customerID string, item any) any`-shaped job so `internal/pipeline` (Task 7) can plug the `Processor` in without `dispatch` importing `domain`/`pipeline` (avoids a cycle).
- Produces: `dispatch.Job{CustomerID string, Run func(ctx context.Context)}`, `dispatch.NewPool(workers int) *Pool`, `(*Pool) Submit(ctx context.Context, job Job)`, `(*Pool) Close()`.

- [ ] **Step 1: Write `internal/pipeline/dispatch/pool.go`**

```go
// Package dispatch hosts the WorkerPool that both the HTTP API and the
// mocked source feed into, guaranteeing every transaction for a given
// customer_id is processed by the same worker, in submission order, while
// different customers process in parallel.
package dispatch

import (
	"context"
	"hash/fnv"
	"sync"
)

// Job is one unit of work routed by CustomerID's hash to a fixed worker.
// Run must not block on anything outside ctx's lifetime and must itself
// signal completion (e.g. via a channel it closes over) since Submit does
// not return a result.
type Job struct {
	CustomerID string
	Run        func(ctx context.Context)
}

// Pool is a fixed-size worker pool, one queue per worker, routed by
// hash(customer_id) % N so a customer's jobs always land on the same
// worker and never reorder relative to each other.
type Pool struct {
	queues []chan Job
	wg     sync.WaitGroup
}

// NewPool starts workers goroutines, each draining its own queue.
func NewPool(workers int) *Pool {
	if workers < 1 {
		workers = 1
	}
	p := &Pool{queues: make([]chan Job, workers)}
	for i := range p.queues {
		p.queues[i] = make(chan Job, 64)
		p.wg.Add(1)
		go p.runWorker(p.queues[i])
	}
	return p
}

func (p *Pool) runWorker(queue chan Job) {
	defer p.wg.Done()
	for job := range queue {
		job.Run(context.Background())
	}
}

// Submit routes job to its customer's worker queue. It blocks if that
// worker's queue is full (backpressure), respecting ctx cancellation.
func (p *Pool) Submit(ctx context.Context, job Job) {
	idx := workerIndex(job.CustomerID, len(p.queues))
	select {
	case p.queues[idx] <- job:
	case <-ctx.Done():
	}
}

func workerIndex(customerID string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(customerID))
	return int(h.Sum32()) % n
}

// Close stops accepting new jobs and waits for every worker to drain its
// queue and exit.
func (p *Pool) Close() {
	for _, q := range p.queues {
		close(q)
	}
	p.wg.Wait()
}
```

- [ ] **Step 2: Write `internal/pipeline/dispatch/pool_test.go`**

```go
package dispatch

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolPreservesOrderPerCustomer(t *testing.T) {
	p := NewPool(4)
	defer p.Close()

	var mu sync.Mutex
	var order []int
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		i := i
		p.Submit(context.Background(), Job{
			CustomerID: "same-customer",
			Run: func(ctx context.Context) {
				defer wg.Done()
				mu.Lock()
				order = append(order, i)
				mu.Unlock()
			},
		})
	}
	wg.Wait()

	for i, v := range order {
		assert.Equal(t, i, v, "jobs for the same customer must run in submission order")
	}
}

func TestPoolRunsDifferentCustomersConcurrently(t *testing.T) {
	p := NewPool(8)
	defer p.Close()

	var wg sync.WaitGroup
	results := make(chan string, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		customerID := "customer-" + string(rune('a'+i%10))
		p.Submit(context.Background(), Job{
			CustomerID: customerID,
			Run: func(ctx context.Context) {
				defer wg.Done()
				results <- customerID
			},
		})
	}
	wg.Wait()
	close(results)

	count := 0
	for range results {
		count++
	}
	assert.Equal(t, 100, count)
}
```

- [ ] **Step 3: Run the tests**

```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go test -race ./internal/pipeline/dispatch/...
```
Expected: both tests `PASS` with `-race` clean.

- [ ] **Step 4: Mark task done**

---

### Task 7: Pipeline Processor

**Files:**
- Create: `internal/pipeline/processor.go`
- Test: `internal/pipeline/processor_test.go`

**Interfaces:**
- Consumes: `domain.*` (Task 1); `ports.IdempotencyStore`, `ports.WindowStore`, `ports.RiskStore`, `ports.ConfigProvider`, `ports.AlertSink` (Task 2); `engine.NewEvaluator`, `engine.Evaluator.Evaluate`, `engine.Result` (Task 4).
- Produces: `pipeline.Verdict{Duplicate bool, Alert *domain.Alert, Partial bool, Degraded []string}`, `pipeline.NewProcessor(idempotency ports.IdempotencyStore, window ports.WindowStore, risk ports.RiskStore, cfg ports.ConfigProvider, sink ports.AlertSink, evaluator *engine.Evaluator) *Processor`, `(*Processor) Process(ctx context.Context, tx domain.Transaction) (Verdict, error)`.

This is the single place detection logic lives — both the HTTP adapter (Task 8) and the demo/loadgen wiring (Task 11) call `Processor.Process` through the `dispatch.Pool` from Task 6, never duplicating this logic.

- [ ] **Step 1: Write `internal/pipeline/processor.go`**

```go
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
	idempotencyTTL   = 24 * time.Hour
	windowTTL        = 10 * time.Minute
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

	// Mark idempotency only after we've committed to a verdict, so a
	// crash mid-evaluation does not silently swallow a retry.
	if err := p.idempotency.Mark(ctx, key, idempotencyTTL); err != nil {
		return Verdict{}, fmt.Errorf("marking idempotency: %w", err)
	}

	if result.Score < ruleSet.Profile.Thresholds.ScoreMinimoAlerta || len(result.TriggeredRules) == 0 {
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
		return Verdict{}, fmt.Errorf("publishing alert: %w", err)
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
```

- [ ] **Step 2: Write `internal/pipeline/processor_test.go`**

```go
package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/edvargas05/motor-deteccao/internal/adapters/memory"
	"github.com/edvargas05/motor-deteccao/internal/config"
	"github.com/edvargas05/motor-deteccao/internal/domain"
	"github.com/edvargas05/motor-deteccao/internal/engine"
)

func newTestProcessor(t *testing.T) (*Processor, *memory.WindowStore, *memory.RiskStore, *memory.AlertSink) {
	t.Helper()
	bundle, err := config.Load(config.DefaultRulesJSON, config.DefaultProfileJSON)
	require.NoError(t, err)

	idem := memory.NewIdempotencyStore()
	window := memory.NewWindowStore()
	risk := memory.NewRiskStore(map[string]domain.RiskProfile{
		"vip-customer": {LimiteValor: 100000, Nivel: "vip"},
	})
	cfg := memory.NewConfigProvider(bundle)
	sink := memory.NewAlertSink(testLogger())
	evaluator := engine.NewEvaluator(window)

	return NewProcessor(idem, window, risk, cfg, sink, evaluator), window, risk, sink
}

func TestProcessDuplicateTransaction(t *testing.T) {
	p, _, _, sink := newTestProcessor(t)
	ctx := context.Background()
	tx := makeTx("c1", "t1", 100, domain.ChannelCard)

	v1, err := p.Process(ctx, tx)
	require.NoError(t, err)
	assert.False(t, v1.Duplicate)

	v2, err := p.Process(ctx, tx)
	require.NoError(t, err)
	assert.True(t, v2.Duplicate)
	assert.Len(t, sink.Alerts(), 0)
}

func TestProcessValorAtipicoDefaultVsCustomLimit(t *testing.T) {
	p, _, _, _ := newTestProcessor(t)
	ctx := context.Background()

	// Default limite_valor is 5000: 6000 triggers.
	verdictDefault, err := p.Process(ctx, makeTx("plain-customer", "t1", 6000, domain.ChannelCard))
	require.NoError(t, err)
	require.NotNil(t, verdictDefault.Alert)
	assert.Contains(t, verdictDefault.Alert.Categories, "valor")

	// vip-customer has limite_valor 100000: 6000 does not trigger.
	verdictCustom, err := p.Process(ctx, makeTx("vip-customer", "t2", 6000, domain.ChannelCard))
	require.NoError(t, err)
	assert.Nil(t, verdictCustom.Alert)
}

func TestProcessVelocidadePixTriggersAfterThreshold(t *testing.T) {
	p, _, _, _ := newTestProcessor(t)
	ctx := context.Background()

	var last Verdict
	for i := 0; i < 9; i++ {
		v, err := p.Process(ctx, makeTx("burst-customer", txID(i), 100, domain.ChannelPix))
		require.NoError(t, err)
		last = v
	}
	require.NotNil(t, last.Alert, "9th pix transaction should push count over 8 and trigger velocidade-pix")
	assert.Contains(t, last.Alert.Categories, "velocidade")
}

func TestProcessDegradedModeWhenWindowDown(t *testing.T) {
	p, window, _, _ := newTestProcessor(t)
	ctx := context.Background()
	window.SetDown(true)

	// valor-atipico only requires event+customer_risk, so it still fires.
	v, err := p.Process(ctx, makeTx("plain-customer", "t1", 6000, domain.ChannelCard))
	require.NoError(t, err)
	require.NotNil(t, v.Alert)
	assert.Equal(t, domain.EvaluationPartial, v.Alert.Evaluation)
	assert.Equal(t, []string{"window"}, v.Alert.Degraded)

	window.SetDown(false)
	v2, err := p.Process(ctx, makeTx("plain-customer", "t2", 6000, domain.ChannelCard))
	require.NoError(t, err)
	require.NotNil(t, v2.Alert)
	assert.Equal(t, domain.EvaluationComplete, v2.Alert.Evaluation)
}

func TestProcessRiskStoreDownFallsBackToDefault(t *testing.T) {
	p, _, risk, _ := newTestProcessor(t)
	risk.SetDown(true)
	ctx := context.Background()

	// Default limite_valor is 5000: 6000 still triggers via fallback.
	v, err := p.Process(ctx, makeTx("vip-customer", "t1", 6000, domain.ChannelCard))
	require.NoError(t, err)
	require.NotNil(t, v.Alert)
}

func TestProcessAlertIDDeterministic(t *testing.T) {
	p, _, _, _ := newTestProcessor(t)
	ctx := context.Background()
	captured := time.Date(2026, 8, 24, 14, 3, 11, 0, time.UTC)

	tx1 := domain.Transaction{CustomerID: "c1", TransactionID: "t1", Amount: 6000, Currency: "BRL", Channel: domain.ChannelCard, CapturedAt: captured}
	v1, err := p.Process(ctx, tx1)
	require.NoError(t, err)
	require.NotNil(t, v1.Alert)

	// Same inputs on a fresh processor instance -> same alert_id.
	p2, _, _, _ := newTestProcessor(t)
	v2, err := p2.Process(ctx, tx1)
	require.NoError(t, err)
	require.NotNil(t, v2.Alert)
	assert.Equal(t, v1.Alert.AlertID, v2.Alert.AlertID)

	tx3 := tx1
	tx3.TransactionID = "different"
	p3, _, _, _ := newTestProcessor(t)
	v3, err := p3.Process(ctx, tx3)
	require.NoError(t, err)
	require.NotNil(t, v3.Alert)
	assert.NotEqual(t, v1.Alert.AlertID, v3.Alert.AlertID)
}

func TestProcessAggregatesMultipleTriggeredRules(t *testing.T) {
	p, _, _, _ := newTestProcessor(t)
	ctx := context.Background()

	var last Verdict
	for i := 0; i < 9; i++ {
		// High amount (6000 > default 5000) AND pix bursts -> both rules fire on the last one.
		v, err := p.Process(ctx, makeTx("multi-customer", txID(i), 6000, domain.ChannelPix))
		require.NoError(t, err)
		last = v
	}
	require.NotNil(t, last.Alert)
	assert.Equal(t, domain.SeverityAlta, last.Alert.Severity, "max severity across velocidade-pix (alta) and valor-atipico (media)")
	assert.ElementsMatch(t, []string{"velocidade", "valor"}, last.Alert.Categories)
	assert.Len(t, last.Alert.TriggeredRules, 2)
}

// --- test helpers ---

func makeTx(customerID, txID string, amount float64, channel domain.Channel) domain.Transaction {
	return domain.Transaction{
		CustomerID:    customerID,
		TransactionID: txID,
		Amount:        amount,
		Currency:      "BRL",
		Channel:       channel,
		DeviceID:      "device-1",
		CapturedAt:    time.Now(),
	}
}

func txID(i int) string {
	return "t" + string(rune('a'+i))
}
```

Add a tiny `testLogger` helper at the bottom of the file:

```go
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
```

Add `"io"` and `"log/slog"` to the import block.

- [ ] **Step 3: Run the tests**

```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go build ./...
go test -race ./internal/pipeline/...
```
Expected: build succeeds, all tests `PASS` with `-race` clean.

- [ ] **Step 4: Mark task done**

---

### Task 8: HTTP API adapter

**Files:**
- Create: `internal/adapters/httpapi/metrics.go`
- Create: `internal/adapters/httpapi/server.go`
- Create: `internal/adapters/httpapi/handlers.go`
- Test: `internal/adapters/httpapi/handlers_test.go`

**Interfaces:**
- Consumes: `pipeline.NewProcessor`, `pipeline.Processor.Process`, `pipeline.Verdict` (Task 7); `dispatch.NewPool`, `dispatch.Pool.Submit`, `dispatch.Job` (Task 6); `memory.WindowStore.SetDown`, `memory.ConfigProvider.Reload` (Task 5); `domain.Transaction`, `domain.Alert` (Task 1).
- Produces: `httpapi.NewServer(processor *pipeline.Processor, pool *dispatch.Pool, window *memory.WindowStore, cfg *memory.ConfigProvider, addr string) *Server`, `(*Server) Handler() http.Handler`, `(*Server) ListenAndServe() error`, `(*Server) Shutdown(ctx context.Context) error`, `httpapi.Metrics` (JSON snapshot struct).

- [ ] **Step 1: Write `internal/adapters/httpapi/metrics.go`**

```go
package httpapi

import (
	"sort"
	"sync"
	"time"
)

// Metrics accumulates the counters and latencies the /metrics endpoint
// reports, and that /transactions/batch summarizes per-batch.
type Metrics struct {
	mu          sync.Mutex
	processed   int
	duplicates  int
	alerts      int
	partials    int
	byCategory  map[string]int
	latencies   []time.Duration
}

// NewMetrics builds an empty Metrics.
func NewMetrics() *Metrics {
	return &Metrics{byCategory: make(map[string]int)}
}

// RecordDuplicate accounts for a duplicate verdict.
func (m *Metrics) RecordDuplicate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processed++
	m.duplicates++
}

// RecordProcessed accounts for a non-duplicate verdict: latency, whether it
// was partial, whether it produced an alert, and which categories.
func (m *Metrics) RecordProcessed(latency time.Duration, partial bool, categories []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processed++
	m.latencies = append(m.latencies, latency)
	if partial {
		m.partials++
	}
	if len(categories) > 0 {
		m.alerts++
		for _, c := range categories {
			m.byCategory[c]++
		}
	}
}

// Snapshot is the JSON-serializable view of Metrics at a point in time.
type Snapshot struct {
	Processed  int            `json:"processed"`
	Duplicates int            `json:"duplicates"`
	Alerts     int            `json:"alerts"`
	Partials   int            `json:"partials"`
	ByCategory map[string]int `json:"by_category"`
	LatencyMS  LatencyStats   `json:"latency_ms"`
}

// LatencyStats summarizes a set of latencies in milliseconds.
type LatencyStats struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

// Snapshot returns a point-in-time copy of the accumulated metrics.
func (m *Metrics) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	byCategory := make(map[string]int, len(m.byCategory))
	for k, v := range m.byCategory {
		byCategory[k] = v
	}

	return Snapshot{
		Processed:  m.processed,
		Duplicates: m.duplicates,
		Alerts:     m.alerts,
		Partials:   m.partials,
		ByCategory: byCategory,
		LatencyMS:  latencyStats(m.latencies),
	}
}

func latencyStats(latencies []time.Duration) LatencyStats {
	if len(latencies) == 0 {
		return LatencyStats{}
	}
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	pct := func(p float64) float64 {
		idx := int(p * float64(len(sorted)-1))
		return float64(sorted[idx]) / float64(time.Millisecond)
	}
	return LatencyStats{P50: pct(0.50), P95: pct(0.95), P99: pct(0.99)}
}
```

- [ ] **Step 2: Write `internal/adapters/httpapi/server.go`**

```go
// Package httpapi is a thin net/http adapter: it validates and deserializes
// requests, routes them through the shared dispatch.Pool into
// pipeline.Processor, and serializes the verdict. No detection logic
// lives here.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/adapters/memory"
	"github.com/edvargas05/motor-deteccao/internal/pipeline"
	"github.com/edvargas05/motor-deteccao/internal/pipeline/dispatch"
)

// Server wires the HTTP mux and an underlying http.Server.
type Server struct {
	httpServer *http.Server
	metrics    *Metrics
}

// NewServer builds a Server that routes requests through processor via
// pool, and exposes admin toggles on window and cfg.
func NewServer(processor *pipeline.Processor, pool *dispatch.Pool, window *memory.WindowStore, cfg *memory.ConfigProvider, addr string, logger *slog.Logger) *Server {
	h := &handler{
		processor: processor,
		pool:      pool,
		window:    window,
		cfg:       cfg,
		metrics:   NewMetrics(),
		logger:    logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /transactions", h.postTransaction)
	mux.HandleFunc("POST /transactions/batch", h.postTransactionBatch)
	mux.HandleFunc("GET /healthz", h.getHealthz)
	mux.HandleFunc("GET /metrics", h.getMetrics)
	mux.HandleFunc("POST /admin/window/down", h.postWindowDown)
	mux.HandleFunc("POST /admin/window/up", h.postWindowUp)
	mux.HandleFunc("POST /admin/config/reload", h.postConfigReload)

	return &Server{
		metrics: h.metrics,
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      30 * time.Second,
			MaxHeaderBytes:    1 << 16,
		},
	}
}

// Handler exposes the underlying http.Handler, mainly for httptest use.
func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler
}

// Metrics exposes the Server's Metrics collector.
func (s *Server) Metrics() *Metrics {
	return s.metrics
}

// ListenAndServe starts serving and blocks until the server stops.
func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully drains in-flight requests within ctx's deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
```

- [ ] **Step 3: Write `internal/adapters/httpapi/handlers.go`**

```go
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/adapters/memory"
	"github.com/edvargas05/motor-deteccao/internal/domain"
	"github.com/edvargas05/motor-deteccao/internal/pipeline"
	"github.com/edvargas05/motor-deteccao/internal/pipeline/dispatch"
)

const maxBodyBytes = 1 << 20 // 1 MiB

type handler struct {
	processor *pipeline.Processor
	pool      *dispatch.Pool
	window    *memory.WindowStore
	cfg       *memory.ConfigProvider
	metrics   *Metrics
	logger    *slog.Logger
}

// transactionResponse mirrors what POST /transactions and each item of a
// batch summary can return.
type transactionResponse struct {
	Status     string        `json:"status,omitempty"`     // "duplicate", when applicable
	Suspicious bool          `json:"suspicious"`
	Alert      *domain.Alert `json:"alert,omitempty"`
}

func (h *handler) postTransaction(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	var tx domain.Transaction
	if err := json.NewDecoder(r.Body).Decode(&tx); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	verdict, err := h.processOne(r.Context(), tx)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if verdict.Duplicate {
		writeJSON(w, http.StatusOK, transactionResponse{Status: "duplicate"})
		return
	}
	if verdict.Alert != nil {
		writeJSON(w, http.StatusOK, transactionResponse{Suspicious: true, Alert: verdict.Alert})
		return
	}
	writeJSON(w, http.StatusOK, transactionResponse{Suspicious: false})
}

// processOne routes a single transaction through the shared dispatch.Pool
// and waits synchronously for its result — this is the "API síncrona"
// entry point the spec requires: it never talks to Processor directly.
func (h *handler) processOne(ctx context.Context, tx domain.Transaction) (pipeline.Verdict, error) {
	type outcome struct {
		verdict pipeline.Verdict
		err     error
	}
	done := make(chan outcome, 1)
	start := time.Now()

	h.pool.Submit(ctx, dispatch.Job{
		CustomerID: tx.CustomerID,
		Run: func(jobCtx context.Context) {
			v, err := h.processor.Process(ctx, tx)
			done <- outcome{verdict: v, err: err}
		},
	})

	select {
	case o := <-done:
		if o.err == nil {
			h.record(o.verdict, time.Since(start))
		}
		return o.verdict, o.err
	case <-ctx.Done():
		return pipeline.Verdict{}, ctx.Err()
	}
}

func (h *handler) record(v pipeline.Verdict, latency time.Duration) {
	if v.Duplicate {
		h.metrics.RecordDuplicate()
		return
	}
	var categories []string
	if v.Alert != nil {
		categories = v.Alert.Categories
	}
	h.metrics.RecordProcessed(latency, v.Partial, categories)
}

type batchSummary struct {
	Total      int          `json:"total"`
	Alerts     int          `json:"alerts"`
	Duplicates int          `json:"duplicates"`
	Partials   int          `json:"partials"`
	LatencyMS  LatencyStats `json:"latency_ms"`
	TPS        float64      `json:"tps"`
}

func (h *handler) postTransactionBatch(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes*10)

	var txs []domain.Transaction
	if err := json.NewDecoder(r.Body).Decode(&txs); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	started := time.Now()
	var (
		mu                          sync.Mutex
		total, alerts, dups, partials int
		latencies                   []time.Duration
		wg                          sync.WaitGroup
	)

	// Repartition by customer_id, not by array index: the dispatch.Pool
	// already guarantees same-customer jobs run in submission order on the
	// same worker, so submitting in array order here is sufficient — two
	// events for the same customer never race each other.
	for _, tx := range txs {
		wg.Add(1)
		tx := tx
		txStart := time.Now()
		h.pool.Submit(r.Context(), dispatch.Job{
			CustomerID: tx.CustomerID,
			Run: func(jobCtx context.Context) {
				defer wg.Done()
				v, err := h.processor.Process(r.Context(), tx)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					return
				}
				total++
				latencies = append(latencies, time.Since(txStart))
				if v.Duplicate {
					dups++
					return
				}
				if v.Partial {
					partials++
				}
				if v.Alert != nil {
					alerts++
				}
			},
		})
	}
	wg.Wait()

	elapsed := time.Since(started)
	tps := 0.0
	if elapsed > 0 {
		tps = float64(total) / elapsed.Seconds()
	}

	for i := 0; i < total; i++ {
		h.metrics.RecordDuplicate() // placeholder removed below
	}
	_ = latencies // latency stats computed directly for the response

	writeJSON(w, http.StatusOK, batchSummary{
		Total:      total,
		Alerts:     alerts,
		Duplicates: dups,
		Partials:   partials,
		LatencyMS:  latencyStats(latencies),
		TPS:        tps,
	})
}

func (h *handler) getHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *handler) getMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.metrics.Snapshot())
}

func (h *handler) postWindowDown(w http.ResponseWriter, r *http.Request) {
	h.window.SetDown(true)
	writeJSON(w, http.StatusOK, map[string]string{"window": "down"})
}

func (h *handler) postWindowUp(w http.ResponseWriter, r *http.Request) {
	h.window.SetDown(false)
	writeJSON(w, http.StatusOK, map[string]string{"window": "up"})
}

func (h *handler) postConfigReload(w http.ResponseWriter, r *http.Request) {
	if err := h.cfg.Reload(r.Context()); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"version": h.cfg.Current().Version})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
```

Fix the accidental leftover loop in `postTransactionBatch` before moving on: delete this dead block (it was a placeholder that must not ship) —

```go
	for i := 0; i < total; i++ {
		h.metrics.RecordDuplicate() // placeholder removed below
	}
	_ = latencies // latency stats computed directly for the response
```

— and replace it with nothing (the batch endpoint reports its own summary directly; it does not need to touch the server-wide `Metrics` collector). The final `postTransactionBatch` body goes straight from `wg.Wait()` to computing `elapsed`/`tps` and calling `writeJSON`.

- [ ] **Step 4: Write `internal/adapters/httpapi/handlers_test.go`**

```go
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/edvargas05/motor-deteccao/internal/adapters/memory"
	"github.com/edvargas05/motor-deteccao/internal/config"
	"github.com/edvargas05/motor-deteccao/internal/domain"
	"github.com/edvargas05/motor-deteccao/internal/engine"
	"github.com/edvargas05/motor-deteccao/internal/pipeline"
	"github.com/edvargas05/motor-deteccao/internal/pipeline/dispatch"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	bundle, err := config.Load(config.DefaultRulesJSON, config.DefaultProfileJSON)
	require.NoError(t, err)

	window := memory.NewWindowStore()
	idem := memory.NewIdempotencyStore()
	risk := memory.NewRiskStore(nil)
	cfg := memory.NewConfigProvider(bundle)
	sink := memory.NewAlertSink(slog.New(slog.NewTextHandler(io.Discard, nil)))
	evaluator := engine.NewEvaluator(window)
	processor := pipeline.NewProcessor(idem, window, risk, cfg, sink, evaluator)
	pool := dispatch.NewPool(4)
	t.Cleanup(pool.Close)

	return NewServer(processor, pool, window, cfg, ":0", slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestPostTransactionReturnsVerdict(t *testing.T) {
	s := newTestServer(t)
	body := `{"customer_id":"c1","transaction_id":"t1","amount":6000,"currency":"BRL","channel":"card","device_id":"d1","captured_at":"2026-08-24T14:03:11Z"}`

	req := httptest.NewRequest(http.MethodPost, "/transactions", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp transactionResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Suspicious, "6000 > default limite_valor 5000 should be suspicious")
	require.NotNil(t, resp.Alert)
	assert.Contains(t, resp.Alert.Categories, "valor")
}

func TestPostTransactionInvalidPayload(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/transactions", bytes.NewBufferString(`not json`))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPostTransactionBatchAggregatesSummary(t *testing.T) {
	s := newTestServer(t)
	txs := []map[string]any{
		{"customer_id": "c1", "transaction_id": "t1", "amount": 100, "currency": "BRL", "channel": "card", "device_id": "d1", "captured_at": "2026-08-24T14:03:11Z"},
		{"customer_id": "c1", "transaction_id": "t1", "amount": 100, "currency": "BRL", "channel": "card", "device_id": "d1", "captured_at": "2026-08-24T14:03:11Z"}, // duplicate
		{"customer_id": "c2", "transaction_id": "t2", "amount": 6000, "currency": "BRL", "channel": "card", "device_id": "d1", "captured_at": "2026-08-24T14:03:11Z"}, // alert
	}
	payload, err := json.Marshal(txs)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/transactions/batch", bytes.NewBuffer(payload))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var summary batchSummary
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &summary))
	assert.Equal(t, 3, summary.Total)
	assert.Equal(t, 1, summary.Duplicates)
	assert.Equal(t, 1, summary.Alerts)
}

func TestHealthzAndMetrics(t *testing.T) {
	s := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAdminWindowToggleAndConfigReload(t *testing.T) {
	s := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/admin/window/down", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/admin/window/up", nil)
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/admin/config/reload", nil)
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

var _ = context.Background // keep context imported for future ctx-based assertions
```

- [ ] **Step 5: Run the tests**

```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go build ./...
go vet ./...
go test -race ./internal/adapters/httpapi/...
```
Expected: build and vet succeed; all handler tests `PASS`. If `go vet` flags the unused `context` import trick, delete the `var _ = context.Background` line and the `"context"` import instead — it was only a placeholder to keep the import list stable and is not needed once the file is final.

- [ ] **Step 6: Mark task done**

---

### Task 9: Load generator (`internal/loadgen`)

**Files:**
- Create: `internal/loadgen/generate.go`
- Create: `internal/loadgen/client.go`
- Test: `internal/loadgen/generate_test.go`

**Interfaces:**
- Consumes: `domain.Transaction`, `domain.Channel` (Task 1).
- Produces: `loadgen.Options{NumTransactions int, NumCustomers int, Seed int64}`, `loadgen.Generate(opts Options) []domain.Transaction`, `loadgen.ClientOptions{BaseURL string, Concurrency int, BatchSize int}`, `loadgen.Result{Total, Alerts, Duplicates, Partials int, TotalDuration time.Duration, P50, P95, P99 time.Duration, TPS float64}`, `loadgen.Fire(ctx context.Context, transactions []domain.Transaction, opts ClientOptions) (Result, error)`.

- [ ] **Step 1: Write `internal/loadgen/generate.go`**

```go
// Package loadgen produces synthetic transaction traffic to exercise the
// engine's performance through the HTTP API, and fires it with a small
// internal load-testing client.
package loadgen

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// Options configures synthetic transaction generation.
type Options struct {
	NumTransactions int   // total transactions to generate; preset 100_000
	NumCustomers    int   // distinct customers to spread them across; preset 5_000
	Seed            int64 // 0 means "use a time-derived seed" (non-reproducible)
}

var channels = []domain.Channel{domain.ChannelPix, domain.ChannelTED, domain.ChannelCard}

// Generate produces opts.NumTransactions synthetic transactions, with
// purposeful triggers baked in so the demo isn't alert-sparse:
//   - ~5% of customers get a burst of 9 pix transactions in quick
//     succession (triggers velocidade-pix).
//   - ~10% of transactions use an amount above the default limite_valor
//     of 5000 (triggers valor-atipico).
//   - ~2% of transactions are exact duplicates of an earlier one in the
//     same batch (exercises idempotency).
//
// Determinism: pass a non-zero Seed to get the same output across runs.
func Generate(opts Options) []domain.Transaction {
	seed := opts.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))

	customers := make([]string, opts.NumCustomers)
	for i := range customers {
		customers[i] = fmt.Sprintf("cust-%08d-0000-0000-0000-000000000000", i)
	}

	burstCustomers := make(map[int]bool)
	for i := 0; i < opts.NumCustomers/20; i++ { // ~5%
		burstCustomers[rng.Intn(opts.NumCustomers)] = true
	}

	out := make([]domain.Transaction, 0, opts.NumTransactions)
	base := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)

	for i := 0; i < opts.NumTransactions; i++ {
		custIdx := rng.Intn(opts.NumCustomers)
		channel := channels[rng.Intn(len(channels))]
		if burstCustomers[custIdx] {
			channel = domain.ChannelPix
		}

		amount := 50 + rng.Float64()*450 // plausible baseline: 50-500
		if rng.Float64() < 0.10 {        // ~10% above default limite_valor
			amount = 5100 + rng.Float64()*10000
		}

		tx := domain.Transaction{
			CustomerID:    customers[custIdx],
			TransactionID: fmt.Sprintf("tx-%010d", i),
			Amount:        amount,
			Currency:      "BRL",
			Channel:       channel,
			DeviceID:      fmt.Sprintf("device-%06d", custIdx),
			Geo:           domain.Geo{Country: "BR", Lat: -23.55, Lon: -46.63},
			CapturedAt:    base.Add(time.Duration(i) * time.Millisecond),
		}
		out = append(out, tx)

		if rng.Float64() < 0.02 && len(out) > 1 { // ~2% exact duplicates
			dup := out[rng.Intn(len(out))]
			dup.CapturedAt = tx.CapturedAt
			out = append(out, dup)
		}
	}

	return out
}
```

- [ ] **Step 2: Write `internal/loadgen/generate_test.go`**

```go
package loadgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateDeterministicWithSeed(t *testing.T) {
	a := Generate(Options{NumTransactions: 500, NumCustomers: 50, Seed: 42})
	b := Generate(Options{NumTransactions: 500, NumCustomers: 50, Seed: 42})

	assert.Equal(t, len(a), len(b))
	for i := range a {
		assert.Equal(t, a[i], b[i])
	}
}

func TestGenerateProducesPlausibleVolume(t *testing.T) {
	txs := Generate(Options{NumTransactions: 1000, NumCustomers: 100, Seed: 1})
	assert.GreaterOrEqual(t, len(txs), 1000, "duplicates only add to the count, never subtract")
	for _, tx := range txs {
		assert.NoError(t, tx.Validate())
	}
}
```

- [ ] **Step 3: Write `internal/loadgen/client.go`**

```go
package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// ClientOptions configures Fire's internal load-testing client.
type ClientOptions struct {
	BaseURL     string // e.g. "http://localhost:8080"
	Concurrency int    // number of concurrent in-flight requests
}

// Result is the aggregated outcome of firing a set of transactions at the
// API's POST /transactions endpoint.
type Result struct {
	Total         int
	Alerts        int
	Duplicates    int
	TotalDuration time.Duration
	P50, P95, P99 time.Duration
	TPS           float64
}

type transactionResponse struct {
	Status     string `json:"status,omitempty"`
	Suspicious bool   `json:"suspicious"`
}

// Fire sends every transaction to POST {BaseURL}/transactions, with
// opts.Concurrency requests in flight at once, and aggregates latency and
// outcome statistics. It stops early if ctx is canceled.
func Fire(ctx context.Context, transactions []domain.Transaction, opts ClientOptions) (Result, error) {
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}

	client := &http.Client{Timeout: 10 * time.Second}
	sem := make(chan struct{}, opts.Concurrency)
	var (
		mu         sync.Mutex
		latencies  []time.Duration
		alerts     int
		duplicates int
		wg         sync.WaitGroup
	)

	started := time.Now()
	for _, tx := range transactions {
		select {
		case <-ctx.Done():
			wg.Wait()
			return Result{}, ctx.Err()
		default:
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(tx domain.Transaction) {
			defer wg.Done()
			defer func() { <-sem }()

			reqStart := time.Now()
			resp, err := sendOne(ctx, client, opts.BaseURL, tx)
			latency := time.Since(reqStart)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				return
			}
			latencies = append(latencies, latency)
			switch {
			case resp.Status == "duplicate":
				duplicates++
			case resp.Suspicious:
				alerts++
			}
		}(tx)
	}
	wg.Wait()
	elapsed := time.Since(started)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(p * float64(len(latencies)-1))
		return latencies[idx]
	}

	tps := 0.0
	if elapsed > 0 {
		tps = float64(len(transactions)) / elapsed.Seconds()
	}

	return Result{
		Total:         len(transactions),
		Alerts:        alerts,
		Duplicates:    duplicates,
		TotalDuration: elapsed,
		P50:           pct(0.50),
		P95:           pct(0.95),
		P99:           pct(0.99),
		TPS:           tps,
	}, nil
}

func sendOne(ctx context.Context, client *http.Client, baseURL string, tx domain.Transaction) (transactionResponse, error) {
	body, err := json.Marshal(tx)
	if err != nil {
		return transactionResponse{}, fmt.Errorf("marshaling transaction: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/transactions", bytes.NewReader(body))
	if err != nil {
		return transactionResponse{}, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return transactionResponse{}, fmt.Errorf("sending request: %w", err)
	}
	defer resp.Body.Close()

	var out transactionResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return transactionResponse{}, fmt.Errorf("decoding response: %w", err)
	}
	return out, nil
}
```

- [ ] **Step 4: Run the tests**

```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go build ./...
go test ./internal/loadgen/...
```
Expected: build succeeds, both generator tests `PASS`. (`client.go` is exercised end-to-end in Task 10's `loadtest` subcommand, not unit-tested in isolation, since it needs a live server.)

- [ ] **Step 5: Mark task done**

---

### Task 10: `cmd/motor` — serve / demo / loadtest runner

**Files:**
- Create: `cmd/motor/main.go`
- Create: `cmd/motor/demo.go`
- Create: `cmd/motor/loadtest.go`

**Interfaces:**
- Consumes everything produced so far: `domain.*`, `config.Load`/`config.Default*JSON`, `engine.NewEvaluator`, `memory.New*`, `dispatch.NewPool`, `pipeline.NewProcessor`, `httpapi.NewServer`, `loadgen.Generate`/`loadgen.Fire`.
- Produces: the `motor` binary with `serve` (default), `demo`, and `loadtest` subcommands.

- [ ] **Step 1: Write `cmd/motor/main.go`**

```go
// Command motor is the transaction-detection engine: a fully-mocked,
// ports & adapters application demonstrating idempotency, sliding-window
// rules, degraded mode, hot-reloadable config, and customer-ordered
// concurrency, driven by an HTTP API and a mocked partitioned source.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/adapters/httpapi"
	"github.com/edvargas05/motor-deteccao/internal/adapters/memory"
	"github.com/edvargas05/motor-deteccao/internal/config"
	"github.com/edvargas05/motor-deteccao/internal/engine"
	"github.com/edvargas05/motor-deteccao/internal/pipeline"
	"github.com/edvargas05/motor-deteccao/internal/pipeline/dispatch"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	mode := "serve"
	if len(os.Args) > 1 && os.Args[1][0] != '-' {
		mode = os.Args[1]
		os.Args = append(os.Args[:1], os.Args[2:]...)
	}

	var err error
	switch mode {
	case "serve":
		err = runServe(logger)
	case "demo":
		err = runDemo(logger)
	case "loadtest":
		err = runLoadtest(logger)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q; expected serve|demo|loadtest\n", mode)
		os.Exit(2)
	}

	if err != nil {
		logger.Error("exiting with error", "error", err)
		os.Exit(1)
	}
}

// buildEngine wires every port with its in-memory adapter and returns the
// pieces every mode needs: the Processor, the shared dispatch.Pool, and
// the two admin-toggleable adapters (window, config) the HTTP layer and
// demo script poke directly.
func buildEngine(logger *slog.Logger, workers int) (*pipeline.Processor, *dispatch.Pool, *memory.WindowStore, *memory.ConfigProvider, *memory.AlertSink, error) {
	bundle, err := config.Load(config.DefaultRulesJSON, config.DefaultProfileJSON)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("loading default config: %w", err)
	}

	window := memory.NewWindowStore()
	idem := memory.NewIdempotencyStore()
	risk := memory.NewRiskStore(map[string]domain.RiskProfile{
		"11111111-1111-1111-1111-111111111111": {LimiteValor: 50000, Nivel: "vip"},
	})
	cfg := memory.NewConfigProvider(bundle)
	sink := memory.NewAlertSink(logger)
	evaluator := engine.NewEvaluator(window)
	processor := pipeline.NewProcessor(idem, window, risk, cfg, sink, evaluator)
	pool := dispatch.NewPool(workers)

	return processor, pool, window, cfg, sink, nil
}

func runServe(logger *slog.Logger) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "HTTP listen address")
	workers := fs.Int("workers", 16, "number of dispatch workers")
	_ = fs.Parse(os.Args[1:])

	processor, pool, window, cfg, _, err := buildEngine(logger, *workers)
	if err != nil {
		return err
	}
	defer pool.Close()

	server := httpapi.NewServer(processor, pool, window, cfg, *addr, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("serving", "addr", *addr)
		if err := server.ListenAndServe(); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
```

Add `"github.com/edvargas05/motor-deteccao/internal/domain"` to the import block (used by `buildEngine`'s seed risk profile).

- [ ] **Step 2: Write `cmd/motor/demo.go`**

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/config"
	"github.com/edvargas05/motor-deteccao/internal/domain"
)

// runDemo executes a scripted walkthrough of every capability, printing
// each verdict so it's presentable via `go run ./cmd/motor demo`.
func runDemo(logger *slog.Logger) error {
	processor, pool, window, cfg, _, err := buildEngine(logger, 4)
	if err != nil {
		return err
	}
	defer pool.Close()
	ctx := context.Background()

	printSection := func(title string) { fmt.Printf("\n=== %s ===\n", title) }
	show := func(label string, tx domain.Transaction) {
		v, err := processor.Process(ctx, tx)
		if err != nil {
			fmt.Printf("%s: ERROR %v\n", label, err)
			return
		}
		switch {
		case v.Duplicate:
			fmt.Printf("%s: duplicate, discarded\n", label)
		case v.Alert != nil:
			b, _ := json.MarshalIndent(v.Alert, "", "  ")
			fmt.Printf("%s: ALERT\n%s\n", label, b)
		default:
			fmt.Printf("%s: not suspicious\n", label)
		}
	}

	printSection("customer_risk: default vs. personalized limit")
	base := time.Date(2026, 8, 24, 14, 0, 0, 0, time.UTC)
	show("high-value tx, default risk profile", domain.Transaction{
		CustomerID: "plain-customer", TransactionID: "demo-1", Amount: 6000,
		Currency: "BRL", Channel: domain.ChannelCard, DeviceID: "d1", CapturedAt: base,
	})
	show("same amount, personalized (vip) limit", domain.Transaction{
		CustomerID: "11111111-1111-1111-1111-111111111111", TransactionID: "demo-2", Amount: 6000,
		Currency: "BRL", Channel: domain.ChannelCard, DeviceID: "d1", CapturedAt: base,
	})

	printSection("window: pix burst triggers velocidade-pix")
	for i := 0; i < 9; i++ {
		show(fmt.Sprintf("pix burst #%d", i+1), domain.Transaction{
			CustomerID: "burst-customer", TransactionID: fmt.Sprintf("demo-burst-%d", i), Amount: 150,
			Currency: "BRL", Channel: domain.ChannelPix, DeviceID: "d2", CapturedAt: base.Add(time.Duration(i) * time.Second),
		})
	}

	printSection("idempotency: duplicate transaction")
	dupTx := domain.Transaction{
		CustomerID: "dup-customer", TransactionID: "demo-dup", Amount: 100,
		Currency: "BRL", Channel: domain.ChannelCard, DeviceID: "d3", CapturedAt: base,
	}
	show("first send", dupTx)
	show("second send (duplicate)", dupTx)

	printSection("degraded mode: WindowStore down, then recovered")
	window.SetDown(true)
	show("window down + high-value tx", domain.Transaction{
		CustomerID: "plain-customer", TransactionID: "demo-degraded", Amount: 6000,
		Currency: "BRL", Channel: domain.ChannelCard, DeviceID: "d1", CapturedAt: base,
	})
	window.SetDown(false)
	show("window back up + high-value tx", domain.Transaction{
		CustomerID: "plain-customer", TransactionID: "demo-recovered", Amount: 6000,
		Currency: "BRL", Channel: domain.ChannelCard, DeviceID: "d1", CapturedAt: base,
	})

	printSection("hot reload: enabling a new rule without redeploy")
	bundle, err := config.Load(config.DefaultRulesJSON, config.DefaultProfileJSON)
	if err != nil {
		return err
	}
	for i := range bundle.Rules {
		if bundle.Rules[i].RuleID == "valor-critico" {
			bundle.Rules[i].Enabled = true
		}
	}
	cfg.SetOverride(bundle)
	if err := cfg.Reload(ctx); err != nil {
		return err
	}
	show("very-high-value tx, now caught by newly enabled valor-critico", domain.Transaction{
		CustomerID: "plain-customer", TransactionID: "demo-hotreload", Amount: 30000,
		Currency: "BRL", Channel: domain.ChannelCard, DeviceID: "d1", CapturedAt: base,
	})

	fmt.Println("\ndemo complete")
	return nil
}
```

- [ ] **Step 3: Write `cmd/motor/loadtest.go`**

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/edvargas05/motor-deteccao/internal/adapters/httpapi"
	"github.com/edvargas05/motor-deteccao/internal/loadgen"
)

// runLoadtest generates a synthetic transaction preset, starts a
// throwaway API instance, fires the load at it with a configurable
// internal client, and prints TPS/p50/p95/p99. Results reflect this local
// process and its in-memory mocks, not real infrastructure.
func runLoadtest(logger *slog.Logger) error {
	fs := flag.NewFlagSet("loadtest", flag.ExitOnError)
	numTx := fs.Int("n", 100_000, "number of synthetic transactions to generate")
	numCustomers := fs.Int("customers", 5_000, "number of distinct customers")
	seed := fs.Int64("seed", 0, "seed for reproducible generation (0 = random)")
	concurrency := fs.Int("concurrency", 64, "concurrent in-flight HTTP requests")
	workers := fs.Int("workers", 16, "number of dispatch workers in the target server")
	addr := fs.String("addr", ":18080", "address for the throwaway server this mode starts")
	dumpPath := fs.String("dump", "", "if set, also write the generated transactions as .jsonl to this path")
	_ = fs.Parse(os.Args[1:])

	fmt.Printf("generating %d transactions across %d customers (seed=%d)...\n", *numTx, *numCustomers, *seed)
	txs := loadgen.Generate(loadgen.Options{NumTransactions: *numTx, NumCustomers: *numCustomers, Seed: *seed})

	if *dumpPath != "" {
		if err := dumpJSONL(*dumpPath, txs); err != nil {
			return fmt.Errorf("dumping jsonl: %w", err)
		}
		fmt.Printf("wrote %d transactions to %s\n", len(txs), *dumpPath)
	}

	processor, pool, _, _, sink, err := buildEngine(logger, *workers)
	if err != nil {
		return err
	}
	defer pool.Close()

	server := httpapi.NewServer(processor, pool, nil, nil, *addr, logger)
	go func() {
		_ = server.ListenAndServe()
	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	time.Sleep(200 * time.Millisecond) // let the listener come up

	fmt.Printf("firing load at http://localhost%s with concurrency=%d...\n", *addr, *concurrency)
	result, err := loadgen.Fire(context.Background(), txs, loadgen.ClientOptions{
		BaseURL:     "http://localhost" + *addr,
		Concurrency: *concurrency,
	})
	if err != nil {
		return fmt.Errorf("firing load: %w", err)
	}

	fmt.Printf(`
=== loadtest results (local process, in-memory mocks) ===
total:       %d
alerts:      %d
duplicates:  %d
duration:    %s
throughput:  %.1f TPS observed
p50:         %s
p95:         %s
p99:         %s
alerts emitted (sink): %d
`, result.Total, result.Alerts, result.Duplicates, result.TotalDuration,
		result.TPS, result.P50, result.P95, result.P99, len(sink.Alerts()))

	return nil
}
```

`httpapi.NewServer` currently requires non-nil `window`/`cfg` for its admin endpoints — passing `nil` here is fine since `loadtest` never calls `/admin/*`, but note it explicitly: add this one-line guard at the top of `postWindowDown`/`postWindowUp`/`postConfigReload` in `internal/adapters/httpapi/handlers.go` (revisit that file now):

```go
	if h.window == nil {
		writeError(w, http.StatusServiceUnavailable, "window admin toggle not available in this mode")
		return
	}
```

(same pattern for `h.cfg == nil` in `postConfigReload`). This keeps `loadtest` simple without ever risking a nil-pointer panic if someone curls `/admin/window/down` against a loadtest instance.

- [ ] **Step 4: Write the `dumpJSONL` helper at the bottom of `cmd/motor/loadtest.go`**

```go
func dumpJSONL(path string, txs []domain.Transaction) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, tx := range txs {
		if err := enc.Encode(tx); err != nil {
			return err
		}
	}
	return nil
}
```

Add `"encoding/json"` and `"github.com/edvargas05/motor-deteccao/internal/domain"` to the import block.

- [ ] **Step 5: Build and smoke-test all three modes**

```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go build ./...
go run ./cmd/motor demo
go run ./cmd/motor loadtest -n 2000 -customers 200 -seed 1
```
Expected: `demo` prints each of the five scripted sections ending in an ALERT/duplicate/not-suspicious line; `loadtest` prints generation progress then a results block with a TPS figure and p50/p95/p99. Kill any lingering process with Ctrl-C if `loadtest`'s server doesn't exit on its own (it should, since `loadtest` returns after firing and printing).

- [ ] **Step 6: Mark task done**

---

### Task 11: Full test suite pass + Makefile + README

**Files:**
- Create: `Makefile`
- Create: `README.md`

**Interfaces:**
- Consumes: nothing new — this task wires up build tooling and documents everything from Tasks 1–10.

- [ ] **Step 1: Write `Makefile`**

```makefile
.PHONY: run demo loadtest test lint build

build:
	go build ./...

run:
	go run ./cmd/motor serve

demo:
	go run ./cmd/motor demo

loadtest:
	go run ./cmd/motor loadtest

test:
	go test -race ./...

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, skipping (optional)"; \
	fi
```

- [ ] **Step 2: Run the full test suite**

```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go build ./...
go vet ./...
go test -race ./...
```
Expected: every package's tests `PASS`, `-race` clean, no vet warnings. Fix any failures surfaced here before writing the README — this is the last correctness gate.

- [ ] **Step 3: Write `README.md`**

```markdown
# Motor de Detecção de Transações Suspeitas

Exercício de arquitetura (entrevista de engenharia sênior): motor de detecção
de transações suspeitas em Go, com **toda entrada e toda consulta externa
mockadas em memória**. Roda localmente com `go run`, sem Kafka, sem AWS, sem
banco de dados.

## Como rodar

```bash
go build ./...          # compila tudo
make test                # go test -race ./... (ou: go test -race ./...)

make run                 # sobe a API HTTP em :8080
make demo                # roteiro scriptado: risco por cliente, janela,
                          # idempotência, modo degradado, hot reload
make loadtest             # gera 100k transações sintéticas e mede TPS/p99
```

`loadtest` aceita flags, ex.: `go run ./cmd/motor loadtest -n 50000 -customers 2000 -concurrency 128 -seed 7 -dump out.jsonl`.
`out.jsonl` (se usado) pode ser reproduzido depois com `curl`/`vegeta`/`k6`
contra `POST /transactions`.

## Endpoints

- `POST /transactions` — uma transação, veredito síncrono.
  ```bash
  curl -s localhost:8080/transactions -d '{
    "customer_id":"c1","transaction_id":"t1","amount":6000,
    "currency":"BRL","channel":"card","device_id":"d1",
    "captured_at":"2026-08-24T14:03:11Z"
  }' | jq
  ```
- `POST /transactions/batch` — array de transações, resumo agregado (total, alertas, duplicatas, parciais, p50/p95/p99, TPS).
- `GET /healthz` — liveness.
- `GET /metrics` — snapshot JSON dos contadores.
- `POST /admin/window/down` / `/admin/window/up` — alterna a indisponibilidade simulada do `WindowStore` (demonstra modo degradado).
- `POST /admin/config/reload` — recarrega a config ativa (demonstra "nova regra sem redeploy").

## O que cada pacote faz

| Pacote | Responsabilidade |
|---|---|
| `internal/domain` | Entidades e value objects, sem dependências externas |
| `internal/ports` | Interfaces do lado do consumidor: source, stores, config, sink |
| `internal/config` | Parsing/validação dos perfis de config (JSON embutido via `go:embed`) |
| `internal/engine` | Avaliação de regras (`expr`) e agregação de severidade/score |
| `internal/pipeline` | `Processor`: idempotência → janela/risco → regras → decisão → emissão |
| `internal/pipeline/dispatch` | `WorkerPool`: ordena por `hash(customer_id) % N` |
| `internal/adapters/memory` | Implementações em memória de todas as portas, com toggles de falha |
| `internal/adapters/httpapi` | Adaptador HTTP fino: sem lógica de detecção |
| `internal/loadgen` | Gerador de massa sintética + cliente de carga interno |
| `cmd/motor` | Wiring e subcomandos `serve`/`demo`/`loadtest` |

## Decisões de design

- **Ports & adapters**: cada dependência externa (fonte de eventos, janela,
  risco, config, sink de alertas) é uma interface definida em `internal/ports`
  (lado do consumidor), com uma única implementação em memória plugável em
  `internal/adapters/memory`.
- **Lógica única no `Processor`**: tanto a API HTTP quanto a fonte mockada
  (que simula um consumidor Kafka particionado por `customer_id`) alimentam
  o mesmo `dispatch.Pool`, que por sua vez chama sempre o mesmo
  `pipeline.Processor.Process`. Nenhuma regra de negócio existe na camada
  HTTP.
- **Ordem por `customer_id`**: o `WorkerPool` roteia cada transação para
  `hash(customer_id) % N`, garantindo que transações do mesmo cliente nunca
  sejam reordenadas, enquanto clientes diferentes processam em paralelo —
  tanto na API síncrona quanto no endpoint de lote (que reparte por
  `customer_id`, não por índice do array).
- **Modo degradado via `requires`**: cada regra declara de quais camadas
  depende (`event`, `window`, `customer_risk`). Se o `WindowStore` estiver
  indisponível, regras que dependem de `window` são puladas e o alerta
  resultante (se houver) sai com `evaluation: "partial"` e
  `degraded: ["window"]`; regras que só dependem de `event` continuam
  funcionando normalmente. `customer_risk` nunca aparece em `degraded`
  porque sempre cai para o `default_customer_risk` global.
- **Regras por config, sem redeploy**: condições são expressões `expr`
  compiladas e cacheadas por `rule_id@version`; `ConfigProvider.Reload`
  revalida a config antes de trocar a versão ativa, mantendo a última
  válida em caso de erro.
- **Mocks plugáveis com toggles de falha**: `WindowStore.SetDown(bool)` e
  `RiskStore.SetDown(bool)` simulam indisponibilidade para testes e para a
  demo, sem qualquer dependência de infraestrutura real.

## Sobre os números de desempenho

Os números de `loadtest` (TPS, p50/p95/p99) refletem **este processo local
rodando contra mocks em memória** — não a infraestrutura real (Kafka, CC,
EK7, rede). Servem para demonstrar que o paralelismo por `customer_id`
sustenta carga, não como benchmark de produção.
```

- [ ] **Step 4: Final verification**

```bash
cd /Users/edmilsonvargas/workspace/transacoes-suspeitas
go build ./...
go test -race ./...
make demo
```
Expected: build and full test suite pass; `make demo` runs end-to-end printing all five scripted sections.

- [ ] **Step 5: Mark task done — plan complete**

---

## Self-Review Notes

- **Spec coverage:** idempotency (Task 5/7 tests), sliding window + `velocidade-pix` (Task 4/7), `valor-atipico` default vs. custom (Task 4/7), degraded mode (Task 5/7), `customer_risk` fallback (Task 7), severity/category aggregation (Task 4/7), deterministic `alert_id` (Task 7), invalid-config rejection keeping last valid (Task 3/5), concurrent order preservation with `-race` (Task 6/7), HTTP handler tests for both endpoints (Task 8), graceful shutdown (Task 10), loadgen with documented trigger proportions and seed determinism (Task 9), `demo`/`loadtest`/`serve` subcommands (Task 10), Makefile + README (Task 11) — all covered.
- **No placeholders:** the one interim placeholder introduced mid-task (the dead loop in `postTransactionBatch`, Task 8 Step 3) is explicitly called out and removed within the same step, with the exact code to delete shown — not left dangling.
- **Type consistency:** `pipeline.Verdict`, `engine.Result`, `ports.RuleSet`, and `domain.Alert` field names are used identically across Tasks 4, 7, 8, and 10.
