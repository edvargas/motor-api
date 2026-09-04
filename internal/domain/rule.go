package domain

// WindowType is the kind of aggregate a rule's window layer computes.
type WindowType string

const (
	WindowTypeCount           WindowType = "count"
	WindowTypeGeoDistance     WindowType = "geo_distance"
	WindowTypeDeviceDiversity WindowType = "device_diversity"
)

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
