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
