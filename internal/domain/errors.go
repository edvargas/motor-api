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
