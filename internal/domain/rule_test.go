package domain

import (
	"testing"
	"time"
)

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

func mustParseTime(t *testing.T, s string) (out time.Time) {
	t.Helper()
	out, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse time: %v", err)
	}
	return out
}
