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
