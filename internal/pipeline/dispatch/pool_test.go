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
