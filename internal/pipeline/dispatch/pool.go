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
