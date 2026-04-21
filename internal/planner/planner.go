// Package planner serializes plan computations through a bounded worker pool.
// Running inline on the proxy goroutine is unsafe: a SyncGroup intercept
// blocks the upstream pump until Compute returns, and compute is
// O(members × partitions). The pool caps CPU use and goroutine count.
//
// Plan is synchronous: callers send a Request and block on Reply up to a
// context deadline. Workers run plan.Compute.
package planner

import (
	"context"
	"errors"
	"runtime"
	"time"

	"github.com/mohsanabbas/kproxy/internal/plan"
)

// Request is the input to a single plan computation. Reply must be a buffered
// channel of size 1 so the worker can send-and-forget without blocking on a
// caller that timed out.
type Request struct {
	In    plan.Inputs
	Reply chan<- Reply
}

// Reply carries the planner's output and elapsed time.
type Reply struct {
	Plan    plan.Plan
	Elapsed time.Duration
	Err     error
	// Panic is non-nil when Compute panicked. Callers treat it as a hard
	// failure (passthrough) and surface the value to a metric/logger.
	Panic any
}

// ErrTimeout is returned when a Plan call's context deadline expires before a
// worker accepts the Request.
var ErrTimeout = errors.New("planner: timed out waiting for worker")

// ErrClosed is returned when Plan is called after Close.
var ErrClosed = errors.New("planner: closed")

// Pool is a bounded worker pool. Zero value is unusable; use New.
type Pool struct {
	workers int
	queue   chan Request
	closed  chan struct{}
}

// New returns a Pool with `workers` goroutines and an internal queue of
// `queueDepth` (extra requests buffered when all workers are busy). Defaults:
// workers=GOMAXPROCS, queueDepth=workers*4.
func New(workers, queueDepth int) *Pool {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if queueDepth < 0 {
		queueDepth = workers * 4
	}
	p := &Pool{
		workers: workers,
		queue:   make(chan Request, queueDepth),
		closed:  make(chan struct{}),
	}
	for range workers {
		go p.worker()
	}
	return p
}

func (p *Pool) worker() {
	for {
		select {
		case <-p.closed:
			return
		case req, ok := <-p.queue:
			if !ok {
				return
			}
			p.runOne(req)
		}
	}
}

// runOne computes a single plan with panic isolation. A panicking Compute
// must not take down the worker (and via worker exit, eventually the whole
// pool); it must only fail this one request so the caller falls back to
// passthrough.
func (p *Pool) runOne(req Request) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			req.Reply <- Reply{Plan: nil, Elapsed: time.Since(start), Panic: r}
		}
	}()
	out := plan.Compute(req.In)
	req.Reply <- Reply{Plan: out, Elapsed: time.Since(start)}
}

// Plan runs Compute synchronously, respecting ctx for both queue admission
// and worker completion. The returned Plan is owned by the caller.
func (p *Pool) Plan(ctx context.Context, in plan.Inputs) (plan.Plan, time.Duration, error) {
	// Fast path: refuse immediately if the pool is closed. Without this, a
	// caller could race past the first select into a queue send that
	// succeeds (the queue has buffer space) but then block forever on reply
	// because the workers have exited.
	select {
	case <-p.closed:
		return nil, 0, ErrClosed
	default:
	}
	reply := make(chan Reply, 1)
	req := Request{In: in, Reply: reply}
	select {
	case <-p.closed:
		return nil, 0, ErrClosed
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	case p.queue <- req:
	}
	select {
	case r := <-reply:
		return r.Plan, r.Elapsed, r.Err
	case <-p.closed:
		return nil, 0, ErrClosed
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

// Close stops the worker pool. In-flight requests run to completion; queued
// but not-yet-picked-up requests are dropped (callers see ctx.Err when their
// context expires - callers MUST always pass a context with a deadline).
func (p *Pool) Close() {
	select {
	case <-p.closed:
		return
	default:
	}
	close(p.closed)
}
