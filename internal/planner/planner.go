// Package planner serializes plan computations through a bounded worker pool.
//
// Why a worker pool rather than computing inline on the proxy goroutine: a
// SyncGroup intercept blocks the per-conn upstream pump until a plan is
// ready. We must bound CPU time (compute is O(members × partitions)) and we
// don't want a flood of simultaneous JoinGroup events from N consumer groups
// to spawn unbounded goroutines.
//
// The pool exposes a synchronous Plan call: callers send a Request and block
// on its Reply channel up to a context deadline. Workers consume Requests
// from a shared channel and run plan.Compute on each.
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
	for i := 0; i < workers; i++ {
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
			start := time.Now()
			out := plan.Compute(req.In)
			req.Reply <- Reply{Plan: out, Elapsed: time.Since(start)}
		}
	}
}

// Plan runs Compute synchronously, respecting ctx for both queue admission
// and worker completion. The returned Plan is owned by the caller.
func (p *Pool) Plan(ctx context.Context, in plan.Inputs) (plan.Plan, time.Duration, error) {
	// Fast path: refuse immediately if the pool is closed. Without this we
	// could race past the first select into a queue send that succeeds (the
	// queue has buffer space) but then block forever on reply because the
	// workers have exited.
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
// requests that have not yet been picked up will be dropped (their callers
// see ctx.Err if their context expires, otherwise block forever — callers MUST
// always pass a context with a deadline).
func (p *Pool) Close() {
	select {
	case <-p.closed:
		return
	default:
	}
	close(p.closed)
}
