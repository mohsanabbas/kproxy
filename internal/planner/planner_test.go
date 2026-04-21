package planner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mohsanabbas/kproxy/internal/plan"
)

func TestPlanReturnsPlan(t *testing.T) {
	t.Parallel()
	p := New(2, 4)
	defer p.Close()

	in := plan.Inputs{
		Members:    []plan.Member{{MemberID: "m1", Topics: []string{"t"}}},
		Partitions: map[string][]int32{"t": {0, 1}},
	}
	pl, elapsed, err := p.Plan(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed <= 0 {
		t.Fatal("elapsed should be > 0")
	}
	if len(pl["m1"]["t"]) != 2 {
		t.Fatalf("plan = %v", pl)
	}
}

func TestPlanContextCancelled(t *testing.T) {
	t.Parallel()
	p := New(1, 0)
	defer p.Close()

	// Saturate the only worker with a long compute so the next Plan call
	// must wait on the queue.
	busy := make(chan struct{})
	go func() {
		_, _, _ = p.Plan(context.Background(), plan.Inputs{
			Members:    []plan.Member{{MemberID: "m1", Topics: []string{"t"}}},
			Partitions: bigPartitions(200_000),
		})
		close(busy)
	}()
	// Give the goroutine above a moment to grab the worker.
	time.Sleep(5 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled
	_, _, err := p.Plan(ctx, plan.Inputs{
		Members:    []plan.Member{{MemberID: "m1", Topics: []string{"t"}}},
		Partitions: map[string][]int32{"t": {0}},
	})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled, got %v", err)
	}
	<-busy
}

func TestPlanAfterCloseFails(t *testing.T) {
	t.Parallel()
	p := New(1, 1)
	p.Close()
	_, _, err := p.Plan(context.Background(), plan.Inputs{})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}

func bigPartitions(n int) map[string][]int32 {
	out := make(map[string][]int32, 1)
	ids := make([]int32, n)
	for i := range ids {
		ids[i] = int32(i)
	}
	out["t"] = ids
	return out
}
