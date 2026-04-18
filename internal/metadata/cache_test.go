package metadata

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type stubSource struct {
	calls atomic.Int32
	snap  *Snapshot
	err   error
	delay time.Duration
}

func (s *stubSource) Fetch(ctx context.Context) (*Snapshot, error) {
	s.calls.Add(1)
	if s.delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(s.delay):
		}
	}
	return s.snap, s.err
}

func TestRefreshPublishes(t *testing.T) {
	t.Parallel()
	src := &stubSource{snap: &Snapshot{ByTopic: map[string][]int32{"t": {0, 1}}}}
	c := NewCache(src, time.Hour)
	if c.Get() != nil {
		t.Fatal("Get pre-refresh should be nil")
	}
	snap, err := c.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap == nil || c.Get() != snap {
		t.Fatal("snapshot not published")
	}
	if got := c.Get().ByTopic["t"]; len(got) != 2 {
		t.Fatalf("partitions = %v", got)
	}
}

func TestRefreshSingleFlight(t *testing.T) {
	t.Parallel()
	src := &stubSource{snap: &Snapshot{}, delay: 50 * time.Millisecond}
	c := NewCache(src, time.Hour)

	const n = 8
	errs := make(chan error, n)
	for range n {
		go func() {
			_, err := c.Refresh(context.Background())
			errs <- err
		}()
	}
	for range n {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if got := src.calls.Load(); got != 1 {
		t.Fatalf("Source.Fetch called %d times, want 1 (single-flight)", got)
	}
}

func TestRefreshErrorNotPublished(t *testing.T) {
	t.Parallel()
	src := &stubSource{err: errors.New("boom")}
	c := NewCache(src, time.Hour)
	if _, err := c.Refresh(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if c.Get() != nil {
		t.Fatal("error should not publish a snapshot")
	}
}
