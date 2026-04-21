// Package metadata maintains an immutable, atomically-swappable snapshot of
// topic → partition-id list. Readers do a single atomic.Pointer load and walk
// a plain map; writers (the refresher) build a brand-new map and swap.
//
// Why not a sync.RWMutex? The planner reads the snapshot on every JoinGroup
// rebalance and the telemetry poller reads it on every tick. Published
// snapshots are never mutated
// the published map in place. Atomic-pointer swap eliminates reader contention
// and avoids any per-read alloc.
package metadata

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mohsanabbas/kproxy/internal/kclient"
)

// Snapshot is the immutable topic → partition-ids map plus the time it was
// built. Callers MUST treat it (and the slices it contains) as read-only.
type Snapshot struct {
	BuiltAt    time.Time
	ByTopic    map[string][]int32
	BrokerByID map[int32]Broker
}

// Broker is the (host, port) pair as advertised by the Kafka cluster - i.e.
// the *real* broker addresses, before any topology rewrite.
type Broker struct {
	Host string
	Port int32
}

// Cache holds the current Snapshot. The zero value is unusable; use NewCache.
type Cache struct {
	cur     atomic.Pointer[Snapshot]
	source  Source
	refresh time.Duration

	// single-flight: at most one refresh runs at any time. Concurrent callers
	// of Refresh wait on the in-flight one.
	mu     sync.Mutex
	flight *refreshCall

	// errors counts background refresh failures observed by Run. Exported as
	// kproxy_metadata_refresh_errors_total.
	errors atomic.Int64

	// OnError, if set, is invoked synchronously from Run on every failed
	// background refresh. Useful for log surfacing without coupling the
	// cache to a concrete logger.
	OnError func(error)
}

// Source supplies fresh metadata. In production this is implemented by a
// kclient.Conn (see KClientSource); tests inject a stub.
type Source interface {
	Fetch(ctx context.Context) (*Snapshot, error)
}

type refreshCall struct {
	done chan struct{}
	snap *Snapshot
	err  error
}

// NewCache returns a Cache that will refresh from src every `refresh` when
// Run is called. The first Refresh is the caller's responsibility (call
// Refresh once before serving traffic, otherwise Get returns nil).
func NewCache(src Source, refresh time.Duration) *Cache {
	if refresh <= 0 {
		refresh = 30 * time.Second
	}
	return &Cache{source: src, refresh: refresh}
}

// Get returns the current snapshot or nil if no refresh has succeeded yet.
// The returned pointer is safe to read concurrently.
func (c *Cache) Get() *Snapshot { return c.cur.Load() }

// Age returns time.Since(snap.BuiltAt) for the current snapshot, or a very
// large duration if no snapshot has been published yet (so callers comparing
// against a freshness budget treat "never refreshed" as stale).
func (c *Cache) Age() time.Duration {
	snap := c.cur.Load()
	if snap == nil {
		return time.Hour * 24 * 365
	}
	return time.Since(snap.BuiltAt)
}

// Refresh forces a fetch and atomic publish. Concurrent callers share a single
// in-flight fetch. Returns the published snapshot.
func (c *Cache) Refresh(ctx context.Context) (*Snapshot, error) {
	c.mu.Lock()
	if c.flight != nil {
		call := c.flight
		c.mu.Unlock()
		select {
		case <-call.done:
			return call.snap, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	call := &refreshCall{done: make(chan struct{})}
	c.flight = call
	c.mu.Unlock()

	snap, err := c.source.Fetch(ctx)
	c.mu.Lock()
	c.flight = nil
	c.mu.Unlock()
	if err == nil && snap != nil {
		c.cur.Store(snap)
	}
	call.snap, call.err = snap, err
	close(call.done)
	return snap, err
}

// Run blocks driving periodic refreshes until ctx is canceled. Refresh
// failures are reported via OnError (if set) and counted in ErrorsTotal so
// callers can alert on "snapshot age > N seconds" without parsing logs.
func (c *Cache) Run(ctx context.Context) {
	t := time.NewTicker(c.refresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := c.Refresh(ctx); err != nil {
				c.errors.Add(1)
				if c.OnError != nil {
					c.OnError(err)
				}
			}
		}
	}
}

// ErrorsTotal returns the count of refresh failures since the cache was
// created. Wired into the Prometheus exporter as
// kproxy_metadata_refresh_errors_total.
func (c *Cache) ErrorsTotal() int64 { return c.errors.Load() }

// KClientSource adapts a kclient.Conn into a Source by issuing Metadata(nil)
// and reshaping the response.
type KClientSource struct {
	Conn *kclient.Conn
}

// Fetch implements Source.
func (s KClientSource) Fetch(ctx context.Context) (*Snapshot, error) {
	resp, err := s.Conn.Metadata(nil, false)
	if err != nil {
		return nil, err
	}
	snap := &Snapshot{
		BuiltAt:    time.Now(),
		ByTopic:    make(map[string][]int32, len(resp.Topics)),
		BrokerByID: make(map[int32]Broker, len(resp.Brokers)),
	}
	for _, b := range resp.Brokers {
		snap.BrokerByID[b.NodeID] = Broker{Host: b.Host, Port: b.Port}
	}
	for _, t := range resp.Topics {
		if t.NameNull || t.Name == "" || len(t.Partitions) == 0 {
			continue
		}
		ids := make([]int32, len(t.Partitions))
		for i, p := range t.Partitions {
			ids[i] = p.PartitionIndex
		}
		// Copy the topic name to avoid retaining the codec's input slice.
		snap.ByTopic[string([]byte(t.Name))] = ids
	}
	return snap, nil
}
