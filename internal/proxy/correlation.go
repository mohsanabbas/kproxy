package proxy

import (
	"sync"
	"time"
)

// Pending describes a request frame that the proxy has forwarded upstream and
// for which it intends to inspect or rewrite the response when it returns.
//
// The proxy may also register a Pending purely for telemetry (e.g. counting
// the latency of a JoinGroup) without rewriting; in that case Rewrite is nil.
type Pending struct {
	APIKey     int16
	APIVersion int16
	CorrelID   int32

	// Sent is the time the upstream pump wrote the request. Used for latency
	// metrics and for eviction of long-stale entries.
	Sent time.Time

	// Rewrite, if non-nil, is invoked with the decoded response body (i.e.
	// after the response header has been stripped) and must return the bytes
	// to send back to the client. It may return body unchanged for inspection-
	// only intercepts.
	Rewrite RewriteFunc

	// RewriteRequest, if non-nil, replaces the request payload (everything
	// after the request header) before the frame is forwarded upstream. The
	// request header itself is preserved so correlation tracking still works
	// from the client's point of view.
	//
	// This is the hook the SyncGroup interceptor uses to substitute the
	// leader's Assignments[] with the planner's assignments before the broker
	// fans them out to followers.
	RewriteRequest []byte
}

// RewriteFunc transforms a response body. dst is a scratch slice the rewriter
// may append to and return; if it returns body unchanged, the proxy forwards
// the original frame without copy.
type RewriteFunc func(dst, body []byte, p *Pending) ([]byte, error)

// Tracker holds in-flight pending requests keyed by correlation id. It is
// owned by exactly one Conn (one per direction-pair) and accessed by both the
// upstream and downstream pump goroutines, so accesses are locked.
//
// Why a plain map rather than an LRU/linked-list+map: lookup is by random
// correlation id (the response's id), not by recency, so the list pointers
// would be pure overhead. Removal is also random (responses arrive out of
// order). The only operation that benefits from ordering is age-based
// eviction, which is rare (entries are normally removed by Take when the
// response arrives) and bounded by maxSize. We keep the map and amortise the
// O(n) sweep so it doesn't run on every Register.
type Tracker struct {
	mu          sync.Mutex
	entries     map[int32]*Pending
	maxSize     int
	maxAge      time.Duration
	sweepEvery  time.Duration
	lastSweepNS int64 // monotonic ns of the most recent expiry sweep
}

// NewTracker builds a Tracker bounded to maxSize concurrent in-flight pending
// requests. Entries older than maxAge are evicted on the next call. maxSize
// <=0 defaults to 4096; maxAge <=0 defaults to 5 minutes.
func NewTracker(maxSize int, maxAge time.Duration) *Tracker {
	if maxSize <= 0 {
		maxSize = 4096
	}
	if maxAge <= 0 {
		maxAge = 5 * time.Minute
	}
	// Sweep at least once every maxAge/4, capped at 1s to avoid spinning on
	// long-lived deployments. With the default 5-min maxAge we sweep every
	// second; with a 10ms test value we sweep every ~2.5ms.
	sweep := min(maxAge/4, time.Second)
	if sweep <= 0 {
		sweep = time.Microsecond
	}
	return &Tracker{
		entries:    make(map[int32]*Pending),
		maxSize:    maxSize,
		maxAge:     maxAge,
		sweepEvery: sweep,
	}
}

// Register adds p to the tracker. Returns false if the tracker is full or if
// a pending entry with the same correlation id already exists (which would
// indicate a client correlation-id reuse bug). The caller is expected to drop
// the registration (i.e. proceed as passthrough) when this returns false.
func (t *Tracker) Register(p *Pending) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	if len(t.entries) >= t.maxSize || now.UnixNano()-t.lastSweepNS >= int64(t.sweepEvery) {
		t.evictExpiredLocked(now)
		t.lastSweepNS = now.UnixNano()
	}
	if len(t.entries) >= t.maxSize {
		return false
	}
	if _, exists := t.entries[p.CorrelID]; exists {
		return false
	}
	t.entries[p.CorrelID] = p
	return true
}

// Take removes and returns the pending entry for correlID, or nil if none.
func (t *Tracker) Take(correlID int32) *Pending {
	t.mu.Lock()
	defer t.mu.Unlock()
	p, ok := t.entries[correlID]
	if !ok {
		return nil
	}
	delete(t.entries, correlID)
	return p
}

// Len returns the number of in-flight entries (for metrics/tests).
func (t *Tracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.entries)
}

func (t *Tracker) evictExpiredLocked(now time.Time) {
	for id, p := range t.entries {
		if now.Sub(p.Sent) > t.maxAge {
			delete(t.entries, id)
		}
	}
}
