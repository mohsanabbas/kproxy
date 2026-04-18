// Package telemetry exposes per-(group, member) load measurements that the
// planner consumes when deciding partition assignment.
//
// In v1 the only signal is committed-offset lag, computed Kafka-natively by
// polling DescribeGroups → OffsetFetch → ListOffsets. Latency is left at zero
// pending KIP-714 client-telemetry support in v2.
//
// The hot read path is a single atomic.Pointer load of an immutable Snapshot;
// the poll path runs in its own goroutine and never blocks the proxy.
package telemetry

import (
	"sync/atomic"
	"time"
)

// MemberLoad is the current load measurement for a single consumer member.
//
// All fields are populated by the poller from authoritative Kafka data; the
// planner treats them as read-only.
type MemberLoad struct {
	// MemberID is the Kafka group member id (assigned by the coordinator on
	// JoinGroup). Stable for the lifetime of the member's session.
	MemberID string

	// AssignedPartitions is the count of partitions currently owned by this
	// member, summed across all topics. Used as a tie-breaker when lag is
	// equal across members.
	AssignedPartitions int

	// LagMessages is the total committed-offset lag (HWM - committed) summed
	// across the member's owned partitions. Higher = more behind = lower
	// capacity for new partitions.
	LagMessages int64

	// LatencyMicros is reserved for v2; always 0 in v1.
	LatencyMicros int64
}

// GroupLoad aggregates member loads for one consumer group.
type GroupLoad struct {
	GroupID string
	Members map[string]MemberLoad
}

// Snapshot is the immutable load picture published by the poller. The
// pointer is swapped atomically; callers MUST treat it as read-only.
type Snapshot struct {
	BuiltAt time.Time
	Groups  map[string]GroupLoad
}

// Source is anything that can produce a Snapshot. Used by the planner and by
// the admin endpoint. Concrete implementation: KafkaNativePoller.
type Source interface {
	Get() *Snapshot
}

// Holder is the simple atomic publish/load box. Pollers Store() it, callers
// Load() it. The zero value is safe (Get returns nil until first Store).
type Holder struct {
	cur atomic.Pointer[Snapshot]
}

// Get returns the current snapshot or nil if none has been published.
func (h *Holder) Get() *Snapshot { return h.cur.Load() }

// Store publishes a new snapshot. Callers must not mutate s after this call.
func (h *Holder) Store(s *Snapshot) { h.cur.Store(s) }

// EmptySnapshot is a safe non-nil zero value the planner can use as a default
// when the poller hasn't produced anything yet. Returned by the planner in the
// "cold start" case to avoid nil-map panics in callers that range over Groups.
func EmptySnapshot() *Snapshot {
	return &Snapshot{BuiltAt: time.Now(), Groups: map[string]GroupLoad{}}
}
