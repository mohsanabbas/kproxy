// Package subscription tracks per-(group, member) subscriptions learned from
// JoinGroup intercepts. The planner reads it on SyncGroup intercepts to
// decide partition assignment without re-inferring subscriptions from frame
// payloads (which is fragile under cooperative-sticky upgrades).
//
// The store is shared across all proxy connections. Concurrency is a single
// RWMutex — entries are written rarely (once per JoinGroup per member, every
// few seconds at most) and read on every SyncGroup. Lock contention is not
// expected to be a hotspot; if it ever is, switch to per-group sharding.
package subscription

import (
	"sync"
	"time"

	"github.com/mohsanabbas/kproxy/internal/kwire"
)

// Subscription is a single member's subscription record. All fields are owned
// by the store (deep-copied from the inbound frame on Put).
type Subscription struct {
	GroupID         string
	MemberID        string
	Topics          []string
	OwnedPartitions []kwire.TopicPartitions
	GenerationID    int32
	UpdatedAt       time.Time
}

// Store is the in-memory subscription database. Zero value is unusable; use
// NewStore.
type Store struct {
	mu  sync.RWMutex
	max int
	// keyed by group → memberID → Subscription
	byGroup map[string]map[string]*Subscription

	// onChange is called (without the lock) after every Put or RemoveMember
	// with the affected group id. cmd/kproxy uses this to keep
	// telemetry.SyncRegistry in sync.
	onChange func(group string)
}

// NewStore returns an empty Store bounded to maxMembers across all groups.
// maxMembers <=0 disables the cap.
func NewStore(maxMembers int) *Store {
	return &Store{
		max:     maxMembers,
		byGroup: make(map[string]map[string]*Subscription),
	}
}

// SetOnChange registers a callback fired after any mutation. Must be set
// before the store is exposed to traffic; it is not concurrency-safe to call
// at runtime.
func (s *Store) SetOnChange(fn func(group string)) { s.onChange = fn }

// Put records or updates a subscription. The Topics and OwnedPartitions
// slices are deep-copied so the caller may reuse its frame buffer.
//
// Returns false if the cap is exceeded (in which case the entry is NOT
// added; the caller should let the JoinGroup proceed unrewritten).
func (s *Store) Put(sub Subscription) bool {
	cp := Subscription{
		GroupID:         string([]byte(sub.GroupID)),
		MemberID:        string([]byte(sub.MemberID)),
		Topics:          dedupCopy(sub.Topics),
		OwnedPartitions: cloneTopicParts(sub.OwnedPartitions),
		GenerationID:    sub.GenerationID,
		UpdatedAt:       time.Now(),
	}
	s.mu.Lock()
	g, ok := s.byGroup[cp.GroupID]
	if !ok {
		if s.max > 0 && s.totalLocked() >= s.max {
			s.mu.Unlock()
			return false
		}
		g = make(map[string]*Subscription)
		s.byGroup[cp.GroupID] = g
	}
	if _, exists := g[cp.MemberID]; !exists && s.max > 0 && s.totalLocked() >= s.max {
		s.mu.Unlock()
		return false
	}
	g[cp.MemberID] = &cp
	s.mu.Unlock()
	if s.onChange != nil {
		s.onChange(cp.GroupID)
	}
	return true
}

// Get returns the subscription for (group, member) or nil if absent.
// The returned pointer is read-only; callers MUST NOT mutate it.
func (s *Store) Get(group, member string) *Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if g := s.byGroup[group]; g != nil {
		return g[member]
	}
	return nil
}

// MembersOf returns the set of member ids known for a group. The returned
// slice is a fresh copy.
func (s *Store) MembersOf(group string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	g := s.byGroup[group]
	if len(g) == 0 {
		return nil
	}
	out := make([]string, 0, len(g))
	for m := range g {
		out = append(out, m)
	}
	return out
}

// Groups returns the set of group ids known to the store. Used by the
// telemetry poller's GroupRegistry. Fresh slice; safe to mutate.
func (s *Store) Groups() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.byGroup))
	for g := range s.byGroup {
		out = append(out, g)
	}
	return out
}

// RemoveMember drops a single (group, member) entry. The group entry itself
// is removed when its last member leaves.
func (s *Store) RemoveMember(group, member string) {
	s.mu.Lock()
	g, ok := s.byGroup[group]
	if !ok {
		s.mu.Unlock()
		return
	}
	if _, present := g[member]; !present {
		s.mu.Unlock()
		return
	}
	delete(g, member)
	if len(g) == 0 {
		delete(s.byGroup, group)
	}
	s.mu.Unlock()
	if s.onChange != nil {
		s.onChange(group)
	}
}

// Len returns the total number of (group, member) entries. O(groups).
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.totalLocked()
}

func (s *Store) totalLocked() int {
	n := 0
	for _, g := range s.byGroup {
		n += len(g)
	}
	return n
}

// dedupCopy returns a fresh slice with input topics deduplicated and copied.
// Order-preserving on first occurrence.
func dedupCopy(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, t := range in {
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, string([]byte(t)))
	}
	return out
}

func cloneTopicParts(in []kwire.TopicPartitions) []kwire.TopicPartitions {
	if len(in) == 0 {
		return nil
	}
	out := make([]kwire.TopicPartitions, len(in))
	for i, tp := range in {
		out[i] = kwire.TopicPartitions{
			Topic:      string([]byte(tp.Topic)),
			Partitions: append([]int32(nil), tp.Partitions...),
		}
	}
	return out
}
