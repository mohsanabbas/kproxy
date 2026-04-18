// Package plan computes a per-group consumer assignment: which member owns
// which (topic, partition). All inputs are taken by value (or as read-only
// pointers); planning is a pure function suitable for the bounded worker pool
// in package planner.
//
// The algorithm is capacity-weighted sticky:
//
//  1. Start from the previous plan and retain (member, topic, partition)
//     where the member is still subscribed to that topic. Anything else goes
//     to the "free" pool.
//  2. Distribute the free pool across subscribed members in inverse-lag order:
//     members with lower lag get more partitions. Ties broken by member id
//     for determinism.
//  3. Feasibility pass: if partitions ≥ members, every subscribed member
//     must own ≥1 partition. Steal from over-loaded members to fill gaps.
//
// Output is a Plan, which the proxy renders into a SyncGroup response.
package plan

import (
	"sort"

	"github.com/mohsanabbas/kproxy/internal/kwire"
)

// Member is the input shape for one consumer member.
type Member struct {
	MemberID string
	Topics   []string
	Lag      int64 // current committed-offset lag (capacity = inverse)
}

// Inputs bundles everything the planner needs to produce a Plan.
type Inputs struct {
	GroupID    string
	Members    []Member
	Partitions map[string][]int32 // topic → partition ids; from metadata cache
	Previous   Plan               // last known assignment for stickiness; may be nil
}

// Plan maps memberID → topic → []partition. Values are owned by the Plan and
// safe to mutate by the caller (callers typically re-encode them into a
// SyncGroup response and discard the Plan).
type Plan map[string]map[string][]int32

// Add assigns (topic, partition) to member, growing inner maps as needed.
func (p Plan) Add(member, topic string, part int32) {
	tm, ok := p[member]
	if !ok {
		tm = make(map[string][]int32)
		p[member] = tm
	}
	tm[topic] = append(tm[topic], part)
}

// Compute produces a Plan for in. Determinism: equal inputs produce equal
// outputs regardless of map iteration order.
func Compute(in Inputs) Plan {
	out := make(Plan, len(in.Members))
	if len(in.Members) == 0 {
		return out
	}

	// Determinism: index members by id sorted lexicographically.
	memberIDs := make([]string, len(in.Members))
	memberByID := make(map[string]Member, len(in.Members))
	for i, m := range in.Members {
		memberIDs[i] = m.MemberID
		memberByID[m.MemberID] = m
	}
	sort.Strings(memberIDs)

	// Topic → set of subscribed member ids (for fast lookup).
	subscribers := make(map[string]map[string]struct{})
	for _, mid := range memberIDs {
		for _, t := range memberByID[mid].Topics {
			s, ok := subscribers[t]
			if !ok {
				s = make(map[string]struct{})
				subscribers[t] = s
			}
			s[mid] = struct{}{}
		}
	}

	// Step 1: stickiness — keep what we can from the previous plan.
	free := freePool{} // (topic, part) pairs to distribute
	for topic, parts := range in.Partitions {
		for _, p := range parts {
			free.add(topic, p)
		}
	}
	if in.Previous != nil {
		for _, mid := range memberIDs {
			prev := in.Previous[mid]
			if prev == nil {
				continue
			}
			for topic, parts := range prev {
				if _, sub := subscribers[topic][mid]; !sub {
					continue
				}
				for _, p := range parts {
					if free.remove(topic, p) {
						out.Add(mid, topic, p)
					}
				}
			}
		}
	}

	// Step 2: capacity-weighted distribution. We use integer weights:
	//   weight = max(1, scale - min(scale-1, lag/lagDivisor))
	// so members with lag=0 get the highest weight, and weights stay bounded.
	const (
		scale       = 100
		lagDivisor  = 1000 // 1k messages of lag drops one weight unit
	)
	weight := make(map[string]int, len(memberIDs))
	for _, mid := range memberIDs {
		m := memberByID[mid]
		w := scale
		if m.Lag > 0 {
			drop := int(m.Lag / lagDivisor)
			if drop >= scale-1 {
				drop = scale - 1
			}
			w -= drop
		}
		if w < 1 {
			w = 1
		}
		weight[mid] = w
	}

	// Distribute the free pool. We iterate the free pool in deterministic
	// (topic, partition) order. For each partition, pick the eligible member
	// (subscribed to the topic) with the highest remaining weight quota.
	// Quota is updated by deducting 1 per assigned partition; on tie we pick
	// the member with the lowest current load, then the lowest id.
	load := make(map[string]int, len(memberIDs))
	for mid, parts := range out {
		for _, ps := range parts {
			load[mid] += len(ps)
		}
	}
	free.forEach(func(topic string, p int32) {
		subs := subscribers[topic]
		if len(subs) == 0 {
			return // no one subscribed to this topic
		}
		bestID := ""
		bestScore := -1
		bestLoad := 0
		for _, mid := range memberIDs {
			if _, ok := subs[mid]; !ok {
				continue
			}
			// Score = weight - (load * scaleAdjust). Higher = better candidate.
			// We bias toward low-load members to spread evenly when weights
			// are equal.
			score := weight[mid]*8 - load[mid]
			if bestID == "" || score > bestScore || (score == bestScore && load[mid] < bestLoad) {
				bestID = mid
				bestScore = score
				bestLoad = load[mid]
			}
		}
		if bestID != "" {
			out.Add(bestID, topic, p)
			load[bestID]++
		}
	})

	// Step 3: feasibility — every subscribed member should own ≥1 partition
	// when partitions ≥ members for at least one of their subscribed topics.
	// We steal from the most-loaded eligible member to a starved one.
	for _, mid := range memberIDs {
		if hasAny(out[mid]) {
			continue
		}
		// Find a topic this member subscribes to that has ≥ members partitions.
		for _, topic := range memberByID[mid].Topics {
			parts := in.Partitions[topic]
			subs := subscribers[topic]
			if len(parts) < len(subs) {
				continue
			}
			// Steal the last partition from the most-loaded subscriber.
			donorID := ""
			donorLoad := -1
			for did := range subs {
				if did == mid {
					continue
				}
				if l := load[did]; l > donorLoad {
					donorID = did
					donorLoad = l
				}
			}
			if donorID == "" {
				continue
			}
			donorParts := out[donorID][topic]
			if len(donorParts) <= 1 {
				continue
			}
			stolen := donorParts[len(donorParts)-1]
			out[donorID][topic] = donorParts[:len(donorParts)-1]
			load[donorID]--
			out.Add(mid, topic, stolen)
			load[mid]++
			break
		}
	}

	// Sort partition lists for determinism (rendering into SyncGroup must be
	// stable across rebalances of identical inputs, otherwise tests flake).
	for _, byTopic := range out {
		for topic, ps := range byTopic {
			sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
			byTopic[topic] = ps
		}
	}

	return out
}

// ToAssignments renders a Plan into the per-member kwire.Assignment slices
// the SyncGroup response carries. Order is deterministic: members and topics
// sorted lexicographically.
func ToAssignments(p Plan) []MemberAssignment {
	ids := make([]string, 0, len(p))
	for mid := range p {
		ids = append(ids, mid)
	}
	sort.Strings(ids)
	out := make([]MemberAssignment, 0, len(ids))
	for _, mid := range ids {
		topics := p[mid]
		topicNames := make([]string, 0, len(topics))
		for t := range topics {
			topicNames = append(topicNames, t)
		}
		sort.Strings(topicNames)
		tps := make([]kwire.TopicPartitions, 0, len(topicNames))
		for _, t := range topicNames {
			tps = append(tps, kwire.TopicPartitions{Topic: t, Partitions: append([]int32(nil), topics[t]...)})
		}
		out = append(out, MemberAssignment{MemberID: mid, Partitions: tps})
	}
	return out
}

// MemberAssignment pairs a memberID with its assigned topic-partitions.
type MemberAssignment struct {
	MemberID   string
	Partitions []kwire.TopicPartitions
}

func hasAny(byTopic map[string][]int32) bool {
	for _, ps := range byTopic {
		if len(ps) > 0 {
			return true
		}
	}
	return false
}

// freePool is an ordered set of (topic, partition) pairs supporting O(1)
// remove. We keep insertion order for determinism: Compute iterates topics
// and partitions in sorted order before adding to the pool.
type freePool struct {
	keys  []freeKey                // ordered; nil entries mean removed
	index map[freeKey]int          // key → position in keys; missing = removed
}

type freeKey struct {
	topic string
	part  int32
}

func (f *freePool) add(topic string, p int32) {
	if f.index == nil {
		f.index = make(map[freeKey]int)
	}
	k := freeKey{topic: topic, part: p}
	if _, exists := f.index[k]; exists {
		return
	}
	f.index[k] = len(f.keys)
	f.keys = append(f.keys, k)
}

func (f *freePool) remove(topic string, p int32) bool {
	k := freeKey{topic: topic, part: p}
	idx, ok := f.index[k]
	if !ok {
		return false
	}
	f.keys[idx] = freeKey{} // tombstone
	delete(f.index, k)
	return true
}

// forEach calls fn on each remaining (topic, partition) in deterministic order
// (sorted by topic, then partition).
func (f *freePool) forEach(fn func(topic string, p int32)) {
	remaining := make([]freeKey, 0, len(f.keys))
	for _, k := range f.keys {
		if _, ok := f.index[k]; ok {
			remaining = append(remaining, k)
		}
	}
	sort.Slice(remaining, func(i, j int) bool {
		if remaining[i].topic != remaining[j].topic {
			return remaining[i].topic < remaining[j].topic
		}
		return remaining[i].part < remaining[j].part
	})
	for _, k := range remaining {
		fn(k.topic, k.part)
	}
}
