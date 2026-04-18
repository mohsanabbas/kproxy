package telemetry

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/mohsanabbas/kproxy/internal/kclient"
	"github.com/mohsanabbas/kproxy/internal/kwire"
)

// GroupRegistry yields the set of consumer-group ids the poller should
// describe. In production this is backed by the subscription store, which
// learns about groups from JoinGroup intercepts. Tests inject a static set.
type GroupRegistry interface {
	Groups() []string
}

// StaticGroups is a trivial GroupRegistry useful for tests and bootstrap.
type StaticGroups []string

// Groups implements GroupRegistry.
func (s StaticGroups) Groups() []string { return ([]string)(s) }

// Coordinator routes RPCs for a given group/topic. The poller calls these on
// every tick. In production a single kclient.Conn per broker suffices; the
// caller is responsible for opening conns to the right broker (group
// coordinator for OffsetFetch/DescribeGroups, partition leader for
// ListOffsets).
//
// For v1 we use a single *kclient.Conn for everything (the bootstrap broker
// will forward as needed; performance is not the goal — telemetry is a
// background job). We keep the interface narrow so a multi-broker pool can
// drop in later without touching the poller.
type Coordinator interface {
	DescribeGroups(req kwire.DescribeGroupsRequest) (kwire.DescribeGroupsResponse, error)
	OffsetFetch(req kwire.OffsetFetchRequest) (kwire.OffsetFetchResponse, error)
	ListOffsets(req kwire.ListOffsetsRequest) (kwire.ListOffsetsResponse, error)
}

// Static check: a real *kclient.Conn satisfies Coordinator.
var _ Coordinator = (*kclient.Conn)(nil)

// Poller refreshes a Holder by polling Kafka at a fixed interval.
//
// Each tick:
//  1. r.Groups() → list of group ids of interest.
//  2. DescribeGroups for that set → per-member assignment blobs (parsed to
//     learn each member's owned (topic, partition) set).
//  3. OffsetFetch for each group's owned (topic, partition) set → committed.
//  4. ListOffsets latest for the union of (topic, partition) → HWM.
//  5. Compute lag per (group, member) = sum(HWM - committed) over owned parts.
//  6. Atomically Store the new Snapshot.
//
// Errors at any step poison only that group's slice of the snapshot —
// surviving groups are still published. The previous snapshot stays visible
// while the next tick is in flight.
type Poller struct {
	Coord    Coordinator
	Registry GroupRegistry
	Holder   *Holder
	Interval time.Duration

	// OnError is called for every per-group failure with the group id and the
	// error. Optional; nil disables logging. The poller never blocks on this.
	OnError func(group string, err error)
}

// Run blocks driving polls until ctx is cancelled. The first poll happens
// immediately so callers don't have to wait one Interval for warm-up.
func (p *Poller) Run(ctx context.Context) {
	if p.Interval <= 0 {
		p.Interval = 15 * time.Second
	}
	if p.Holder == nil {
		p.Holder = &Holder{}
	}
	p.tick(ctx)
	t := time.NewTicker(p.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick(ctx)
		}
	}
}

// tick runs one full DescribeGroups → OffsetFetch → ListOffsets pass.
func (p *Poller) tick(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	groups := p.Registry.Groups()
	snap := &Snapshot{
		BuiltAt: time.Now(),
		Groups:  make(map[string]GroupLoad, len(groups)),
	}
	if len(groups) == 0 {
		p.Holder.Store(snap)
		return
	}

	// 1. DescribeGroups gives us per-member assignment blobs.
	dg, err := p.Coord.DescribeGroups(kwire.DescribeGroupsRequest{Groups: groups})
	if err != nil {
		p.reportAll(groups, err)
		// Publish previous-or-empty snapshot rather than overwrite with garbage.
		if p.Holder.Get() == nil {
			p.Holder.Store(snap)
		}
		return
	}

	// Per-group owned (topic, partition) sets, derived from MemberAssignment.
	type ownedKey struct {
		topic string
		part  int32
	}
	ownedByGroup := make(map[string]map[string][]kwire.TopicPartitions, len(dg.Groups))
	allParts := make(map[string]map[int32]struct{}) // topic → set of part ids (union across groups)

	for _, g := range dg.Groups {
		if g.ErrorCode != 0 {
			p.report(g.GroupID, kafkaErr("DescribeGroups", g.ErrorCode))
			continue
		}
		ownedByGroup[g.GroupID] = make(map[string][]kwire.TopicPartitions, len(g.Members))
		for _, m := range g.Members {
			if len(m.MemberAssignment) == 0 {
				continue
			}
			a, err := kwire.DecodeAssignment(m.MemberAssignment)
			if err != nil {
				p.report(g.GroupID, err)
				continue
			}
			// Copy strings out of the assignment blob — DescribeGroups
			// response buffer is reused by the kclient on the next RPC.
			cp := make([]kwire.TopicPartitions, 0, len(a.Partitions))
			for _, tp := range a.Partitions {
				topic := string([]byte(tp.Topic))
				ids := append([]int32(nil), tp.Partitions...)
				cp = append(cp, kwire.TopicPartitions{Topic: topic, Partitions: ids})
				ts, ok := allParts[topic]
				if !ok {
					ts = make(map[int32]struct{})
					allParts[topic] = ts
				}
				for _, id := range ids {
					ts[id] = struct{}{}
				}
			}
			ownedByGroup[g.GroupID][string([]byte(m.MemberID))] = cp
		}
	}

	// 2. ListOffsets latest for the union of (topic, partition).
	hwm := make(map[ownedKey]int64, 64)
	if len(allParts) > 0 {
		req := kwire.ListOffsetsRequest{
			ReplicaID:      -1,
			IsolationLevel: kwire.IsolationReadUncommitted,
			Topics:         make([]kwire.ListOffsetsTopic, 0, len(allParts)),
		}
		for topic, parts := range allParts {
			ids := make([]int32, 0, len(parts))
			for id := range parts {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			lp := make([]kwire.ListOffsetsPartition, len(ids))
			for i, id := range ids {
				lp[i] = kwire.ListOffsetsPartition{
					PartitionIndex:     id,
					CurrentLeaderEpoch: -1,
					Timestamp:          kwire.OffsetLatest,
				}
			}
			req.Topics = append(req.Topics, kwire.ListOffsetsTopic{Name: topic, Partitions: lp})
		}
		lo, err := p.Coord.ListOffsets(req)
		if err != nil {
			p.reportAll(groups, err)
		} else {
			for _, t := range lo.Topics {
				for _, pr := range t.Partitions {
					if pr.ErrorCode != 0 {
						continue
					}
					hwm[ownedKey{t.Name, pr.PartitionIndex}] = pr.Offset
				}
			}
		}
	}

	// 3. OffsetFetch (multi-group v8) for committed offsets.
	committed := make(map[string]map[ownedKey]int64, len(ownedByGroup))
	if len(ownedByGroup) > 0 {
		ofReq := kwire.OffsetFetchRequest{
			Groups:        make([]kwire.OffsetFetchGroup, 0, len(ownedByGroup)),
			RequireStable: false,
		}
		for gid, members := range ownedByGroup {
			topicMap := make(map[string]map[int32]struct{})
			for _, ownedList := range members {
				for _, tp := range ownedList {
					ts, ok := topicMap[tp.Topic]
					if !ok {
						ts = make(map[int32]struct{})
						topicMap[tp.Topic] = ts
					}
					for _, id := range tp.Partitions {
						ts[id] = struct{}{}
					}
				}
			}
			topics := make([]kwire.OffsetFetchTopicReq, 0, len(topicMap))
			for topic, parts := range topicMap {
				ids := make([]int32, 0, len(parts))
				for id := range parts {
					ids = append(ids, id)
				}
				sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
				topics = append(topics, kwire.OffsetFetchTopicReq{Name: topic, PartitionIndexes: ids})
			}
			ofReq.Groups = append(ofReq.Groups, kwire.OffsetFetchGroup{
				GroupID: gid,
				Topics:  topics,
			})
		}
		of, err := p.Coord.OffsetFetch(ofReq)
		if err != nil {
			p.reportAll(groups, err)
		} else {
			for _, g := range of.Groups {
				if g.ErrorCode != 0 {
					p.report(g.GroupID, kafkaErr("OffsetFetch", g.ErrorCode))
					continue
				}
				m := make(map[ownedKey]int64, 32)
				for _, t := range g.Topics {
					for _, pr := range t.Partitions {
						if pr.ErrorCode != 0 || pr.CommittedOffset < 0 {
							continue
						}
						m[ownedKey{t.Name, pr.PartitionIndex}] = pr.CommittedOffset
					}
				}
				committed[g.GroupID] = m
			}
		}
	}

	// 4. Compute lag per (group, member) and assemble the snapshot.
	for gid, members := range ownedByGroup {
		gl := GroupLoad{GroupID: gid, Members: make(map[string]MemberLoad, len(members))}
		commit := committed[gid]
		for memberID, ownedList := range members {
			ml := MemberLoad{MemberID: memberID}
			for _, tp := range ownedList {
				for _, id := range tp.Partitions {
					ml.AssignedPartitions++
					key := ownedKey{tp.Topic, id}
					h, hOK := hwm[key]
					c, cOK := commit[key]
					if hOK && cOK && h > c {
						ml.LagMessages += h - c
					}
				}
			}
			gl.Members[memberID] = ml
		}
		snap.Groups[gid] = gl
	}

	p.Holder.Store(snap)
}

func (p *Poller) report(group string, err error) {
	if p.OnError != nil && err != nil {
		p.OnError(group, err)
	}
}

func (p *Poller) reportAll(groups []string, err error) {
	if p.OnError == nil || err == nil {
		return
	}
	for _, g := range groups {
		p.OnError(g, err)
	}
}

// kafkaErr wraps a non-zero Kafka error code with context. We don't pull in a
// full error-code table for v1; the numeric code is enough for ops.
func kafkaErr(op string, code int16) error {
	return &kErr{op: op, code: code}
}

type kErr struct {
	op   string
	code int16
}

func (e *kErr) Error() string { return e.op + ": kafka error code " + itoa(int64(e.code)) }

// itoa avoids an strconv import for a tiny formatting helper. Allocates a
// short string per error path; not on the hot tick path beyond error reports.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// Compile-time guard: a Holder satisfies Source.
var _ Source = (*Holder)(nil)

// SyncRegistry is a tiny mutex-protected GroupRegistry useful for the
// subscription store wiring (and for the poller's tests). We keep it here so
// telemetry has no dependency on the (yet-unbuilt) subscription package.
type SyncRegistry struct {
	mu     sync.Mutex
	groups map[string]struct{}
}

// NewSyncRegistry returns an empty SyncRegistry.
func NewSyncRegistry() *SyncRegistry {
	return &SyncRegistry{groups: make(map[string]struct{})}
}

// Add records a group id.
func (r *SyncRegistry) Add(group string) {
	r.mu.Lock()
	r.groups[group] = struct{}{}
	r.mu.Unlock()
}

// Remove drops a group id.
func (r *SyncRegistry) Remove(group string) {
	r.mu.Lock()
	delete(r.groups, group)
	r.mu.Unlock()
}

// Groups implements GroupRegistry. The returned slice is a fresh copy and is
// owned by the caller.
func (r *SyncRegistry) Groups() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.groups))
	for g := range r.groups {
		out = append(out, g)
	}
	sort.Strings(out)
	return out
}
