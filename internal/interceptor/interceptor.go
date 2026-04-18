// Package interceptor wires the proxy's Interceptor interface to the rest of
// kproxy: subscription tracking on JoinGroup, planning + assignment-rewrite on
// SyncGroup, and topology rewriting on Metadata/FindCoordinator.
//
// One Interceptor is shared across all proxy connections.
package interceptor

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/mohsanabbas/kproxy/internal/kwire"
	"github.com/mohsanabbas/kproxy/internal/metadata"
	"github.com/mohsanabbas/kproxy/internal/obs"
	"github.com/mohsanabbas/kproxy/internal/plan"
	"github.com/mohsanabbas/kproxy/internal/planner"
	"github.com/mohsanabbas/kproxy/internal/proxy"
	"github.com/mohsanabbas/kproxy/internal/subscription"
	"github.com/mohsanabbas/kproxy/internal/telemetry"
	"github.com/mohsanabbas/kproxy/internal/topology"
)

// Deps bundles every collaborator the Interceptor needs. Only Topology is
// strictly required for Metadata/FindCoord rewriting; the rest enable
// JoinGroup subscription capture and SyncGroup planning.
type Deps struct {
	Topology     *topology.Mapping
	Metadata     *metadata.Cache
	Subscription *subscription.Store
	Telemetry    telemetry.Source
	Planner      *planner.Pool
	PlanTimeout  time.Duration
	Metrics      *obs.Metrics
}

// Interceptor is the production proxy.Interceptor implementation. Compose-free
// for v1: a single struct dispatches by API key.
type Interceptor struct {
	deps Deps

	// syncGroupRewrites counts how many SyncGroup requests were rewritten.
	// Exposed via SyncGroupRewrites() for test assertions production code
	// observes via deps.Metrics.PlanCount.
	syncGroupRewrites atomic.Int64
}

// New returns a ready-to-use Interceptor.
func New(d Deps) *Interceptor {
	if d.PlanTimeout <= 0 {
		d.PlanTimeout = 2 * time.Second
	}
	return &Interceptor{deps: d}
}

// OnRequest implements proxy.Interceptor. It dispatches by API key and
// returns a Pending whose Rewrite (if non-nil) will be invoked on the
// downstream pump for the matching response.
func (i *Interceptor) OnRequest(ctx context.Context, h kwire.RequestHeader, body []byte) *proxy.Pending {
	if i.deps.Metrics != nil {
		i.deps.Metrics.InterceptsTotal.Add(1)
	}
	switch h.APIKey {
	case kwire.APIMetadata:
		return i.onMetadata(h)
	case kwire.APIFindCoordinator:
		return i.onFindCoordinator(h)
	case kwire.APIJoinGroup:
		return i.onJoinGroup(h, body)
	case kwire.APISyncGroup:
		return i.onSyncGroup(ctx, h, body)
	}
	if i.deps.Metrics != nil {
		i.deps.Metrics.InterceptsPassthru.Add(1)
	}
	return nil
}

// onMetadata captures the request only insofar as we need to know the version
// to decode the response. Rewriting happens in the response path.
func (i *Interceptor) onMetadata(h kwire.RequestHeader) *proxy.Pending {
	if i.deps.Topology == nil || i.deps.Topology.Len() == 0 {
		if i.deps.Metrics != nil {
			i.deps.Metrics.InterceptsPassthru.Add(1)
		}
		return nil
	}
	return &proxy.Pending{
		APIKey:     h.APIKey,
		APIVersion: h.APIVersion,
		Rewrite:    i.rewriteMetadataResp,
	}
}

func (i *Interceptor) rewriteMetadataResp(dst, body []byte, p *proxy.Pending) ([]byte, error) {
	r, err := kwire.DecodeMetadataResponse(body, p.APIVersion)
	if err != nil {
		i.bumpErr()
		return nil, err
	}
	n := topology.RewriteMetadataResponse(&r, i.deps.Topology)
	if i.deps.Metrics != nil {
		i.deps.Metrics.InterceptsRewrote.Add(1)
		if missed := len(r.Brokers) - n; missed > 0 {
			i.deps.Metrics.UnmappedBrokers.Add(int64(missed))
		}
	}
	return kwire.AppendMetadataResponse(dst, r), nil
}

func (i *Interceptor) onFindCoordinator(h kwire.RequestHeader) *proxy.Pending {
	if i.deps.Topology == nil || i.deps.Topology.Len() == 0 {
		if i.deps.Metrics != nil {
			i.deps.Metrics.InterceptsPassthru.Add(1)
		}
		return nil
	}
	return &proxy.Pending{
		APIKey:     h.APIKey,
		APIVersion: h.APIVersion,
		Rewrite:    i.rewriteFindCoordResp,
	}
}

func (i *Interceptor) rewriteFindCoordResp(dst, body []byte, p *proxy.Pending) ([]byte, error) {
	r, err := kwire.DecodeFindCoordinatorResponse(body, p.APIVersion)
	if err != nil {
		i.bumpErr()
		return nil, err
	}
	topology.RewriteFindCoordinatorResponse(&r, i.deps.Topology)
	if i.deps.Metrics != nil {
		i.deps.Metrics.InterceptsRewrote.Add(1)
	}
	return kwire.AppendFindCoordinatorResponse(dst, r), nil
}

// onJoinGroup decodes the request, extracts the member's protocol metadata
// (ConsumerProtocolSubscription) for the first protocol entry, and stores it
// in the subscription store. We do NOT rewrite the response.
func (i *Interceptor) onJoinGroup(h kwire.RequestHeader, body []byte) *proxy.Pending {
	if i.deps.Subscription == nil {
		if i.deps.Metrics != nil {
			i.deps.Metrics.InterceptsPassthru.Add(1)
		}
		return nil
	}
	jg, err := kwire.DecodeJoinGroupRequest(body, h.APIVersion)
	if err != nil {
		i.bumpErr()
		return nil
	}
	if len(jg.Protocols) == 0 || jg.MemberID == "" {
		// First JoinGroup from a member has empty MemberID; the broker
		// assigns it in the response. We capture on subsequent rebalances.
		if i.deps.Metrics != nil {
			i.deps.Metrics.InterceptsPassthru.Add(1)
		}
		return nil
	}
	// Use the first protocol entry's metadata. Real consumers send one.
	sub, err := kwire.DecodeSubscription(jg.Protocols[0].Metadata)
	if err != nil {
		i.bumpErr()
		return nil
	}
	i.deps.Subscription.Put(subscription.Subscription{
		GroupID:         jg.Group,
		MemberID:        jg.MemberID,
		Topics:          sub.Topics,
		OwnedPartitions: sub.OwnedPartitions,
		GenerationID:    sub.GenerationID,
	})
	if i.deps.Metrics != nil {
		i.deps.Metrics.InterceptsPassthru.Add(1) // counted as observed-only
		i.deps.Metrics.SubscriptionLen.Store(int64(i.deps.Subscription.Len()))
	}
	return nil
}

// onSyncGroup intercepts only when this is the leader's SyncGroup (the only
// member sending non-empty Assignments). We compute a fresh plan and rewrite
// the request.Assignments before forwarding to the broker, so that members
// receive our planned assignment in their SyncGroupResponse.
func (i *Interceptor) onSyncGroup(ctx context.Context, h kwire.RequestHeader, body []byte) *proxy.Pending {
	if i.deps.Planner == nil || i.deps.Subscription == nil {
		if i.deps.Metrics != nil {
			i.deps.Metrics.InterceptsPassthru.Add(1)
		}
		return nil
	}
	sg, err := kwire.DecodeSyncGroupRequest(body, h.APIVersion)
	if err != nil {
		i.bumpErr()
		return nil
	}
	// Only the leader sends Assignments. Followers send empty.
	if len(sg.Assignments) == 0 {
		if i.deps.Metrics != nil {
			i.deps.Metrics.InterceptsPassthru.Add(1)
		}
		return nil
	}

	// Build inputs: members from the leader's assignment list; subscriptions
	// from our store; partitions from metadata cache; lag from telemetry.
	members := make([]plan.Member, 0, len(sg.Assignments))
	for _, a := range sg.Assignments {
		sub := i.deps.Subscription.Get(sg.Group, a.MemberID)
		if sub == nil {
			continue
		}
		var lag int64
		if i.deps.Telemetry != nil {
			if snap := i.deps.Telemetry.Get(); snap != nil {
				if gl, ok := snap.Groups[sg.Group]; ok {
					lag = gl.Members[a.MemberID].LagMessages
				}
			}
		}
		members = append(members, plan.Member{
			MemberID: a.MemberID,
			Topics:   sub.Topics,
			Lag:      lag,
		})
	}
	if len(members) == 0 {
		if i.deps.Metrics != nil {
			i.deps.Metrics.InterceptsPassthru.Add(1)
		}
		return nil
	}

	parts := map[string][]int32{}
	if i.deps.Metadata != nil {
		if snap := i.deps.Metadata.Get(); snap != nil {
			for _, m := range members {
				for _, t := range m.Topics {
					if _, ok := parts[t]; ok {
						continue
					}
					if ps := snap.ByTopic[t]; len(ps) > 0 {
						parts[t] = ps
					}
				}
			}
		}
	}
	if len(parts) == 0 {
		// No partition info — fall back to passthrough rather than emit a
		// degenerate plan that gives every consumer nothing.
		if i.deps.Metrics != nil {
			i.deps.Metrics.InterceptsPassthru.Add(1)
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, i.deps.PlanTimeout)
	pl, dur, err := i.deps.Planner.Plan(ctx, plan.Inputs{
		GroupID:    sg.Group,
		Members:    members,
		Partitions: parts,
	})
	cancel()
	if err != nil {
		if i.deps.Metrics != nil {
			i.deps.Metrics.InterceptsTimeout.Add(1)
		}
		return nil
	}
	if i.deps.Metrics != nil {
		i.deps.Metrics.PlanCount.Add(1)
		i.deps.Metrics.PlanDurationNanosSum.Add(dur.Nanoseconds())
	}

	// Rewrite the leader's Assignments[]: replace each member's blob with
	// our planned per-member assignment, then re-encode the SyncGroupRequest
	// body and hand it to proxy.Conn via Pending.RewriteRequest. The broker
	// will fan these out to followers, so every member receives the planned
	// assignment in their own SyncGroupResponse.
	newSG := sg
	newSG.Assignments = make([]kwire.SyncGroupAssignment, 0, len(sg.Assignments))
	for _, a := range sg.Assignments {
		mp := pl[a.MemberID]
		newSG.Assignments = append(newSG.Assignments, kwire.SyncGroupAssignment{
			MemberID:         a.MemberID,
			MemberAssignment: encodeAssignment(mp),
		})
	}
	rewrittenReq := kwire.AppendSyncGroupRequest(nil, newSG)

	i.syncGroupRewrites.Add(1)
	return &proxy.Pending{
		APIKey:         h.APIKey,
		APIVersion:     h.APIVersion,
		RewriteRequest: rewrittenReq,
	}
}

// SyncGroupRewrites returns the number of SyncGroup requests rewritten by
// this Interceptor. Useful for test assertions; production code observes via
// Metrics.PlanCount.
func (i *Interceptor) SyncGroupRewrites() int64 { return i.syncGroupRewrites.Load() }

func (i *Interceptor) bumpErr() {
	if i.deps.Metrics != nil {
		i.deps.Metrics.InterceptsError.Add(1)
	}
}

func encodeAssignment(byTopic map[string][]int32) []byte {
	a := kwire.Assignment{Version: 0, UserDataNull: true}
	for topic, parts := range byTopic {
		ps := make([]int32, len(parts))
		copy(ps, parts)
		a.Partitions = append(a.Partitions, kwire.TopicPartitions{Topic: topic, Partitions: ps})
	}
	return kwire.AppendAssignment(nil, a)
}
