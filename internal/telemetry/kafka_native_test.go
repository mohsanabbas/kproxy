package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/mohsanabbas/kproxy/internal/kwire"
)

// fakeCoord lets tests stage canned responses without going through TCP.
type fakeCoord struct {
	dg func(kwire.DescribeGroupsRequest) (kwire.DescribeGroupsResponse, error)
	of func(kwire.OffsetFetchRequest) (kwire.OffsetFetchResponse, error)
	lo func(kwire.ListOffsetsRequest) (kwire.ListOffsetsResponse, error)
}

func (f *fakeCoord) DescribeGroups(r kwire.DescribeGroupsRequest) (kwire.DescribeGroupsResponse, error) {
	return f.dg(r)
}
func (f *fakeCoord) OffsetFetch(r kwire.OffsetFetchRequest) (kwire.OffsetFetchResponse, error) {
	return f.of(r)
}
func (f *fakeCoord) ListOffsets(r kwire.ListOffsetsRequest) (kwire.ListOffsetsResponse, error) {
	return f.lo(r)
}

// makeAssignment encodes a ConsumerProtocolAssignment v0 blob owning the given
// (topic, partitions) pairs.
func makeAssignment(t *testing.T, parts map[string][]int32) []byte {
	t.Helper()
	a := kwire.Assignment{Version: 0}
	for topic, ids := range parts {
		a.Partitions = append(a.Partitions, kwire.TopicPartitions{Topic: topic, Partitions: ids})
	}
	a.UserDataNull = true
	return kwire.AppendAssignment(nil, a)
}

func TestPollerComputesLag(t *testing.T) {
	t.Parallel()

	asm1 := makeAssignment(t, map[string][]int32{"orders": {0, 1}})
	asm2 := makeAssignment(t, map[string][]int32{"orders": {2}})

	coord := &fakeCoord{
		dg: func(req kwire.DescribeGroupsRequest) (kwire.DescribeGroupsResponse, error) {
			return kwire.DescribeGroupsResponse{
				Groups: []kwire.DescribedGroup{{
					GroupID: "g1",
					Members: []kwire.DescribedGroupMember{
						{MemberID: "m1", MemberAssignment: asm1},
						{MemberID: "m2", MemberAssignment: asm2},
					},
				}},
			}, nil
		},
		of: func(req kwire.OffsetFetchRequest) (kwire.OffsetFetchResponse, error) {
			return kwire.OffsetFetchResponse{
				Groups: []kwire.OffsetFetchGroupResp{{
					GroupID: "g1",
					Topics: []kwire.OffsetFetchTopicResp{{
						Name: "orders",
						Partitions: []kwire.OffsetFetchPartitionResp{
							{PartitionIndex: 0, CommittedOffset: 100},
							{PartitionIndex: 1, CommittedOffset: 200},
							{PartitionIndex: 2, CommittedOffset: 50},
						},
					}},
				}},
			}, nil
		},
		lo: func(req kwire.ListOffsetsRequest) (kwire.ListOffsetsResponse, error) {
			return kwire.ListOffsetsResponse{
				Topics: []kwire.ListOffsetsTopicResp{{
					Name: "orders",
					Partitions: []kwire.ListOffsetsPartitionResp{
						{PartitionIndex: 0, Offset: 150},
						{PartitionIndex: 1, Offset: 250},
						{PartitionIndex: 2, Offset: 1000},
					},
				}},
			}, nil
		},
	}

	p := &Poller{
		Coord:    coord,
		Registry: StaticGroups{"g1"},
		Holder:   &Holder{},
		Interval: time.Hour,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p.tick(ctx)

	snap := p.Holder.Get()
	if snap == nil {
		t.Fatal("no snapshot published")
	}
	gl, ok := snap.Groups["g1"]
	if !ok {
		t.Fatalf("group g1 missing: %+v", snap.Groups)
	}
	m1 := gl.Members["m1"]
	if m1.AssignedPartitions != 2 || m1.LagMessages != 100 { // (150-100) + (250-200)
		t.Errorf("m1 = %+v, want assigned=2 lag=100", m1)
	}
	m2 := gl.Members["m2"]
	if m2.AssignedPartitions != 1 || m2.LagMessages != 950 { // 1000 - 50
		t.Errorf("m2 = %+v, want assigned=1 lag=950", m2)
	}
}

func TestPollerEmptyRegistry(t *testing.T) {
	t.Parallel()
	coord := &fakeCoord{
		dg: func(kwire.DescribeGroupsRequest) (kwire.DescribeGroupsResponse, error) {
			t.Fatal("DescribeGroups should not be called when registry is empty")
			return kwire.DescribeGroupsResponse{}, nil
		},
	}
	p := &Poller{Coord: coord, Registry: StaticGroups{}, Holder: &Holder{}, Interval: time.Hour}
	p.tick(context.Background())
	if snap := p.Holder.Get(); snap == nil || len(snap.Groups) != 0 {
		t.Fatalf("snap = %+v", snap)
	}
}

func TestSyncRegistry(t *testing.T) {
	t.Parallel()
	r := NewSyncRegistry()
	r.Add("g1")
	r.Add("g2")
	r.Add("g1") // dup
	gs := r.Groups()
	if len(gs) != 2 {
		t.Fatalf("groups = %v want 2", gs)
	}
	r.Remove("g1")
	if len(r.Groups()) != 1 {
		t.Fatal("remove did not take effect")
	}
}
