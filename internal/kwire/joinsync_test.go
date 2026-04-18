package kwire

import (
	"reflect"
	"testing"
)

func TestSyncGroupRequestRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []SyncGroupRequest{
		{
			Version:    0,
			Group:      "g",
			Generation: 1,
			MemberID:   "m",
			Assignments: []SyncGroupAssignment{
				{MemberID: "m", MemberAssignment: []byte{0, 1, 2}},
			},
		},
		{
			Version:      3, // pre-flex with InstanceID
			Group:        "g",
			Generation:   2,
			MemberID:     "m",
			InstancePres: true,
			InstanceID:   "static-1",
			Assignments: []SyncGroupAssignment{
				{MemberID: "m", MemberAssignment: []byte{1}},
			},
		},
		{
			Version:       5, // flex + protocol fields
			Group:         "g",
			Generation:    9,
			MemberID:      "leader",
			InstancePres:  true,
			InstanceID:    "",
			InstanceNull:  true,
			ProtoTypePres: true,
			ProtocolType:  "consumer",
			ProtoNamePres: true,
			ProtocolName:  "cooperative-sticky",
			Assignments: []SyncGroupAssignment{
				{MemberID: "leader", MemberAssignment: []byte{9, 8, 7}},
				{MemberID: "follower", MemberAssignment: []byte{}, MAIsNull: true},
			},
		},
	}
	for _, want := range cases {
		buf := AppendSyncGroupRequest(nil, want)
		got, err := DecodeSyncGroupRequest(buf, want.Version)
		if err != nil {
			t.Fatalf("v%d decode: %v", want.Version, err)
		}
		if !syncReqEqual(got, want) {
			t.Fatalf("syncgroup req v%d round-trip mismatch:\n got %#v\nwant %#v", want.Version, got, want)
		}
	}
}

func TestSyncGroupResponseRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []SyncGroupResponse{
		{Version: 0, ErrorCode: 0, MemberAssignment: []byte{1, 2}},
		{Version: 1, ThrottleTimeMs: 100, ErrorCode: 0, MemberAssignment: []byte{3}},
		{Version: 4, ThrottleTimeMs: 0, ErrorCode: 16, MAIsNull: true},
		{
			Version: 5, ThrottleTimeMs: 50, ErrorCode: 0,
			ProtocolType: "consumer", ProtocolName: "range",
			MemberAssignment: []byte{0xab, 0xcd},
		},
	}
	for _, want := range cases {
		buf := AppendSyncGroupResponse(nil, want)
		got, err := DecodeSyncGroupResponse(buf, want.Version)
		if err != nil {
			t.Fatalf("v%d decode: %v", want.Version, err)
		}
		if got.Version != want.Version ||
			got.ThrottleTimeMs != want.ThrottleTimeMs ||
			got.ErrorCode != want.ErrorCode ||
			got.ProtocolType != want.ProtocolType ||
			got.ProtocolName != want.ProtocolName ||
			!bytesEq(got.MemberAssignment, want.MemberAssignment) ||
			got.MAIsNull != want.MAIsNull {
			t.Fatalf("v%d mismatch:\n got %#v\nwant %#v", want.Version, got, want)
		}
	}
}

func TestJoinGroupRequestRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []JoinGroupRequest{
		{
			Version: 0, Group: "g", SessionTimeoutMs: 30000, RebalanceTimeoutMs: -1,
			MemberID: "", ProtocolType: "consumer",
			Protocols: []JoinGroupProtocol{
				{Name: "range", Metadata: []byte{0, 1, 2}},
			},
		},
		{
			Version: 5, Group: "g", SessionTimeoutMs: 30000, RebalanceTimeoutMs: 60000,
			MemberID: "m", GIIDPresent: true, GIIDNull: true,
			ProtocolType: "consumer",
			Protocols: []JoinGroupProtocol{
				{Name: "sticky", Metadata: []byte{4, 5}},
				{Name: "range", Metadata: []byte{6}},
			},
		},
		{
			Version: 9, Group: "g", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 60000,
			MemberID: "leader", GIIDPresent: true, GroupInstanceID: "static-7",
			ProtocolType: "consumer",
			Protocols: []JoinGroupProtocol{
				{Name: "cooperative-sticky", Metadata: []byte{0xff, 0xee}},
			},
			ReasonPresent: true, Reason: "rebalance-triggered",
		},
	}
	for _, want := range cases {
		buf := AppendJoinGroupRequest(nil, want)
		got, err := DecodeJoinGroupRequest(buf, want.Version)
		if err != nil {
			t.Fatalf("v%d decode: %v", want.Version, err)
		}
		if !joinReqEqual(got, want) {
			t.Fatalf("joingroup v%d mismatch:\n got %#v\nwant %#v", want.Version, got, want)
		}
	}
}

func syncReqEqual(a, b SyncGroupRequest) bool {
	if a.Version != b.Version || a.Group != b.Group || a.Generation != b.Generation ||
		a.MemberID != b.MemberID ||
		a.InstancePres != b.InstancePres || a.InstanceID != b.InstanceID || a.InstanceNull != b.InstanceNull ||
		a.ProtoTypePres != b.ProtoTypePres || a.ProtocolType != b.ProtocolType || a.ProtoTypeNull != b.ProtoTypeNull ||
		a.ProtoNamePres != b.ProtoNamePres || a.ProtocolName != b.ProtocolName || a.ProtoNameNull != b.ProtoNameNull {
		return false
	}
	if len(a.Assignments) != len(b.Assignments) {
		return false
	}
	for i := range a.Assignments {
		if a.Assignments[i].MemberID != b.Assignments[i].MemberID ||
			a.Assignments[i].MAIsNull != b.Assignments[i].MAIsNull ||
			!bytesEq(a.Assignments[i].MemberAssignment, b.Assignments[i].MemberAssignment) {
			return false
		}
	}
	return true
}

func joinReqEqual(a, b JoinGroupRequest) bool {
	if a.Version != b.Version || a.Group != b.Group ||
		a.SessionTimeoutMs != b.SessionTimeoutMs || a.RebalanceTimeoutMs != b.RebalanceTimeoutMs ||
		a.MemberID != b.MemberID ||
		a.GIIDPresent != b.GIIDPresent || a.GroupInstanceID != b.GroupInstanceID || a.GIIDNull != b.GIIDNull ||
		a.ProtocolType != b.ProtocolType ||
		a.ReasonPresent != b.ReasonPresent || a.Reason != b.Reason || a.ReasonNull != b.ReasonNull {
		return false
	}
	if len(a.Protocols) != len(b.Protocols) {
		return false
	}
	for i := range a.Protocols {
		if a.Protocols[i].Name != b.Protocols[i].Name ||
			!reflect.DeepEqual(a.Protocols[i].Metadata, b.Protocols[i].Metadata) {
			return false
		}
	}
	return true
}
