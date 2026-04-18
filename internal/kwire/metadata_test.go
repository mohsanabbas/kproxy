package kwire

import (
	"reflect"
	"testing"
)

func TestMetadataResponseRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []MetadataResponse{
		{
			Version: 0,
			Brokers: []MetadataBroker{
				{NodeID: 1, Host: "b1", Port: 9092},
			},
			ControllerID: -1,
			Topics: []MetadataTopic{
				{Name: "t", Partitions: []MetadataPartition{
					{PartitionIndex: 0, LeaderID: 1, ReplicaNodes: []int32{1}, IsrNodes: []int32{1}, LeaderEpoch: -1},
				}},
			},
		},
		{
			Version:        3,
			ThrottleTimeMs: 5,
			Brokers: []MetadataBroker{
				{NodeID: 1, Host: "b1", Port: 9092, Rack: "r1"},
				{NodeID: 2, Host: "b2", Port: 9092, RackNull: true},
			},
			ClusterID:    "kproxy-test",
			ControllerID: 1,
			Topics: []MetadataTopic{
				{
					Name: "orders",
					Partitions: []MetadataPartition{
						{PartitionIndex: 0, LeaderID: 1, ReplicaNodes: []int32{1, 2}, IsrNodes: []int32{1, 2}, LeaderEpoch: -1},
					},
				},
			},
		},
		{
			Version:        9, // first flexible
			ThrottleTimeMs: 10,
			Brokers: []MetadataBroker{
				{NodeID: 1, Host: "b1", Port: 9092, Rack: "r1"},
			},
			ClusterID:    "c",
			ControllerID: 1,
			Topics: []MetadataTopic{
				{
					Name: "orders",
					Partitions: []MetadataPartition{
						{PartitionIndex: 0, LeaderID: 1, LeaderEpoch: 5,
							ReplicaNodes: []int32{1}, IsrNodes: []int32{1}, OfflineReplicas: nil},
					},
					AuthorizedOperations: -2147483648,
				},
			},
			ClusterAuthorizedOperations: -2147483648,
		},
		{
			Version:        12,
			ThrottleTimeMs: 0,
			Brokers: []MetadataBroker{
				{NodeID: 1, Host: "b1", Port: 9092, RackNull: true},
			},
			ClusterIDNull: true,
			ControllerID:  1,
			Topics: []MetadataTopic{
				{
					Name:    "orders",
					TopicID: [16]byte{0xde, 0xad, 0xbe, 0xef, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
					Partitions: []MetadataPartition{
						{PartitionIndex: 0, LeaderID: 1, LeaderEpoch: 7,
							ReplicaNodes: []int32{1}, IsrNodes: []int32{1}, OfflineReplicas: nil},
					},
					AuthorizedOperations: -2147483648,
				},
			},
		},
	}
	for _, want := range cases {
		buf := AppendMetadataResponse(nil, want)
		got, err := DecodeMetadataResponse(buf, want.Version)
		if err != nil {
			t.Fatalf("v%d decode: %v", want.Version, err)
		}
		if !metadataEqual(got, want) {
			t.Fatalf("metadata v%d mismatch:\n got %#v\nwant %#v", want.Version, got, want)
		}
	}
}

func TestFindCoordinatorResponseRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []FindCoordinatorResponse{
		{Version: 0, ErrorCode: 0, NodeID: 1, Host: "b1", Port: 9092},
		{Version: 1, ThrottleTimeMs: 5, ErrorMessageNull: true, NodeID: 1, Host: "b1", Port: 9092},
		{Version: 3, ThrottleTimeMs: 5, ErrorCode: 15, ErrorMessage: "no coord", NodeID: -1, Host: "", Port: -1},
		{
			Version: 4, ThrottleTimeMs: 0,
			Coordinators: []FindCoordinatorEntry{
				{Key: "g1", NodeID: 1, Host: "b1", Port: 9092, ErrorMessageNull: true},
				{Key: "g2", NodeID: 2, Host: "b2", Port: 9092, ErrorCode: 16, ErrorMessage: "rebalance"},
			},
		},
	}
	for _, want := range cases {
		buf := AppendFindCoordinatorResponse(nil, want)
		got, err := DecodeFindCoordinatorResponse(buf, want.Version)
		if err != nil {
			t.Fatalf("v%d decode: %v", want.Version, err)
		}
		if !findCoordEqual(got, want) {
			t.Fatalf("findcoord v%d mismatch:\n got %#v\nwant %#v", want.Version, got, want)
		}
	}
}

func metadataEqual(a, b MetadataResponse) bool {
	if a.Version != b.Version || a.ThrottleTimeMs != b.ThrottleTimeMs ||
		a.ClusterID != b.ClusterID || a.ClusterIDNull != b.ClusterIDNull ||
		a.ControllerID != b.ControllerID ||
		a.ClusterAuthorizedOperations != b.ClusterAuthorizedOperations {
		return false
	}
	if len(a.Brokers) != len(b.Brokers) {
		return false
	}
	for i := range a.Brokers {
		if a.Brokers[i] != b.Brokers[i] {
			return false
		}
	}
	if len(a.Topics) != len(b.Topics) {
		return false
	}
	for i := range a.Topics {
		if !topicEqual(a.Topics[i], b.Topics[i]) {
			return false
		}
	}
	return true
}

func topicEqual(a, b MetadataTopic) bool {
	if a.ErrorCode != b.ErrorCode || a.Name != b.Name || a.NameNull != b.NameNull ||
		a.TopicID != b.TopicID || a.IsInternal != b.IsInternal ||
		a.AuthorizedOperations != b.AuthorizedOperations {
		return false
	}
	if len(a.Partitions) != len(b.Partitions) {
		return false
	}
	for i := range a.Partitions {
		ap, bp := a.Partitions[i], b.Partitions[i]
		if ap.ErrorCode != bp.ErrorCode || ap.PartitionIndex != bp.PartitionIndex ||
			ap.LeaderID != bp.LeaderID || ap.LeaderEpoch != bp.LeaderEpoch ||
			!reflect.DeepEqual(ap.ReplicaNodes, bp.ReplicaNodes) ||
			!reflect.DeepEqual(ap.IsrNodes, bp.IsrNodes) ||
			!reflect.DeepEqual(ap.OfflineReplicas, bp.OfflineReplicas) {
			return false
		}
	}
	return true
}

func findCoordEqual(a, b FindCoordinatorResponse) bool {
	if a.Version != b.Version || a.ThrottleTimeMs != b.ThrottleTimeMs ||
		a.ErrorCode != b.ErrorCode || a.ErrorMessage != b.ErrorMessage ||
		a.ErrorMessageNull != b.ErrorMessageNull ||
		a.NodeID != b.NodeID || a.Host != b.Host || a.Port != b.Port {
		return false
	}
	if len(a.Coordinators) != len(b.Coordinators) {
		return false
	}
	for i := range a.Coordinators {
		if a.Coordinators[i] != b.Coordinators[i] {
			return false
		}
	}
	return true
}
