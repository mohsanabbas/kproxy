package kwire

import (
	"reflect"
	"testing"
)

func TestConsumerProtocolRoundTrip(t *testing.T) {
	t.Parallel()

	subs := []Subscription{
		{
			Version:      0,
			Topics:       []string{"orders", "shipments"},
			UserDataNull: true,
		},
		{
			Version:  1,
			Topics:   []string{"orders"},
			UserData: []byte{1, 2, 3, 4},
			OwnedPartitions: []TopicPartitions{
				{Topic: "orders", Partitions: []int32{0, 3, 7}},
			},
		},
		{
			Version:           2,
			Topics:            []string{"orders"},
			UserData:          []byte("state"),
			OwnedPartitions:   []TopicPartitions{{Topic: "orders", Partitions: []int32{1}}},
			GenerationID:      42,
			GenerationPresent: true,
		},
		{
			Version:           3,
			Topics:            []string{"orders"},
			UserDataNull:      true,
			OwnedPartitions:   nil,
			GenerationID:      0,
			GenerationPresent: true,
			RackID:            "rack-eu-1",
			RackIDPresent:     true,
		},
	}
	for _, want := range subs {
		buf := AppendSubscription(nil, want)
		got, err := DecodeSubscription(buf)
		if err != nil {
			t.Fatalf("decode v%d: %v", want.Version, err)
		}
		if !subscriptionEqual(got, want) {
			t.Fatalf("subscription v%d round-trip mismatch:\n got %#v\nwant %#v", want.Version, got, want)
		}
	}
}

func TestAssignmentRoundTrip(t *testing.T) {
	t.Parallel()

	asgs := []Assignment{
		{
			Version: 0,
			Partitions: []TopicPartitions{
				{Topic: "orders", Partitions: []int32{0, 1, 2}},
			},
			UserDataNull: true,
		},
		{
			Version:    2,
			Partitions: []TopicPartitions{{Topic: "orders", Partitions: []int32{0, 1, 2}}},
			UserData:   []byte("kept"),
		},
		{
			Version:       3,
			Partitions:    []TopicPartitions{{Topic: "orders", Partitions: []int32{5}}},
			UserData:      []byte{0xff},
			RackID:        "rack-1",
			RackIDPresent: true,
		},
	}
	for _, want := range asgs {
		buf := AppendAssignment(nil, want)
		got, err := DecodeAssignment(buf)
		if err != nil {
			t.Fatalf("decode v%d: %v", want.Version, err)
		}
		if got.Version != want.Version ||
			!reflect.DeepEqual(got.Partitions, want.Partitions) ||
			!bytesEq(got.UserData, want.UserData) ||
			got.UserDataNull != want.UserDataNull ||
			got.RackID != want.RackID ||
			got.RackIDNull != want.RackIDNull {
			t.Fatalf("assignment v%d round-trip mismatch:\n got %#v\nwant %#v", want.Version, got, want)
		}
	}
}

func subscriptionEqual(a, b Subscription) bool {
	if a.Version != b.Version ||
		!reflect.DeepEqual(a.Topics, b.Topics) ||
		!bytesEq(a.UserData, b.UserData) ||
		a.UserDataNull != b.UserDataNull ||
		a.GenerationID != b.GenerationID ||
		a.GenerationPresent != b.GenerationPresent ||
		a.RackID != b.RackID ||
		a.RackIDNull != b.RackIDNull ||
		a.RackIDPresent != b.RackIDPresent {
		return false
	}
	if len(a.OwnedPartitions) != len(b.OwnedPartitions) {
		return false
	}
	for i := range a.OwnedPartitions {
		if a.OwnedPartitions[i].Topic != b.OwnedPartitions[i].Topic {
			return false
		}
		if !reflect.DeepEqual(a.OwnedPartitions[i].Partitions, b.OwnedPartitions[i].Partitions) {
			return false
		}
	}
	return true
}

func bytesEq(a, b []byte) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}
