package plan

import (
	"reflect"
	"testing"
)

func TestComputeAllPartitionsAssignedExactlyOnce(t *testing.T) {
	t.Parallel()
	in := Inputs{
		GroupID: "g",
		Members: []Member{
			{MemberID: "m1", Topics: []string{"t"}, Lag: 0},
			{MemberID: "m2", Topics: []string{"t"}, Lag: 0},
		},
		Partitions: map[string][]int32{"t": {0, 1, 2, 3}},
	}
	p := Compute(in)
	seen := make(map[int32]bool)
	for _, byTopic := range p {
		for _, parts := range byTopic {
			for _, x := range parts {
				if seen[x] {
					t.Fatalf("partition %d assigned twice", x)
				}
				seen[x] = true
			}
		}
	}
	if len(seen) != 4 {
		t.Fatalf("assigned %d partitions, want 4 (got %v)", len(seen), p)
	}
}

func TestComputeFeasibility(t *testing.T) {
	t.Parallel()
	// 3 members, 3 partitions - every member must own one.
	in := Inputs{
		Members: []Member{
			{MemberID: "m1", Topics: []string{"t"}},
			{MemberID: "m2", Topics: []string{"t"}, Lag: 9999999}, // crippled
			{MemberID: "m3", Topics: []string{"t"}},
		},
		Partitions: map[string][]int32{"t": {0, 1, 2}},
	}
	p := Compute(in)
	for _, mid := range []string{"m1", "m2", "m3"} {
		if !hasAny(p[mid]) {
			t.Errorf("member %s starved: plan=%v", mid, p)
		}
	}
}

func TestComputeStickiness(t *testing.T) {
	t.Parallel()
	prev := Plan{
		"m1": {"t": []int32{0, 1}},
		"m2": {"t": []int32{2, 3}},
	}
	in := Inputs{
		Members: []Member{
			{MemberID: "m1", Topics: []string{"t"}},
			{MemberID: "m2", Topics: []string{"t"}},
		},
		Partitions: map[string][]int32{"t": {0, 1, 2, 3}},
		Previous:   prev,
	}
	p := Compute(in)
	if !reflect.DeepEqual(p["m1"]["t"], []int32{0, 1}) {
		t.Errorf("m1 lost sticky parts: %v", p["m1"])
	}
	if !reflect.DeepEqual(p["m2"]["t"], []int32{2, 3}) {
		t.Errorf("m2 lost sticky parts: %v", p["m2"])
	}
}

func TestComputeDeterminism(t *testing.T) {
	t.Parallel()
	in := Inputs{
		Members: []Member{
			{MemberID: "m1", Topics: []string{"t"}, Lag: 100},
			{MemberID: "m2", Topics: []string{"t"}, Lag: 100},
			{MemberID: "m3", Topics: []string{"t"}, Lag: 100},
		},
		Partitions: map[string][]int32{"t": {0, 1, 2, 3, 4, 5}},
	}
	a := Compute(in)
	for range 8 {
		b := Compute(in)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("non-deterministic compute:\n a=%v\n b=%v", a, b)
		}
	}
}

func TestComputeEmpty(t *testing.T) {
	t.Parallel()
	if got := Compute(Inputs{}); len(got) != 0 {
		t.Fatalf("empty inputs: %v", got)
	}
}

func TestToAssignments(t *testing.T) {
	t.Parallel()
	p := Plan{
		"m2": {"t": []int32{2}},
		"m1": {"t": []int32{0, 1}},
	}
	out := ToAssignments(p)
	if len(out) != 2 || out[0].MemberID != "m1" || out[1].MemberID != "m2" {
		t.Fatalf("ordering: %v", out)
	}
	if len(out[0].Partitions) != 1 || out[0].Partitions[0].Topic != "t" {
		t.Fatalf("m1 partitions: %v", out[0].Partitions)
	}
}
