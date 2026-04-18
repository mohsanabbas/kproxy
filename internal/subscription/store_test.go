package subscription

import (
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mohsanabbas/kproxy/internal/kwire"
)

func TestPutGet(t *testing.T) {
	t.Parallel()
	s := NewStore(0)
	ok := s.Put(Subscription{
		GroupID:  "g1",
		MemberID: "m1",
		Topics:   []string{"a", "b", "a"}, // dup should be removed
	})
	if !ok {
		t.Fatal("Put rejected")
	}
	got := s.Get("g1", "m1")
	if got == nil {
		t.Fatal("Get returned nil")
	}
	if len(got.Topics) != 2 || got.Topics[0] != "a" || got.Topics[1] != "b" {
		t.Fatalf("topics = %v", got.Topics)
	}
}

func TestPutDeepCopiesOwnedPartitions(t *testing.T) {
	t.Parallel()
	s := NewStore(0)
	owned := []kwire.TopicPartitions{{Topic: "a", Partitions: []int32{0, 1}}}
	s.Put(Subscription{GroupID: "g", MemberID: "m", Topics: []string{"a"}, OwnedPartitions: owned})
	owned[0].Partitions[0] = 999 // mutate caller's slice
	got := s.Get("g", "m")
	if got.OwnedPartitions[0].Partitions[0] != 0 {
		t.Fatalf("store retained caller's slice: %v", got.OwnedPartitions)
	}
}

func TestRemoveMemberDropsEmptyGroup(t *testing.T) {
	t.Parallel()
	s := NewStore(0)
	s.Put(Subscription{GroupID: "g", MemberID: "m1", Topics: []string{"t"}})
	s.RemoveMember("g", "m1")
	if got := s.Groups(); len(got) != 0 {
		t.Fatalf("groups after remove: %v", got)
	}
}

func TestCapEnforced(t *testing.T) {
	t.Parallel()
	s := NewStore(2)
	if !s.Put(Subscription{GroupID: "g", MemberID: "m1", Topics: []string{"t"}}) {
		t.Fatal("first Put rejected")
	}
	if !s.Put(Subscription{GroupID: "g", MemberID: "m2", Topics: []string{"t"}}) {
		t.Fatal("second Put rejected")
	}
	if s.Put(Subscription{GroupID: "g", MemberID: "m3", Topics: []string{"t"}}) {
		t.Fatal("third Put should be rejected by cap")
	}
	// Updating an existing member should still succeed (no growth).
	if !s.Put(Subscription{GroupID: "g", MemberID: "m1", Topics: []string{"t2"}}) {
		t.Fatal("update of existing member rejected")
	}
}

func TestOnChangeFires(t *testing.T) {
	t.Parallel()
	s := NewStore(0)
	var fires atomic.Int32
	s.SetOnChange(func(string) { fires.Add(1) })
	s.Put(Subscription{GroupID: "g", MemberID: "m1", Topics: []string{"t"}})
	s.RemoveMember("g", "m1")
	if got := fires.Load(); got != 2 {
		t.Fatalf("onChange fired %d times, want 2", got)
	}
}

func TestConcurrentPutGet(t *testing.T) {
	t.Parallel()
	s := NewStore(0)
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			s.Put(Subscription{GroupID: "g", MemberID: "m" + itoa(i), Topics: []string{"t"}})
		})
	}
	wg.Wait()
	if s.Len() != 64 {
		ms := s.MembersOf("g")
		sort.Strings(ms)
		t.Fatalf("Len = %d want 64", s.Len())
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
