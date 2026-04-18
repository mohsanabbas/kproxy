package topology

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mohsanabbas/kproxy/internal/kwire"
)

func TestParseFlag(t *testing.T) {
	t.Parallel()
	m, err := ParseFlag("0=10.0.0.1:9092=proxy.local:9192, 1=10.0.0.2:9092=proxy.local:9193")
	if err != nil {
		t.Fatalf("ParseFlag: %v", err)
	}
	if got := m.Len(); got != 2 {
		t.Fatalf("Len = %d want 2", got)
	}
	e, ok := m.Lookup(0, "10.0.0.1", 9092)
	if !ok || e.Host != "proxy.local" || e.Port != 9192 {
		t.Fatalf("lookup nodeID 0: got %+v ok=%v", e, ok)
	}
	// Host:port fallback when nodeID unknown.
	if e, ok = m.Lookup(-1, "10.0.0.2", 9092); !ok || e.Port != 9193 {
		t.Fatalf("host:port fallback: %+v ok=%v", e, ok)
	}
	// Unknown is reported.
	if _, ok = m.Lookup(99, "nope", 1); ok {
		t.Fatal("expected miss for unknown")
	}
}

func TestParseFlagErrors(t *testing.T) {
	t.Parallel()
	cases := []string{
		"0=onlyone",
		"abc=10.0.0.1:9092=p:1",
		"0=10.0.0.1=p:1",
		"0=10.0.0.1:9092=p:0",
	}
	for _, c := range cases {
		if _, err := ParseFlag(c); err == nil {
			t.Errorf("ParseFlag(%q) expected error", c)
		}
	}
}

func TestLoadFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "topo.json")
	if err := writeFile(path, `[
		{"nodeId": 0, "real": "10.0.0.1:9092", "advertised": "proxy:9192"},
		{"nodeId": 1, "real": "10.0.0.2:9092", "advertised": "proxy:9193"}
	]`); err != nil {
		t.Fatal(err)
	}
	m, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if got := m.Len(); got != 2 {
		t.Fatalf("Len = %d want 2", got)
	}
}

func TestRewriteMetadata(t *testing.T) {
	t.Parallel()
	m, _ := ParseFlag("1=b1:9092=p:9192,2=b2:9092=p:9193")
	r := &kwire.MetadataResponse{
		Brokers: []kwire.MetadataBroker{
			{NodeID: 1, Host: "b1", Port: 9092},
			{NodeID: 2, Host: "b2", Port: 9092},
			{NodeID: 9, Host: "unknown", Port: 1234},
		},
	}
	if n := RewriteMetadataResponse(r, m); n != 2 {
		t.Fatalf("rewritten = %d want 2", n)
	}
	want := []kwire.MetadataBroker{
		{NodeID: 1, Host: "p", Port: 9192},
		{NodeID: 2, Host: "p", Port: 9193},
		{NodeID: 9, Host: "unknown", Port: 1234},
	}
	if !reflect.DeepEqual(r.Brokers, want) {
		t.Fatalf("brokers:\n got %+v\nwant %+v", r.Brokers, want)
	}
}

func TestRewriteFindCoordinatorV0(t *testing.T) {
	t.Parallel()
	m, _ := ParseFlag("3=b3:9092=p:9194")
	r := &kwire.FindCoordinatorResponse{
		Version: 3,
		NodeID:  3,
		Host:    "b3",
		Port:    9092,
	}
	if n := RewriteFindCoordinatorResponse(r, m); n != 1 {
		t.Fatalf("rewritten = %d want 1", n)
	}
	if r.Host != "p" || r.Port != 9194 {
		t.Fatalf("got %s:%d", r.Host, r.Port)
	}
}

func TestRewriteFindCoordinatorV4(t *testing.T) {
	t.Parallel()
	m, _ := ParseFlag("3=b3:9092=p:9194")
	r := &kwire.FindCoordinatorResponse{
		Version: 4,
		Coordinators: []kwire.FindCoordinatorEntry{
			{Key: "g1", NodeID: 3, Host: "b3", Port: 9092},
			{Key: "g2", NodeID: -1, Host: "", Port: 0, ErrorCode: 15}, // CoordinatorNotAvailable
		},
	}
	if n := RewriteFindCoordinatorResponse(r, m); n != 1 {
		t.Fatalf("rewritten = %d want 1", n)
	}
	if r.Coordinators[0].Host != "p" || r.Coordinators[0].Port != 9194 {
		t.Fatalf("entry 0: %+v", r.Coordinators[0])
	}
	// Error entry untouched.
	if r.Coordinators[1].Host != "" || r.Coordinators[1].ErrorCode != 15 {
		t.Fatalf("entry 1 mutated: %+v", r.Coordinators[1])
	}
}

func TestAddConflictDetection(t *testing.T) {
	t.Parallel()
	m := New()
	if err := m.Add(0, Endpoint{"a", 1}, Endpoint{"x", 1}); err != nil {
		t.Fatal(err)
	}
	if err := m.Add(0, Endpoint{"a", 1}, Endpoint{"y", 2}); err == nil {
		t.Fatal("expected conflict on remap")
	}
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}
