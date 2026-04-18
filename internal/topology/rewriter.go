package topology

import (
	"github.com/mohsanabbas/kproxy/internal/kwire"
)

// RewriteMetadataResponse rewrites every broker entry in r through m. Brokers
// not present in the mapping are left untouched (operator misconfiguration is
// reported via metrics, not by corrupting the response).
//
// Returns the number of brokers actually rewritten so callers can emit a
// counter for unmapped brokers.
func RewriteMetadataResponse(r *kwire.MetadataResponse, m *Mapping) int {
	if m == nil || r == nil {
		return 0
	}
	n := 0
	for i := range r.Brokers {
		b := &r.Brokers[i]
		if e, ok := m.Lookup(b.NodeID, b.Host, b.Port); ok {
			b.Host = e.Host
			b.Port = e.Port
			n++
		}
	}
	return n
}

// RewriteFindCoordinatorResponse rewrites the coordinator endpoint(s) through
// m, handling both the v0..v3 single-coordinator shape and the v4+
// multi-coordinator shape.
func RewriteFindCoordinatorResponse(r *kwire.FindCoordinatorResponse, m *Mapping) int {
	if m == nil || r == nil {
		return 0
	}
	n := 0
	if r.Version <= 3 {
		// Coordinators with NodeID < 0 / Host == "" are error responses; skip.
		if r.Host != "" && r.Port > 0 {
			if e, ok := m.Lookup(r.NodeID, r.Host, r.Port); ok {
				r.Host = e.Host
				r.Port = e.Port
				n++
			}
		}
		return n
	}
	for i := range r.Coordinators {
		c := &r.Coordinators[i]
		if c.Host == "" || c.Port <= 0 {
			continue
		}
		if e, ok := m.Lookup(c.NodeID, c.Host, c.Port); ok {
			c.Host = e.Host
			c.Port = e.Port
			n++
		}
	}
	return n
}
