// Package topology maps real Kafka broker addresses to the kproxy-advertised
// addresses that clients should be steered towards.
//
// The proxy intercepts MetadataResponse and FindCoordinatorResponse on the
// downstream pump and rewrites every (host, port, nodeID) it sees through this
// table. Without rewriting, clients would learn the real broker addresses on
// their first Metadata fetch and reconnect directly, bypassing kproxy entirely.
//
// The mapping is loaded once at startup from either a `-topology` flag (CSV)
// or a `-topology-file` (JSON) and is then read-only — there is no dynamic
// reload in v1. This keeps the read path lock-free (a plain immutable map).
package topology

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

// Endpoint is a (host, port) pair as understood by Kafka.
type Endpoint struct {
	Host string
	Port int32
}

// String renders the endpoint in host:port form. If host parses as an IPv6
// address it is bracketed. The output is what gets written into rewritten
// Metadata/FindCoordinator responses.
func (e Endpoint) String() string {
	if ip := net.ParseIP(e.Host); ip != nil && ip.To4() == nil {
		return "[" + e.Host + "]:" + strconv.Itoa(int(e.Port))
	}
	return e.Host + ":" + strconv.Itoa(int(e.Port))
}

// Mapping holds the broker → advertised mapping.
//
// Lookups happen on every Metadata/FindCoordinator response on the downstream
// path of every conn. To stay lock-free we publish the whole map immutable;
// reloads (not implemented in v1) would build a new Mapping and atomically
// swap the pointer in the caller.
type Mapping struct {
	byNodeID map[int32]Endpoint
	byHostPt map[string]Endpoint // "host:port" → advertised
}

// New returns a Mapping with no entries. Use Add to populate it before passing
// it to the proxy.
func New() *Mapping {
	return &Mapping{
		byNodeID: make(map[int32]Endpoint),
		byHostPt: make(map[string]Endpoint),
	}
}

// Add registers (nodeID, real) → advertised. Returns an error if the nodeID is
// already present with a different mapping or if either endpoint is invalid.
//
// nodeID may be -1 to register a host:port-only mapping (used for FindCoord
// pre-v4 responses where the node id is per-coordinator, not per-broker).
func (m *Mapping) Add(nodeID int32, real, advertised Endpoint) error {
	if real.Host == "" || real.Port <= 0 {
		return fmt.Errorf("topology: invalid real endpoint %+v", real)
	}
	if advertised.Host == "" || advertised.Port <= 0 {
		return fmt.Errorf("topology: invalid advertised endpoint %+v", advertised)
	}
	if nodeID >= 0 {
		if existing, ok := m.byNodeID[nodeID]; ok && existing != advertised {
			return fmt.Errorf("topology: nodeID %d already mapped to %s, cannot remap to %s", nodeID, existing, advertised)
		}
		m.byNodeID[nodeID] = advertised
	}
	key := real.String()
	if existing, ok := m.byHostPt[key]; ok && existing != advertised {
		return fmt.Errorf("topology: real endpoint %s already mapped to %s, cannot remap to %s", key, existing, advertised)
	}
	m.byHostPt[key] = advertised
	return nil
}

// Lookup resolves (nodeID, host, port) to the advertised endpoint. It tries
// the node id first, then falls back to host:port. The boolean is false when
// no mapping exists; callers should leave the response untouched in that case
// rather than emit garbage.
func (m *Mapping) Lookup(nodeID int32, host string, port int32) (Endpoint, bool) {
	if m == nil {
		return Endpoint{}, false
	}
	if nodeID >= 0 {
		if e, ok := m.byNodeID[nodeID]; ok {
			return e, true
		}
	}
	if e, ok := m.byHostPt[host+":"+strconv.Itoa(int(port))]; ok {
		return e, true
	}
	return Endpoint{}, false
}

// Len returns the number of (nodeID, advertised) entries.
func (m *Mapping) Len() int {
	if m == nil {
		return 0
	}
	return len(m.byNodeID)
}

// ParseFlag parses a CSV-style topology spec of the form
//
//	nodeID=realHost:realPort=advHost:advPort,...
//
// into a Mapping. Whitespace around commas/equals is tolerated. Returns the
// first parse error encountered.
func ParseFlag(spec string) (*Mapping, error) {
	m := New()
	if strings.TrimSpace(spec) == "" {
		return m, nil
	}
	for _, raw := range strings.Split(spec, ",") {
		part := strings.TrimSpace(raw)
		if part == "" {
			continue
		}
		fields := strings.Split(part, "=")
		if len(fields) != 3 {
			return nil, fmt.Errorf("topology: bad entry %q (want nodeID=real=advertised)", part)
		}
		nid, err := strconv.ParseInt(strings.TrimSpace(fields[0]), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("topology: bad node id in %q: %w", part, err)
		}
		real, err := parseEndpoint(strings.TrimSpace(fields[1]))
		if err != nil {
			return nil, fmt.Errorf("topology: bad real endpoint in %q: %w", part, err)
		}
		adv, err := parseEndpoint(strings.TrimSpace(fields[2]))
		if err != nil {
			return nil, fmt.Errorf("topology: bad advertised endpoint in %q: %w", part, err)
		}
		if err := m.Add(int32(nid), real, adv); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// FileEntry mirrors a single entry in the JSON topology file.
type FileEntry struct {
	NodeID     int32  `json:"nodeId"`
	Real       string `json:"real"`       // host:port
	Advertised string `json:"advertised"` // host:port
}

// LoadFile parses a JSON topology file: `[ { "nodeId": ..., "real": "h:p", "advertised": "h:p" }, ... ]`.
//
// path is supplied by the operator via the -topology-file CLI flag; it is
// not derived from network input. Reading it is intentional and not a file
// inclusion vulnerability.
//
// The file is opened explicitly (rather than via os.ReadFile) so the close
// is visible at the call site via defer, and so a Close error can be
// surfaced. Read size is capped at 1 MiB — a topology file describing
// thousands of brokers is well under that.
func LoadFile(path string) (_ *Mapping, err error) {
	const maxTopologyBytes = 1 << 20 // 1 MiB
	f, err := os.Open(path)          // #nosec G304 -- operator-supplied path from CLI flag
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	b, err := io.ReadAll(io.LimitReader(f, maxTopologyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxTopologyBytes {
		return nil, fmt.Errorf("topology: file %s exceeds %d bytes", path, maxTopologyBytes)
	}
	var entries []FileEntry
	if err := json.Unmarshal(b, &entries); err != nil {
		return nil, fmt.Errorf("topology: parse %s: %w", path, err)
	}
	m := New()
	for i, e := range entries {
		real, err := parseEndpoint(e.Real)
		if err != nil {
			return nil, fmt.Errorf("topology: entry %d: %w", i, err)
		}
		adv, err := parseEndpoint(e.Advertised)
		if err != nil {
			return nil, fmt.Errorf("topology: entry %d: %w", i, err)
		}
		if err := m.Add(e.NodeID, real, adv); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func parseEndpoint(s string) (Endpoint, error) {
	if s == "" {
		return Endpoint{}, errors.New("empty endpoint")
	}
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		return Endpoint{}, err
	}
	port, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		return Endpoint{}, fmt.Errorf("bad port: %w", err)
	}
	if port <= 0 || port > 65535 {
		return Endpoint{}, fmt.Errorf("port out of range: %d", port)
	}
	return Endpoint{Host: host, Port: int32(port)}, nil
}
