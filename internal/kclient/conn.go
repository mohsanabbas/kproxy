// Package kclient is a minimal synchronous Kafka client used by kproxy for
// telemetry collection (Metadata, ListOffsets, OffsetFetch, DescribeGroups).
//
// It is intentionally NOT used on the proxy hot path — the proxy forwards
// frames opaquely. kclient exists so the telemetry poller can issue RPCs
// against the real cluster without taking a third-party dependency.
//
// Concurrency model: one outstanding request per *Conn. Callers serialise.
// Callers needing parallelism use multiple *Conn or a Pool.
package kclient

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mohsanabbas/kproxy/internal/frame"
	"github.com/mohsanabbas/kproxy/internal/kwire"
)

// ErrCorrelationMismatch is returned when a response's correlation id does not
// match the in-flight request. This shouldn't happen with the one-at-a-time
// model but guarding against it catches broker bugs and connection-reuse bugs.
var ErrCorrelationMismatch = errors.New("kclient: correlation id mismatch")

// Conn is a single TCP connection to a Kafka broker.
type Conn struct {
	conn     net.Conn
	r        *frame.Reader
	w        *frame.Writer
	clientID string

	// nextCorrelID is allocated atomically so callers don't need to lock to
	// produce a fresh id; the per-call lock below still serialises the actual
	// write+read.
	nextCorrelID atomic.Int32

	// mu serialises Do calls — one outstanding request at a time per Conn.
	mu sync.Mutex

	// respBuf is a single reusable buffer holding the most recent response.
	// Callers must consume the previous response before issuing another Do.
	respBuf *frame.Buffer
}

// Dial opens a TCP connection to addr and wraps it in a Conn. clientID is
// used in every request header (Kafka brokers log it for diagnostics).
func Dial(addr, clientID string, dialTimeout time.Duration) (*Conn, error) {
	d := net.Dialer{Timeout: dialTimeout}
	c, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return New(c, clientID), nil
}

// New wraps an existing net.Conn.
func New(c net.Conn, clientID string) *Conn {
	return &Conn{
		conn:     c,
		r:        frame.NewReader(c, frame.MaxFrameSize),
		w:        frame.NewWriter(c),
		clientID: clientID,
	}
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	if c.respBuf != nil {
		frame.Release(c.respBuf)
		c.respBuf = nil
	}
	return c.conn.Close()
}

// SetDeadline applies a deadline to the next Do call's read+write.
func (c *Conn) SetDeadline(t time.Time) error { return c.conn.SetDeadline(t) }

// Do issues one synchronous request and returns the response body (i.e. the
// bytes after the response header has been stripped). The returned slice
// aliases the frame.Buffer that backs it; callers must finish using it before
// the next Do call (which will reuse the buffer) — or copy out what they need.
func (c *Conn) Do(apiKey, apiVersion int16, reqBody []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	correlID := c.nextCorrelID.Add(1)

	// Build request frame: header then body. We allocate one buffer per call
	// because we don't have a write-side pool yet — telemetry is low-rate, so
	// the GC pressure is negligible.
	hdr := kwire.RequestHeader{
		APIKey:     apiKey,
		APIVersion: apiVersion,
		CorrelID:   correlID,
		ClientID:   c.clientID,
	}
	frameBuf := kwire.AppendRequestHeader(nil, hdr)
	frameBuf = append(frameBuf, reqBody...)

	if err := c.w.WriteFrame(frameBuf); err != nil {
		return nil, err
	}

	// Read response into a pooled buffer. We don't release it here because we
	// return a slice into it; the next Do call reuses the same logical buffer
	// after the caller is done.
	if c.respBuf == nil {
		c.respBuf = frame.Get()
	}
	body, err := c.r.ReadFrame(c.respBuf)
	if err != nil {
		return nil, err
	}
	rh, err := kwire.DecodeResponseHeader(body, apiKey, apiVersion)
	if err != nil {
		return nil, err
	}
	if rh.CorrelID != correlID {
		return nil, ErrCorrelationMismatch
	}
	return body[rh.HeaderSize:], nil
}
