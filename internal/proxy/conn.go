// Package proxy is the kproxy hot-path: per-client connection lifecycle, two
// framed pumps (client→broker and broker→client), correlation tracking, and
// interceptor dispatch. It deliberately knows nothing about Kafka semantics
// beyond the request/response header — all rewriting happens through the
// Interceptor interface.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/mohsanabbas/kproxy/internal/frame"
	"github.com/mohsanabbas/kproxy/internal/kwire"
)

// FrameCounter is the minimal sink the proxy uses to publish per-direction
// frame counts and tracker-drop events. obs.Metrics satisfies this; tests can
// pass a no-op or a stub. Kept as an interface (rather than importing obs) so
// the proxy package stays dependency-free.
type FrameCounter interface {
	IncClientToBroker()
	IncBrokerToClient()
	IncTrackerDropped()
}

type noopFrameCounter struct{}

func (noopFrameCounter) IncClientToBroker() {}
func (noopFrameCounter) IncBrokerToClient() {}
func (noopFrameCounter) IncTrackerDropped() {}

// Config tunes a single Conn.
type Config struct {
	// MaxFrameSize bounds individual request/response payloads. <=0 means use
	// frame.MaxFrameSize.
	MaxFrameSize int
	// IdleTimeout, if >0, is applied as a per-iteration read deadline on both
	// sockets. 0 disables.
	IdleTimeout time.Duration
	// MaxInflight bounds the correlation tracker; <=0 defaults to 4096.
	MaxInflight int
	// PendingMaxAge bounds how long a Pending entry stays in the tracker
	// before being evicted (a broker that never replies must not leak
	// memory). <=0 defaults to 5 min.
	PendingMaxAge time.Duration
	// Frames, if non-nil, receives an Inc per forwarded frame in each
	// direction. nil is allowed and means "don't count".
	Frames FrameCounter
}

// Conn proxies a single client conn to a single broker conn.
type Conn struct {
	cfg         Config
	client      net.Conn
	broker      net.Conn
	interceptor Interceptor
	tracker     *Tracker

	clientR *frame.Reader
	clientW *frame.Writer
	brokerR *frame.Reader
	brokerW *frame.Writer

	// ctx is the per-connection context, cancelled when the conn shuts down.
	// Stored so the upstream pump can pass it to the Interceptor.
	ctx context.Context

	// closeOnce ensures Close is idempotent even when both pumps trip it.
	closeOnce sync.Once
	closeErr  error
}

// New wraps an already-established (client, broker) pair.
func New(cfg Config, client, broker net.Conn, ic Interceptor) *Conn {
	if ic == nil {
		ic = NoopInterceptor{}
	}
	max := cfg.MaxFrameSize
	if max <= 0 {
		max = frame.MaxFrameSize
	}
	if cfg.Frames == nil {
		cfg.Frames = noopFrameCounter{}
	}
	return &Conn{
		cfg:         cfg,
		client:      client,
		broker:      broker,
		interceptor: ic,
		tracker:     NewTracker(cfg.MaxInflight, cfg.PendingMaxAge),
		clientR:     frame.NewReader(client, max),
		clientW:     frame.NewWriter(client),
		brokerR:     frame.NewReader(broker, max),
		brokerW:     frame.NewWriter(broker),
	}
}

// Run drives both pumps until either side closes or ctx is cancelled. It
// returns the first non-EOF error observed, or nil on clean shutdown. Run
// blocks the calling goroutine; the typical caller is the per-conn goroutine
// in the listener.
func (c *Conn) Run(ctx context.Context) error {
	c.ctx = ctx
	// Cancellation: when ctx fires, we close both sockets. The pump goroutines
	// then exit on their next read/write with a "use of closed conn" error,
	// which we map to nil if it was caused by us.
	doneCh := make(chan struct{})
	defer close(doneCh)
	go func() {
		select {
		case <-ctx.Done():
			c.closeBoth(nil)
		case <-doneCh:
		}
	}()

	errCh := make(chan error, 2)
	go func() { errCh <- c.upstreamPump() }()
	go func() { errCh <- c.downstreamPump() }()

	// Wait for one pump to exit, force the other to wind down, then collect.
	first := <-errCh
	c.closeBoth(first)
	<-errCh
	return c.closeErr
}

// upstreamPump reads frames from the client, decodes the request header,
// consults the interceptor (which may register a Pending), and forwards the
// frame to the broker. The frame's bytes are forwarded byte-for-byte — even
// for intercepted requests we don't rewrite the wire form yet (the rewrites
// happen on responses).
func (c *Conn) upstreamPump() error {
	buf := frame.Get()
	defer frame.Release(buf)
	// reqRewriteBuf is reused across iterations to assemble rewritten request
	// frames (header + new payload). Pre-grow modestly; the first SyncGroup
	// rewrite will grow it to its working size and subsequent iterations
	// reuse the capacity.
	reqRewriteBuf := make([]byte, 0, 4096)

	for {
		if d := c.cfg.IdleTimeout; d > 0 {
			if err := c.client.SetReadDeadline(time.Now().Add(d)); err != nil {
				return fmt.Errorf("client SetReadDeadline: %w", err)
			}
		}
		body, err := c.clientR.ReadFrame(buf)
		if err != nil {
			return err
		}
		// Decode header for routing. We only call the interceptor — body bytes
		// are forwarded as-is so that any version we don't have a codec for
		// still flows through.
		h, err := kwire.DecodeRequestHeader(body)
		if err != nil {
			// Malformed header: refuse to forward, kill the conn. The client
			// is broken or hostile.
			return fmt.Errorf("decode req header (len=%d, hex=%x): %w", len(body), body, err)
		}
		out := body
		if p := c.interceptor.OnRequest(c.ctx, h, body[h.HeaderSize:]); p != nil {
			p.APIKey = h.APIKey
			p.APIVersion = h.APIVersion
			p.CorrelID = h.CorrelID
			p.Sent = time.Now()
			if p.RewriteRequest != nil {
				// Splice header + new payload. Use a dedicated scratch buf so
				// we don't alias body (which still backs the header bytes).
				reqRewriteBuf = append(reqRewriteBuf[:0], body[:h.HeaderSize]...)
				reqRewriteBuf = append(reqRewriteBuf, p.RewriteRequest...)
				out = reqRewriteBuf
			}
			if !c.tracker.Register(p) {
				c.cfg.Frames.IncTrackerDropped()
			}
		}
		if err := c.brokerW.WriteFrame(out); err != nil {
			return err
		}
		c.cfg.Frames.IncClientToBroker()
	}
}

// downstreamPump reads frames from the broker, peeks the correlation id, and
// either invokes a registered Rewrite or forwards untouched.
//
// Two independent scratch buffers are kept here:
//   - rewriteBuf is given to the Rewrite callback as its append target.
//   - outBuf is used to assemble the outgoing frame (response header +
//     rewritten payload).
//
// They MUST NOT share backing storage, because the rewriter's returned slice
// is read while we write the response header into outBuf.
func (c *Conn) downstreamPump() error {
	buf := frame.Get()
	defer frame.Release(buf)
	rewriteBuf := make([]byte, 0, 4096)
	outBuf := make([]byte, 0, 4096)

	for {
		if d := c.cfg.IdleTimeout; d > 0 {
			if err := c.broker.SetReadDeadline(time.Now().Add(d)); err != nil {
				return fmt.Errorf("broker SetReadDeadline: %w", err)
			}
		}
		body, err := c.brokerR.ReadFrame(buf)
		if err != nil {
			return err
		}
		correlID, err := kwire.PeekResponseCorrelID(body)
		if err != nil {
			return err
		}
		p := c.tracker.Take(correlID)
		if p == nil || p.Rewrite == nil {
			if err := c.clientW.WriteFrame(body); err != nil {
				return err
			}
			c.cfg.Frames.IncBrokerToClient()
			continue
		}
		rh, err := kwire.DecodeResponseHeader(body, p.APIKey, p.APIVersion)
		if err != nil {
			return err
		}
		newPayload, err := p.Rewrite(rewriteBuf[:0], body[rh.HeaderSize:], p)
		if err != nil {
			return err
		}
		out := outBuf[:0]
		out = kwire.AppendResponseHeader(out, p.APIKey, p.APIVersion, p.CorrelID)
		out = append(out, newPayload...)
		if err := c.clientW.WriteFrame(out); err != nil {
			return err
		}
		c.cfg.Frames.IncBrokerToClient()
		// Retain grown capacities for next iteration.
		outBuf = out[:0]
		rewriteBuf = newPayload[:0]
	}
}

// closeBoth closes both sockets exactly once and remembers the originating
// error (filtering out "expected on shutdown" noise).
func (c *Conn) closeBoth(err error) {
	c.closeOnce.Do(func() {
		// Map shutdown noise to nil so callers can distinguish clean exit
		// from a real error.
		if err != nil && !isExpectedShutdownErr(err) {
			c.closeErr = err
		}
		_ = c.client.Close()
		_ = c.broker.Close()
	})
}

func isExpectedShutdownErr(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	return false
}
