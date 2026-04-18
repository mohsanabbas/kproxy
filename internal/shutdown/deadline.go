// Package shutdown coordinates orderly termination of the proxy listener and
// its in-flight connections.
//
// The model: cmd/kproxy creates a Group, registers each accepted *proxy.Conn
// with it, and on SIGTERM calls Drain(ctx). Drain stops accepting new conns,
// signals every registered conn to finish its current frames, and waits up to
// the context deadline for them all to exit. Past the deadline it force-closes
// remaining sockets to unblock pumps stuck in syscall.
package shutdown

import (
	"context"
	"net"
	"sync"
)

// Group bundles a set of cancellable units (conns) so they can be drained
// together. The zero value is usable.
type Group struct {
	mu      sync.Mutex
	closed  bool
	members map[*member]struct{}
}

type member struct {
	cancel context.CancelFunc
	conn   net.Conn // optional: force-closed on hard deadline
}

// Add registers cancel and the underlying client conn (may be nil if no force
// close is desired) with the group. Returns a release func the caller MUST
// invoke when the connection finishes naturally so the group can forget it.
func (g *Group) Add(cancel context.CancelFunc, conn net.Conn) (release func()) {
	m := &member{cancel: cancel, conn: conn}
	g.mu.Lock()
	if g.members == nil {
		g.members = make(map[*member]struct{})
	}
	if g.closed {
		g.mu.Unlock()
		// Group already drained — cancel immediately.
		cancel()
		return func() {}
	}
	g.members[m] = struct{}{}
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		delete(g.members, m)
		g.mu.Unlock()
	}
}

// Drain cancels every registered member and waits up to ctx for the
// membership to drop to zero. Past the deadline it Close()s any conn that
// was registered with one (this unblocks pumps stuck in Read).
//
// Drain is idempotent: subsequent Add calls return a no-op release and cancel
// immediately.
func (g *Group) Drain(ctx context.Context) {
	g.mu.Lock()
	g.closed = true
	for m := range g.members {
		m.cancel()
	}
	g.mu.Unlock()

	// Polling — cheap because the membership is small (bounded by conn cap).
	for {
		g.mu.Lock()
		n := len(g.members)
		g.mu.Unlock()
		if n == 0 {
			return
		}
		select {
		case <-ctx.Done():
			g.mu.Lock()
			for m := range g.members {
				if m.conn != nil {
					_ = m.conn.Close()
				}
			}
			g.mu.Unlock()
			return
		default:
		}
		// Yield without burning CPU.
		select {
		case <-ctx.Done():
		case <-pollAfter():
		}
	}
}

// Len returns the current number of registered members.
func (g *Group) Len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.members)
}
