package proxy

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// Listener is the accept loop wrapper. It enforces a per-process concurrent
// connection cap via a buffered semaphore channel and dials the broker once
// per accepted client conn.
//
// Construction is via direct struct literal so cmd/kproxy can wire the
// concrete dialer/factory without an extra layer of options.
type Listener struct {
	// Listen is the TCP listener clients connect to.
	Listen net.Listener
	// DialBroker is invoked per accepted client conn to obtain the broker
	// socket. Implementations should honor ctx for cancel during shutdown.
	DialBroker func(ctx context.Context) (net.Conn, error)
	// MakeConn wraps an accepted (client, broker) pair into a *Conn ready for
	// Run. Lets tests inject a custom Config/Interceptor without exposing the
	// proxy package to cmd/kproxy.
	MakeConn func(client, broker net.Conn) *Conn
	// MaxConcurrent caps simultaneous live connections. <=0 disables.
	MaxConcurrent int
	// AcceptTimeout, if >0, is the deadline for one DialBroker attempt.
	AcceptTimeout time.Duration
	// OnAcceptError is called when the listener returns a non-temporary
	// error or DialBroker fails. Optional.
	OnAcceptError func(err error)
	// OnConnStart / OnConnEnd are optional hooks for metrics; called per
	// accepted conn around its Run.
	OnConnStart func()
	OnConnEnd   func()
}

// Serve blocks accepting connections until the listener is closed or ctx is
// cancelled. Each accepted client gets its own goroutine running a *Conn.
//
// Serve does NOT close Listen on return — the caller owns it (so it can be
// closed first to unblock Accept and then Drain pending conns).
func (l *Listener) Serve(ctx context.Context) error {
	var sem chan struct{}
	if l.MaxConcurrent > 0 {
		sem = make(chan struct{}, l.MaxConcurrent)
	}
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		client, err := l.Listen.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if l.OnAcceptError != nil {
				l.OnAcceptError(err)
			}
			return err
		}
		// Apply semaphore before dial so we don't burn broker conns when
		// over capacity.
		if sem != nil {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				_ = client.Close()
				return nil
			}
		}
		dialCtx, cancel := dialContext(ctx, l.AcceptTimeout)
		broker, err := l.DialBroker(dialCtx)
		cancel()
		if err != nil {
			_ = client.Close()
			if sem != nil {
				<-sem
			}
			if l.OnAcceptError != nil {
				l.OnAcceptError(err)
			}
			continue
		}
		wg.Go(func() {
			defer func() {
				if sem != nil {
					<-sem
				}
			}()
			if l.OnConnStart != nil {
				l.OnConnStart()
			}
			defer func() {
				if l.OnConnEnd != nil {
					l.OnConnEnd()
				}
			}()
			conn := l.MakeConn(client, broker)
			if err := conn.Run(ctx); err != nil && l.OnAcceptError != nil {
				l.OnAcceptError(err)
			}
		})
	}
}

func dialContext(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, d)
}
