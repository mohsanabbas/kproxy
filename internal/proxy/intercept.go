package proxy

import (
	"context"

	"github.com/mohsanabbas/kproxy/internal/kwire"
)

// Interceptor decides, for each request frame traveling client→broker, whether
// the proxy should register a Pending entry that will let it inspect or rewrite
// the corresponding response.
//
// All implementations MUST be safe for concurrent use across many *Conn.
//
// The default NoopInterceptor returns nil from OnRequest, which causes the
// proxy to operate as a transparent byte-for-byte forwarder. Real interceptors
// (subscription tracker, sync-group rewriter, metadata rewriter) wrap or
// compose around this.
type Interceptor interface {
	// OnRequest is called after the request header has been decoded but before
	// the frame is forwarded upstream. ctx is the per-connection context; it
	// is canceled when the connection shuts down. body is the request payload
	// (after the header). The interceptor may inspect body but MUST NOT
	// retain it past the call (the underlying buffer is reused).
	//
	// Returning a non-nil *Pending registers it returning nil means
	// passthrough. Returning a Pending whose Rewrite is nil is allowed - the
	// proxy will still let the response flow through but will deliver it to
	// the interceptor's OnResponse hook for telemetry.
	OnRequest(ctx context.Context, h kwire.RequestHeader, body []byte) *Pending
}

// NoopInterceptor forwards every frame untouched. Useful as the bottom of an
// interceptor stack and as a default for tests.
type NoopInterceptor struct{}

// OnRequest implements Interceptor.
func (NoopInterceptor) OnRequest(context.Context, kwire.RequestHeader, []byte) *Pending { return nil }
