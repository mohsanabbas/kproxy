package proxy

import (
	"io"
	"net"
	"sync/atomic"
	"testing"

	"github.com/mohsanabbas/kproxy/internal/frame"
	"github.com/mohsanabbas/kproxy/internal/kwire"
)

// BenchmarkPassthrough measures the per-frame allocation cost of the proxy
// hot path with the NoopInterceptor (i.e. the byte-for-byte forwarder). The
// goal is to keep the steady-state allocs/op as low as possible — the frame
// pool and pre-grown rewrite buffers should make the loop allocation-free
// after warm-up.
//
// Note: net.Pipe itself allocates per Read/Write internally; this benchmark
// reflects the proxy's own allocations layered on top, not a hard 0-alloc
// guarantee. Use it to detect regressions, not to set absolute targets.
func BenchmarkPassthrough(b *testing.B) {
	clientApp, clientSide := net.Pipe()
	brokerSide, brokerApp := net.Pipe()
	defer clientApp.Close()
	defer brokerApp.Close()

	conn := New(Config{}, clientSide, brokerSide, NoopInterceptor{})
	ctx := b.Context()
	go func() { _ = conn.Run(ctx) }()

	// Pre-build the request frame once.
	req := kwire.AppendRequestHeader(nil, kwire.RequestHeader{
		APIKey: kwire.APIMetadata, APIVersion: 9, CorrelID: 1, ClientID: "b",
	})
	req = append(req, 0, 0, 0, 0) // empty topics array (4-byte int32 zero)
	resp := kwire.AppendResponseHeader(nil, kwire.APIMetadata, 9, 1)
	resp = append(resp, 0, 0, 0, 0, 0, 0, 0, 0) // throttle + empty arrays

	cw := frame.NewWriter(clientApp)
	cr := frame.NewReader(clientApp, frame.MaxFrameSize)
	bw := frame.NewWriter(brokerApp)
	br := frame.NewReader(brokerApp, frame.MaxFrameSize)

	// Broker echoer: read each request frame, write a fixed response.
	stop := make(chan struct{})
	go func() {
		buf := frame.Get()
		defer frame.Release(buf)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := br.ReadFrame(buf); err != nil {
				return
			}
			if err := bw.WriteFrame(resp); err != nil {
				return
			}
		}
	}()
	defer close(stop)

	clientBuf := frame.Get()
	defer frame.Release(clientBuf)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cw.WriteFrame(req); err != nil {
			b.Fatalf("write: %v", err)
		}
		if _, err := cr.ReadFrame(clientBuf); err != nil {
			if err == io.EOF {
				return
			}
			b.Fatalf("read: %v", err)
		}
	}
	b.StopTimer()
	_ = atomic.LoadInt64(new(int64)) // keep the linter quiet about the import
}
