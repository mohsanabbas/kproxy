package proxy

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/mohsanabbas/kproxy/internal/frame"
	"github.com/mohsanabbas/kproxy/internal/kwire"
)

// rewritingInterceptor swaps the request payload with a fixed marker so the
// test can assert the broker received it.
type rewritingInterceptor struct{ replacement []byte }

func (r rewritingInterceptor) OnRequest(h kwire.RequestHeader, body []byte) *Pending {
	return &Pending{RewriteRequest: r.replacement}
}

func TestPending_RewriteRequest_ReplacesUpstreamPayload(t *testing.T) {
	clientApp, clientSide := net.Pipe()
	brokerSide, brokerApp := net.Pipe()
	defer clientApp.Close()
	defer brokerApp.Close()

	replacement := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	conn := New(Config{}, clientSide, brokerSide, rewritingInterceptor{replacement: replacement})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = conn.Run(ctx) }()

	// Client sends a request with a known payload.
	hdr := kwire.AppendRequestHeader(nil, kwire.RequestHeader{
		APIKey: kwire.APISyncGroup, APIVersion: 0, CorrelID: 7, ClientID: "t",
	})
	original := []byte{0x01, 0x02, 0x03}
	hdrSize := len(hdr)
	cw := frame.NewWriter(clientApp)
	if err := cw.WriteFrame(append(hdr, original...)); err != nil {
		t.Fatalf("client write: %v", err)
	}

	// Broker reads — should see header preserved, payload replaced.
	br := frame.NewReader(brokerApp, frame.MaxFrameSize)
	buf := frame.Get()
	defer frame.Release(buf)
	_ = brokerApp.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := br.ReadFrame(buf)
	if err != nil {
		t.Fatalf("broker read: %v", err)
	}
	if !bytes.Equal(got[:hdrSize], hdr) {
		t.Fatalf("header tampered: got %x want %x", got[:hdrSize], hdr)
	}
	if !bytes.Equal(got[hdrSize:], replacement) {
		t.Fatalf("payload not rewritten: got %x want %x", got[hdrSize:], replacement)
	}
}
