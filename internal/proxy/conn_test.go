package proxy

import (
	"bytes"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mohsanabbas/kproxy/internal/frame"
	"github.com/mohsanabbas/kproxy/internal/kwire"
)

// pair wires a kproxy Conn between fake-client and fake-broker net.Pipes,
// returns reader/writer handles for the two ends, plus a shutdown closure.
type pair struct {
	clientEnd net.Conn // the test's "client" — writes requests, reads responses
	brokerEnd net.Conn // the test's "broker" — reads requests, writes responses
	conn      *Conn
	runErr    chan error
	cancel    context.CancelFunc
}

func newPair(t *testing.T, ic Interceptor) *pair {
	t.Helper()
	clientApp, clientSide := net.Pipe()
	brokerSide, brokerApp := net.Pipe()

	c := New(Config{}, clientSide, brokerSide, ic)
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		_ = clientApp.Close()
		_ = brokerApp.Close()
		<-runErr
	})
	return &pair{
		clientEnd: clientApp,
		brokerEnd: brokerApp,
		conn:      c,
		runErr:    runErr,
		cancel:    cancel,
	}
}

// sendReq writes a Metadata v9 request from the fake client and returns the
// correlation id used.
func sendReq(t *testing.T, w net.Conn, apiKey, version int16, correlID int32, payload []byte) {
	t.Helper()
	hdr := kwire.AppendRequestHeader(nil, kwire.RequestHeader{
		APIKey: apiKey, APIVersion: version, CorrelID: correlID, ClientID: "test",
	})
	hdr = append(hdr, payload...)
	fw := frame.NewWriter(w)
	if err := fw.WriteFrame(hdr); err != nil {
		t.Fatalf("client write: %v", err)
	}
}

// sendResp writes a response with the given correlID and payload.
func sendResp(t *testing.T, w net.Conn, apiKey, version int16, correlID int32, payload []byte) {
	t.Helper()
	hdr := kwire.AppendResponseHeader(nil, apiKey, version, correlID)
	hdr = append(hdr, payload...)
	fw := frame.NewWriter(w)
	if err := fw.WriteFrame(hdr); err != nil {
		t.Fatalf("broker write: %v", err)
	}
}

func TestPassthroughByteForByte(t *testing.T) {
	t.Parallel()
	p := newPair(t, NoopInterceptor{})

	const correlID int32 = 7
	reqPayload := []byte{0x01, 0x02, 0x03}
	respPayload := []byte("hello-world")

	// Client → broker.
	sendReq(t, p.clientEnd, kwire.APIMetadata, 9, correlID, reqPayload)

	// Broker reads what we sent and asserts header+payload integrity.
	br := frame.NewReader(p.brokerEnd, frame.MaxFrameSize)
	buf := frame.Get()
	defer frame.Release(buf)
	gotReq, err := br.ReadFrame(buf)
	if err != nil {
		t.Fatalf("broker read: %v", err)
	}
	h, err := kwire.DecodeRequestHeader(gotReq)
	if err != nil {
		t.Fatalf("decode req hdr: %v", err)
	}
	if h.APIKey != kwire.APIMetadata || h.APIVersion != 9 || h.CorrelID != correlID {
		t.Fatalf("unexpected req header: %+v", h)
	}
	if !bytes.Equal(gotReq[h.HeaderSize:], reqPayload) {
		t.Fatalf("payload mismatch: %x vs %x", gotReq[h.HeaderSize:], reqPayload)
	}

	// Broker → client. Send a response.
	sendResp(t, p.brokerEnd, kwire.APIMetadata, 9, correlID, respPayload)

	// Client reads it.
	cr := frame.NewReader(p.clientEnd, frame.MaxFrameSize)
	cbuf := frame.Get()
	defer frame.Release(cbuf)
	gotResp, err := cr.ReadFrame(cbuf)
	if err != nil {
		t.Fatalf("client read resp: %v", err)
	}
	rh, err := kwire.DecodeResponseHeader(gotResp, kwire.APIMetadata, 9)
	if err != nil {
		t.Fatalf("decode resp hdr: %v", err)
	}
	if rh.CorrelID != correlID {
		t.Fatalf("correl mismatch: %d", rh.CorrelID)
	}
	if !bytes.Equal(gotResp[rh.HeaderSize:], respPayload) {
		t.Fatalf("resp payload mismatch")
	}
}

// recordingInterceptor counts OnRequest invocations and optionally registers
// a Pending whose Rewrite mutates the payload.
type recordingInterceptor struct {
	mu          sync.Mutex
	calls       atomic.Int32
	rewriteFn   RewriteFunc
	wantAPIKeys map[int16]bool
}

func (r *recordingInterceptor) OnRequest(h kwire.RequestHeader, _ []byte) *Pending {
	r.calls.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.wantAPIKeys != nil && !r.wantAPIKeys[h.APIKey] {
		return nil
	}
	return &Pending{Rewrite: r.rewriteFn}
}

func TestInterceptorOnlyFiresForSelectedAPIs(t *testing.T) {
	t.Parallel()
	ic := &recordingInterceptor{wantAPIKeys: map[int16]bool{kwire.APISyncGroup: true}}
	p := newPair(t, ic)

	// net.Pipe is synchronous (unbuffered): every Write blocks until the peer
	// Reads. The proxy's upstream pump can only forward request N once the
	// broker side has consumed request N-1. Drain concurrently.
	drained := make(chan error, 1)
	go func() {
		br := frame.NewReader(p.brokerEnd, frame.MaxFrameSize)
		buf := frame.Get()
		defer frame.Release(buf)
		for range 3 {
			if _, err := br.ReadFrame(buf); err != nil {
				drained <- err
				return
			}
		}
		drained <- nil
	}()

	sendReq(t, p.clientEnd, kwire.APIMetadata, 9, 1, []byte{0xaa})
	sendReq(t, p.clientEnd, kwire.APIMetadata, 9, 2, []byte{0xbb})
	sendReq(t, p.clientEnd, kwire.APISyncGroup, 5, 3, []byte{0xcc})

	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("broker drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker drain timed out")
	}

	if got := ic.calls.Load(); got != 3 {
		t.Fatalf("OnRequest call count = %d, want 3", got)
	}
	if got := p.conn.tracker.Len(); got != 1 {
		t.Fatalf("tracker.Len = %d, want 1 (only SyncGroup registered)", got)
	}
}

func TestRewriteReplacesResponsePayload(t *testing.T) {
	t.Parallel()
	rewritten := []byte("REWRITTEN")
	rewriteFn := func(dst, body []byte, _ *Pending) ([]byte, error) {
		return append(dst, rewritten...), nil
	}
	ic := &recordingInterceptor{rewriteFn: rewriteFn}
	p := newPair(t, ic)

	// SyncGroup v5 (flex). Payload bytes here are arbitrary because the proxy
	// only inspects the header on the upstream side.
	const correlID int32 = 42
	sendReq(t, p.clientEnd, kwire.APISyncGroup, 5, correlID, []byte{0x00})

	// Drain on broker side.
	br := frame.NewReader(p.brokerEnd, frame.MaxFrameSize)
	bbuf := frame.Get()
	defer frame.Release(bbuf)
	if _, err := br.ReadFrame(bbuf); err != nil {
		t.Fatalf("broker read: %v", err)
	}

	// Broker replies with an arbitrary payload that the rewriter MUST replace.
	sendResp(t, p.brokerEnd, kwire.APISyncGroup, 5, correlID, []byte("original"))

	// Client reads.
	cr := frame.NewReader(p.clientEnd, frame.MaxFrameSize)
	cbuf := frame.Get()
	defer frame.Release(cbuf)
	gotResp, err := cr.ReadFrame(cbuf)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	rh, err := kwire.DecodeResponseHeader(gotResp, kwire.APISyncGroup, 5)
	if err != nil {
		t.Fatalf("decode resp hdr: %v", err)
	}
	if rh.CorrelID != correlID {
		t.Fatalf("correl mismatch: %d", rh.CorrelID)
	}
	if !bytes.Equal(gotResp[rh.HeaderSize:], rewritten) {
		t.Fatalf("payload not rewritten: got %q want %q", gotResp[rh.HeaderSize:], rewritten)
	}
	if got := p.conn.tracker.Len(); got != 0 {
		t.Fatalf("tracker not drained: %d", got)
	}
}

func TestTrackerEvictsOnExpiry(t *testing.T) {
	t.Parallel()
	tr := NewTracker(0, 10*time.Millisecond)
	tr.Register(&Pending{CorrelID: 1, Sent: time.Now()})
	if tr.Len() != 1 {
		t.Fatalf("len=%d", tr.Len())
	}
	time.Sleep(20 * time.Millisecond)
	tr.Register(&Pending{CorrelID: 2, Sent: time.Now()})
	if got := tr.Len(); got != 1 {
		t.Fatalf("len after expiry sweep = %d, want 1", got)
	}
	if tr.Take(1) != nil {
		t.Fatalf("expired entry should not be retrievable")
	}
}

func TestTrackerRejectsDuplicateCorrelID(t *testing.T) {
	t.Parallel()
	tr := NewTracker(0, 0)
	if !tr.Register(&Pending{CorrelID: 1, Sent: time.Now()}) {
		t.Fatal("first register failed")
	}
	if tr.Register(&Pending{CorrelID: 1, Sent: time.Now()}) {
		t.Fatal("second register should have failed")
	}
}
