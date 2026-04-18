package interceptor

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mohsanabbas/kproxy/internal/frame"
	"github.com/mohsanabbas/kproxy/internal/kwire"
	"github.com/mohsanabbas/kproxy/internal/proxy"
	"github.com/mohsanabbas/kproxy/internal/topology"
)

// TestInterceptor_RewritesMetadataEndToEnd wires a proxy.Conn between two
// net.Pipes with our Interceptor configured against a topology mapping. The
// fake "broker" returns a Metadata response whose broker host/port the proxy
// MUST rewrite before the fake "client" receives it.
func TestInterceptor_RewritesMetadataEndToEnd(t *testing.T) {
	topo := topology.New()
	if err := topo.Add(
		1,
		topology.Endpoint{Host: "broker-1.internal", Port: 9092},
		topology.Endpoint{Host: "proxy.example.com", Port: 19092},
	); err != nil {
		t.Fatalf("add: %v", err)
	}

	ic := New(Deps{Topology: topo})

	clientApp, clientSide := net.Pipe()
	brokerSide, brokerApp := net.Pipe()
	defer clientApp.Close()
	defer brokerApp.Close()

	conn := proxy.New(proxy.Config{}, clientSide, brokerSide, ic)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = conn.Run(ctx) }()

	// Build & send a Metadata v9 request from the fake client.
	reqHdr := kwire.AppendRequestHeader(nil, kwire.RequestHeader{
		APIKey: kwire.APIMetadata, APIVersion: 9, CorrelID: 42, ClientID: "t",
	})
	reqBody := append(reqHdr, 0, 0, 0, 0) // empty topics array
	cw := frame.NewWriter(clientApp)
	if err := cw.WriteFrame(reqBody); err != nil {
		t.Fatalf("client write: %v", err)
	}

	// Broker side: read the request, write a response with a single broker.
	brokerR := frame.NewReader(brokerApp, frame.MaxFrameSize)
	rbuf := frame.Get()
	defer frame.Release(rbuf)
	if _, err := brokerR.ReadFrame(rbuf); err != nil {
		t.Fatalf("broker read: %v", err)
	}
	resp := kwire.MetadataResponse{
		Version:      9,
		Brokers:      []kwire.MetadataBroker{{NodeID: 1, Host: "broker-1.internal", Port: 9092, RackNull: true}},
		ClusterID:    "c1",
		ControllerID: 1,
	}
	respBody := kwire.AppendResponseHeader(nil, kwire.APIMetadata, 9, 42)
	respBody = kwire.AppendMetadataResponse(respBody, resp)
	bw := frame.NewWriter(brokerApp)
	if err := bw.WriteFrame(respBody); err != nil {
		t.Fatalf("broker write: %v", err)
	}

	// Client reads the rewritten response.
	cr := frame.NewReader(clientApp, frame.MaxFrameSize)
	cbuf := frame.Get()
	defer frame.Release(cbuf)
	_ = clientApp.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := cr.ReadFrame(cbuf)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	rh, err := kwire.DecodeResponseHeader(got, kwire.APIMetadata, 9)
	if err != nil {
		t.Fatalf("decode resp hdr: %v", err)
	}
	dec, err := kwire.DecodeMetadataResponse(got[rh.HeaderSize:], 9)
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if len(dec.Brokers) != 1 {
		t.Fatalf("brokers=%d", len(dec.Brokers))
	}
	got0 := dec.Brokers[0]
	if got0.Host != "proxy.example.com" || got0.Port != 19092 {
		t.Fatalf("not rewritten: host=%q port=%d", got0.Host, got0.Port)
	}
}
