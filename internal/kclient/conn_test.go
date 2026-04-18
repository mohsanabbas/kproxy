package kclient

import (
	"net"
	"testing"

	"github.com/mohsanabbas/kproxy/internal/frame"
	"github.com/mohsanabbas/kproxy/internal/kwire"
)

// TestConnDoAgainstFakeBroker stands up a net.Pipe-backed fake broker that
// echoes back a synthetic Metadata response, and verifies the client can
// round-trip it.
func TestConnDoAgainstFakeBroker(t *testing.T) {
	t.Parallel()
	clientSide, brokerSide := net.Pipe()
	defer clientSide.Close()
	defer brokerSide.Close()

	wantResp := kwire.MetadataResponse{
		Version:        9,
		ThrottleTimeMs: 0,
		Brokers:        []kwire.MetadataBroker{{NodeID: 1, Host: "b1", Port: 9092}},
		ClusterID:      "test",
		ControllerID:   1,
		Topics: []kwire.MetadataTopic{
			{Name: "t", Partitions: []kwire.MetadataPartition{
				{PartitionIndex: 0, LeaderID: 1, LeaderEpoch: -1,
					ReplicaNodes: []int32{1}, IsrNodes: []int32{1}, OfflineReplicas: nil},
			}},
		},
	}

	go func() {
		// Read the request frame.
		brokerR := frame.NewReader(brokerSide, frame.MaxFrameSize)
		buf := frame.Get()
		defer frame.Release(buf)
		body, err := brokerR.ReadFrame(buf)
		if err != nil {
			t.Errorf("broker read: %v", err)
			return
		}
		// Decode the request header to learn correlation id.
		hdr, err := kwire.DecodeRequestHeader(body)
		if err != nil {
			t.Errorf("broker decode hdr: %v", err)
			return
		}
		if hdr.APIKey != kwire.APIMetadata || hdr.APIVersion != 9 {
			t.Errorf("unexpected req: key=%d ver=%d", hdr.APIKey, hdr.APIVersion)
			return
		}
		// Build response: header + body.
		resp := kwire.AppendResponseHeader(nil, hdr.APIKey, hdr.APIVersion, hdr.CorrelID)
		resp = kwire.AppendMetadataResponse(resp, wantResp)
		brokerW := frame.NewWriter(brokerSide)
		if err := brokerW.WriteFrame(resp); err != nil {
			t.Errorf("broker write: %v", err)
			return
		}
	}()

	c := New(clientSide, "test-client")
	got, err := c.Metadata([]string{"t"}, false)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if got.ClusterID != wantResp.ClusterID || len(got.Brokers) != 1 || len(got.Topics) != 1 {
		t.Fatalf("unexpected response: %+v", got)
	}
}
