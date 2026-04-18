package kwire

import "testing"

func TestRequestHeaderRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		h    RequestHeader
	}{
		{
			name: "v1 SyncGroup with clientID",
			h: RequestHeader{
				APIKey:     APISyncGroup,
				APIVersion: 3, // pre-flex SyncGroup
				CorrelID:   42,
				ClientID:   "my-app",
			},
		},
		{
			name: "v1 SyncGroup with null clientID",
			h: RequestHeader{
				APIKey:     APISyncGroup,
				APIVersion: 3,
				CorrelID:   1,
				ClientNull: true,
			},
		},
		{
			name: "flexible SyncGroup v5",
			h: RequestHeader{
				APIKey:     APISyncGroup,
				APIVersion: 5,
				CorrelID:   99,
				ClientID:   "flex-client",
			},
		},
		{
			name: "flexible JoinGroup v6",
			h: RequestHeader{
				APIKey:     APIJoinGroup,
				APIVersion: 6,
				CorrelID:   12345,
				ClientID:   "consumer-7",
			},
		},
		{
			name: "SaslHandshake never flexible",
			h: RequestHeader{
				APIKey:     APISaslHandshake,
				APIVersion: 1,
				CorrelID:   7,
				ClientID:   "p",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			buf := AppendRequestHeader(nil, tc.h)
			got, err := DecodeRequestHeader(buf)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.APIKey != tc.h.APIKey ||
				got.APIVersion != tc.h.APIVersion ||
				got.CorrelID != tc.h.CorrelID ||
				got.ClientID != tc.h.ClientID ||
				got.ClientNull != tc.h.ClientNull {
				t.Fatalf("round-trip mismatch:\n got %#v\nwant %#v", got, tc.h)
			}
			if got.HeaderSize != len(buf) {
				t.Fatalf("HeaderSize=%d want %d", got.HeaderSize, len(buf))
			}
		})
	}
}

func TestResponseHeaderRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		apiKey     int16
		apiVersion int16
		correlID   int32
	}{
		{"non-flex Metadata v8", APIMetadata, 8, 1},
		{"flex Metadata v9", APIMetadata, 9, 99},
		{"non-flex SyncGroup v3", APISyncGroup, 3, 42},
		{"flex SyncGroup v5", APISyncGroup, 5, 100},
		{"non-flex ApiVersions v3 (response always non-flex)", APIApiVersions, 3, 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			buf := AppendResponseHeader(nil, tc.apiKey, tc.apiVersion, tc.correlID)
			h, err := DecodeResponseHeader(buf, tc.apiKey, tc.apiVersion)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if h.CorrelID != tc.correlID {
				t.Fatalf("correlID=%d want %d", h.CorrelID, tc.correlID)
			}
			if h.HeaderSize != len(buf) {
				t.Fatalf("HeaderSize=%d want %d", h.HeaderSize, len(buf))
			}
		})
	}
}

func TestPeekHelpers(t *testing.T) {
	t.Parallel()
	hdr := AppendRequestHeader(nil, RequestHeader{
		APIKey:     APISyncGroup,
		APIVersion: 5,
		CorrelID:   123,
		ClientID:   "x",
	})
	apiKey, apiVersion, err := PeekRequestKeyVersion(hdr)
	if err != nil || apiKey != APISyncGroup || apiVersion != 5 {
		t.Fatalf("peek: key=%d ver=%d err=%v", apiKey, apiVersion, err)
	}

	resp := AppendResponseHeader(nil, APISyncGroup, 5, 456)
	cid, err := PeekResponseCorrelID(resp)
	if err != nil || cid != 456 {
		t.Fatalf("peek correl: got %d err=%v", cid, err)
	}
}
