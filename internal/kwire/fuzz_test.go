package kwire

import "testing"

// These fuzz targets ensure decoders never panic on adversarial input. We
// don't assert on round-trip equivalence here — that's covered by the unit
// tests with hand-crafted golden frames. The fuzzer's job is purely
// crash-resistance.

func FuzzDecodeMetadataResponse(f *testing.F) {
	// Seeds: a few short well-formed bodies for various versions.
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0}, int(0))
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0}, int(1))
	f.Fuzz(func(t *testing.T, body []byte, v int) {
		if v < 0 || v > 12 {
			t.Skip()
		}
		_, _ = DecodeMetadataResponse(body, int16(v))
	})
}

func FuzzDecodeFindCoordinatorResponse(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0}, int(0))
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 1}, int(4))
	f.Fuzz(func(t *testing.T, body []byte, v int) {
		if v < 0 || v > 6 {
			t.Skip()
		}
		_, _ = DecodeFindCoordinatorResponse(body, int16(v))
	})
}

func FuzzDecodeJoinGroupRequest(f *testing.F) {
	f.Add([]byte{0, 1, 'g', 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0}, int(0))
	f.Fuzz(func(t *testing.T, body []byte, v int) {
		if v < 0 || v > 9 {
			t.Skip()
		}
		_, _ = DecodeJoinGroupRequest(body, int16(v))
	})
}

func FuzzDecodeSyncGroupRequest(f *testing.F) {
	f.Add([]byte{0, 1, 'g', 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0}, int(0))
	f.Fuzz(func(t *testing.T, body []byte, v int) {
		if v < 0 || v > 5 {
			t.Skip()
		}
		_, _ = DecodeSyncGroupRequest(body, int16(v))
	})
}

func FuzzDecodeSubscription(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, body []byte) {
		_, _ = DecodeSubscription(body)
	})
}

func FuzzDecodeAssignment(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, body []byte) {
		_, _ = DecodeAssignment(body)
	})
}
