package frame

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	payloads := [][]byte{
		{},
		[]byte("hello"),
		bytes.Repeat([]byte{0xab}, 1024),
		bytes.Repeat([]byte{0x01, 0x02}, 32<<10), // 64 KiB
	}
	var stream bytes.Buffer
	w := NewWriter(&stream)
	for _, p := range payloads {
		if err := w.WriteFrame(p); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	r := NewReader(&stream, MaxFrameSize)
	for i, want := range payloads {
		buf := Get()
		got, err := r.ReadFrame(buf)
		if err != nil {
			t.Fatalf("read[%d]: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("frame[%d] mismatch: got %d bytes want %d", i, len(got), len(want))
		}
		Release(buf)
	}
}

func TestReaderRejectsOversizeFrame(t *testing.T) {
	t.Parallel()
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 1<<30)
	r := NewReader(bytes.NewReader(hdr[:]), 16<<10)
	buf := Get()
	defer Release(buf)
	_, err := r.ReadFrame(buf)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestReaderRejectsNegativeFrame(t *testing.T) {
	t.Parallel()
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 0xFFFFFFFF) // -1 as int32
	r := NewReader(bytes.NewReader(hdr[:]), MaxFrameSize)
	buf := Get()
	defer Release(buf)
	_, err := r.ReadFrame(buf)
	if !errors.Is(err, ErrNegativeFrame) {
		t.Fatalf("err = %v, want ErrNegativeFrame", err)
	}
}

func TestReaderShortHeader(t *testing.T) {
	t.Parallel()
	r := NewReader(bytes.NewReader([]byte{0, 0}), MaxFrameSize)
	buf := Get()
	defer Release(buf)
	_, err := r.ReadFrame(buf)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want ErrUnexpectedEOF", err)
	}
}

func BenchmarkRoundTripZeroAlloc(b *testing.B) {
	body := bytes.Repeat([]byte{0xab}, 256)
	var sink bytes.Buffer
	w := NewWriter(&sink)
	if err := w.WriteFrame(body); err != nil {
		b.Fatal(err)
	}
	frame := append([]byte(nil), sink.Bytes()...) // capture one encoded frame

	r := NewReader(nil, MaxFrameSize)
	buf := Get()
	defer Release(buf)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.r = bytes.NewReader(frame)
		if _, err := r.ReadFrame(buf); err != nil {
			b.Fatal(err)
		}
	}
}
