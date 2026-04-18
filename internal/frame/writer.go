package frame

import (
	"encoding/binary"
	"io"
	"math"
	"net"
)


// Writer writes length-prefixed Kafka frames to a stream. When the underlying
// writer is a *net.TCPConn we use net.Buffers (writev) to send the 4-byte
// length and the body in a single syscall — zero allocations per frame.
type Writer struct {
	w   io.Writer
	hdr [4]byte
}

// NewWriter builds a Writer over w.
func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// WriteFrame emits one frame consisting of a 4-byte big-endian length and the
// body. The body slice is not retained.
//
// Returns ErrFrameTooLarge if len(body) does not fit in uint32 — defensive
// belt-and-braces against a caller that bypasses the configured MaxFrameSize.
func (wr *Writer) WriteFrame(body []byte) error {
	n := len(body)
	if n < 0 || uint64(n) > math.MaxUint32 {
		return ErrFrameTooLarge
	}
	binary.BigEndian.PutUint32(wr.hdr[:], uint32(n)) // #nosec G115 -- bounds checked above
	// net.Buffers uses writev on supported writers (incl. *net.TCPConn). On
	// other writers it falls back to two Writes, which still avoids the
	// concatenation copy.
	bufs := net.Buffers{wr.hdr[:], body}
	_, err := bufs.WriteTo(wr.w)
	return err
}
