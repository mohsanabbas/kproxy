package frame

import (
	"encoding/binary"
	"errors"
	"io"
)

// MaxFrameSize is the default per-frame payload cap. Configurable per-conn by
// the proxy. Anything larger is treated as a protocol error to defend against
// memory-exhaustion via a malicious peer claiming a 2 GiB frame.
const MaxFrameSize = 16 << 20 // 16 MiB

// ErrFrameTooLarge is returned when a peer announces a frame larger than the
// configured cap.
var ErrFrameTooLarge = errors.New("frame: announced size exceeds limit")

// ErrNegativeFrame is returned when a peer announces a negative frame length.
var ErrNegativeFrame = errors.New("frame: negative length")

// Reader reads length-prefixed Kafka frames off a stream.
//
// It does NOT own the underlying io.Reader and does NOT buffer across frames -
// each ReadFrame issues exactly one io.ReadFull for the 4-byte length prefix
// and one for the body. This keeps the per-conn arena small (one Buffer at a
// time) and lets the upstream/downstream pumps share Reader state with no
// hidden buffering surprises.
type Reader struct {
	r       io.Reader
	maxSize int
	hdr     [4]byte
}

// NewReader builds a Reader bounded to the given max payload size.
func NewReader(r io.Reader, maxSize int) *Reader {
	if maxSize <= 0 {
		maxSize = MaxFrameSize
	}
	return &Reader{r: r, maxSize: maxSize}
}

// ReadFrame reads one frame and writes its payload into buf, growing buf as
// needed. The returned slice is buf.Bytes(). On error, buf is left in an
// undefined state and the caller should Release it.
func (rd *Reader) ReadFrame(buf *Buffer) ([]byte, error) {
	if _, err := io.ReadFull(rd.r, rd.hdr[:]); err != nil {
		return nil, err
	}
	size := int32(binary.BigEndian.Uint32(rd.hdr[:])) // #nosec G115 -- Kafka frame length is signed int32 per wire spec; negative checked below
	if size < 0 {
		return nil, ErrNegativeFrame
	}
	if int(size) > rd.maxSize {
		return nil, ErrFrameTooLarge
	}
	buf.grow(int(size))
	if _, err := io.ReadFull(rd.r, buf.b); err != nil {
		return nil, err
	}
	return buf.b, nil
}
