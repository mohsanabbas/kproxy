// Package kwire implements a minimal, allocation-conscious Kafka wire-protocol
// codec.
//
// Design rules:
//
//   - Decode is cursor-based over a single []byte. There is no io.Reader and
//     therefore no per-call buffer allocation. A Cursor exposes typed Read*
//     methods that advance an offset and return a slice into the underlying
//     buffer (for byte-typed fields) or a primitive value.
//
//   - Encode is append-style: every helper has the signature
//     AppendXxx(dst []byte, v T) []byte. The caller owns the destination
//     buffer and decides when (if ever) to allocate.
//
//   - Strings borrowed from the cursor's underlying slice are NOT copied. The
//     caller is responsible for explicitly copying any string it intends to
//     retain past the lifetime of the input buffer (typically by writing
//     string(b) at the retention boundary).
//
// All multi-byte integers are big-endian, per the Kafka wire format.
package kwire

import (
	"encoding/binary"
	"errors"
)

// ErrTruncated is returned when a decode would read past the end of the
// cursor's underlying buffer. It is a sentinel so callers can distinguish
// truncation from semantic decode errors using errors.Is.
var ErrTruncated = errors.New("kwire: truncated input")

// ErrMalformed is returned when the bytes are present but their structure
// violates the protocol (e.g. negative length on a non-nullable field).
var ErrMalformed = errors.New("kwire: malformed input")

// Cursor is a position over a byte slice. The zero value is invalid; use
// NewCursor.
type Cursor struct {
	buf []byte
	off int
}

// NewCursor returns a Cursor over buf positioned at offset 0. The Cursor
// borrows buf; the caller must not mutate buf while the cursor or any
// returned sub-slices are in use.
func NewCursor(buf []byte) Cursor {
	return Cursor{buf: buf}
}

// Remaining returns how many bytes are left to read.
func (c *Cursor) Remaining() int { return len(c.buf) - c.off }

// Offset returns the current absolute offset within the underlying buffer.
func (c *Cursor) Offset() int { return c.off }

// Bytes returns the underlying buffer (the full slice, not the remainder).
// Useful for sub-slicing relative to absolute offsets.
func (c *Cursor) Bytes() []byte { return c.buf }

// Skip advances the cursor by n bytes.
func (c *Cursor) Skip(n int) error {
	if n < 0 || c.off+n > len(c.buf) {
		return ErrTruncated
	}
	c.off += n
	return nil
}

// ReadInt8 reads a signed 8-bit integer.
func (c *Cursor) ReadInt8() (int8, error) {
	if c.off+1 > len(c.buf) {
		return 0, ErrTruncated
	}
	v := int8(c.buf[c.off]) // #nosec G115 -- intentional reinterpret of single byte as signed int8 per Kafka wire spec
	c.off++
	return v, nil
}

// ReadInt16 reads a big-endian signed 16-bit integer.
func (c *Cursor) ReadInt16() (int16, error) {
	if c.off+2 > len(c.buf) {
		return 0, ErrTruncated
	}
	v := int16(binary.BigEndian.Uint16(c.buf[c.off:])) // #nosec G115 -- Kafka INT16 is signed; reinterpret of same-width unsigned per wire spec
	c.off += 2
	return v, nil
}

// ReadInt32 reads a big-endian signed 32-bit integer.
func (c *Cursor) ReadInt32() (int32, error) {
	if c.off+4 > len(c.buf) {
		return 0, ErrTruncated
	}
	v := int32(binary.BigEndian.Uint32(c.buf[c.off:])) // #nosec G115 -- Kafka INT32 is signed; reinterpret of same-width unsigned per wire spec
	c.off += 4
	return v, nil
}

// ReadInt64 reads a big-endian signed 64-bit integer.
func (c *Cursor) ReadInt64() (int64, error) {
	if c.off+8 > len(c.buf) {
		return 0, ErrTruncated
	}
	v := int64(binary.BigEndian.Uint64(c.buf[c.off:])) // #nosec G115 -- Kafka INT64 is signed; reinterpret of same-width unsigned per wire spec
	c.off += 8
	return v, nil
}

// ReadString reads a Kafka non-flexible STRING: int16 length + bytes. A
// negative length is malformed for a non-nullable string.
func (c *Cursor) ReadString() (string, error) {
	n, err := c.ReadInt16()
	if err != nil {
		return "", err
	}
	if n < 0 {
		return "", ErrMalformed
	}
	if c.off+int(n) > len(c.buf) {
		return "", ErrTruncated
	}
	s := string(c.buf[c.off : c.off+int(n)])
	c.off += int(n)
	return s, nil
}

// ReadNullableString reads a Kafka non-flexible NULLABLE_STRING: int16 length
// (-1 means null) + bytes. Returns (s, false, nil) for the present case and
// ("", true, nil) for the null case.
func (c *Cursor) ReadNullableString() (s string, isNull bool, err error) {
	n, err := c.ReadInt16()
	if err != nil {
		return "", false, err
	}
	if n < 0 {
		return "", true, nil
	}
	if c.off+int(n) > len(c.buf) {
		return "", false, ErrTruncated
	}
	s = string(c.buf[c.off : c.off+int(n)])
	c.off += int(n)
	return s, false, nil
}

// ReadBytes reads a Kafka non-flexible BYTES: int32 length + bytes. The
// returned slice ALIASES the cursor's buffer; copy if retention is required.
// A negative length is malformed for a non-nullable BYTES.
func (c *Cursor) ReadBytes() ([]byte, error) {
	n, err := c.ReadInt32()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, ErrMalformed
	}
	if c.off+int(n) > len(c.buf) {
		return nil, ErrTruncated
	}
	b := c.buf[c.off : c.off+int(n)]
	c.off += int(n)
	return b, nil
}

// ReadNullableBytes reads a Kafka non-flexible NULLABLE_BYTES.
func (c *Cursor) ReadNullableBytes() (b []byte, isNull bool, err error) {
	n, err := c.ReadInt32()
	if err != nil {
		return nil, false, err
	}
	if n < 0 {
		return nil, true, nil
	}
	if c.off+int(n) > len(c.buf) {
		return nil, false, ErrTruncated
	}
	b = c.buf[c.off : c.off+int(n)]
	c.off += int(n)
	return b, false, nil
}

// ReadUvarint reads an unsigned varint (Kafka's compact-collection length
// encoding, equivalent to protobuf's Base 128 Varints, max 5 bytes for
// uint32).
func (c *Cursor) ReadUvarint() (uint32, error) {
	var v uint32
	var shift uint
	for i := 0; i < 5; i++ {
		if c.off >= len(c.buf) {
			return 0, ErrTruncated
		}
		b := c.buf[c.off]
		c.off++
		v |= uint32(b&0x7f) << shift
		if b&0x80 == 0 {
			return v, nil
		}
		shift += 7
	}
	return 0, ErrMalformed
}

// ReadCompactString reads a flexible-version COMPACT_STRING: uvarint(len+1) +
// bytes. uvarint value 0 means null which is malformed for a non-nullable
// COMPACT_STRING.
func (c *Cursor) ReadCompactString() (string, error) {
	n, err := c.ReadUvarint()
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", ErrMalformed
	}
	ln := int(n) - 1
	if c.off+ln > len(c.buf) {
		return "", ErrTruncated
	}
	s := string(c.buf[c.off : c.off+ln])
	c.off += ln
	return s, nil
}

// ReadCompactNullableString reads a COMPACT_NULLABLE_STRING. Returns
// (s, false, nil) when present, ("", true, nil) when null.
func (c *Cursor) ReadCompactNullableString() (s string, isNull bool, err error) {
	n, err := c.ReadUvarint()
	if err != nil {
		return "", false, err
	}
	if n == 0 {
		return "", true, nil
	}
	ln := int(n) - 1
	if c.off+ln > len(c.buf) {
		return "", false, ErrTruncated
	}
	s = string(c.buf[c.off : c.off+ln])
	c.off += ln
	return s, false, nil
}

// ReadCompactBytes reads a COMPACT_BYTES. The returned slice ALIASES the
// cursor's buffer.
func (c *Cursor) ReadCompactBytes() ([]byte, error) {
	n, err := c.ReadUvarint()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, ErrMalformed
	}
	ln := int(n) - 1
	if c.off+ln > len(c.buf) {
		return nil, ErrTruncated
	}
	b := c.buf[c.off : c.off+ln]
	c.off += ln
	return b, nil
}

// ReadCompactNullableBytes reads a COMPACT_NULLABLE_BYTES.
func (c *Cursor) ReadCompactNullableBytes() (b []byte, isNull bool, err error) {
	n, err := c.ReadUvarint()
	if err != nil {
		return nil, false, err
	}
	if n == 0 {
		return nil, true, nil
	}
	ln := int(n) - 1
	if c.off+ln > len(c.buf) {
		return nil, false, ErrTruncated
	}
	b = c.buf[c.off : c.off+ln]
	c.off += ln
	return b, false, nil
}

// ReadArrayLen reads a non-flexible ARRAY length (int32, -1 means null).
func (c *Cursor) ReadArrayLen() (n int, isNull bool, err error) {
	v, err := c.ReadInt32()
	if err != nil {
		return 0, false, err
	}
	if v < 0 {
		return 0, true, nil
	}
	return int(v), false, nil
}

// ReadCompactArrayLen reads a flexible COMPACT_ARRAY length (uvarint(len+1),
// 0 means null).
func (c *Cursor) ReadCompactArrayLen() (n int, isNull bool, err error) {
	v, err := c.ReadUvarint()
	if err != nil {
		return 0, false, err
	}
	if v == 0 {
		return 0, true, nil
	}
	return int(v) - 1, false, nil
}

// SkipTaggedFields walks past a KIP-482 tagged-fields section. We never
// produce tagged fields ourselves, so any we read are discarded.
func (c *Cursor) SkipTaggedFields() error {
	n, err := c.ReadUvarint()
	if err != nil {
		return err
	}
	for i := uint32(0); i < n; i++ {
		if _, err := c.ReadUvarint(); err != nil { // tag id
			return err
		}
		sz, err := c.ReadUvarint()
		if err != nil {
			return err
		}
		if err := c.Skip(int(sz)); err != nil {
			return err
		}
	}
	return nil
}

// ---------- Append helpers (encode side) ----------

// AppendInt8 appends a signed 8-bit integer.
func AppendInt8(dst []byte, v int8) []byte {
	return append(dst, byte(v)) // #nosec G115 -- same-width signed→unsigned reinterpret per Kafka wire spec
}

// AppendInt16 appends a big-endian signed 16-bit integer.
func AppendInt16(dst []byte, v int16) []byte {
	return binary.BigEndian.AppendUint16(dst, uint16(v)) // #nosec G115 -- same-width signed→unsigned reinterpret per Kafka wire spec
}

// AppendInt32 appends a big-endian signed 32-bit integer.
func AppendInt32(dst []byte, v int32) []byte {
	return binary.BigEndian.AppendUint32(dst, uint32(v)) // #nosec G115 -- same-width signed→unsigned reinterpret per Kafka wire spec
}

// AppendInt64 appends a big-endian signed 64-bit integer.
func AppendInt64(dst []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(dst, uint64(v)) // #nosec G115 -- same-width signed→unsigned reinterpret per Kafka wire spec
}

// AppendUvarint appends an unsigned varint.
func AppendUvarint(dst []byte, v uint32) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

// AppendString appends a non-flexible STRING.
//
// NOTE: Kafka non-flexible STRING uses an int16 length prefix so the maximum
// addressable string length is 32 KiB - 1. Callers MUST not pass longer
// strings; the proxy never builds strings near that bound (client-id, topic
// names) so the int conversion is safe in practice. The frame layer also
// rejects any frame larger than MaxFrameSize as a defence in depth.
func AppendString(dst []byte, s string) []byte {
	dst = AppendInt16(dst, int16(len(s))) // #nosec G115 -- bounded by Kafka string max (32KiB) enforced by frame layer
	return append(dst, s...)
}

// AppendNullableString appends a non-flexible NULLABLE_STRING. Pass isNull=true
// to encode the null sentinel.
func AppendNullableString(dst []byte, s string, isNull bool) []byte {
	if isNull {
		return AppendInt16(dst, -1)
	}
	return AppendString(dst, s)
}

// AppendBytes appends a non-flexible BYTES.
//
// Length-prefixed by int32 per the Kafka wire spec; bounded by the proxy's
// frame.MaxFrameSize cap (16 MiB by default).
func AppendBytes(dst, b []byte) []byte {
	dst = AppendInt32(dst, int32(len(b))) // #nosec G115 -- bounded by frame.MaxFrameSize
	return append(dst, b...)
}

// AppendNullableBytes appends a non-flexible NULLABLE_BYTES.
func AppendNullableBytes(dst, b []byte, isNull bool) []byte {
	if isNull {
		return AppendInt32(dst, -1)
	}
	return AppendBytes(dst, b)
}

// AppendCompactString appends a COMPACT_STRING.
//
// Bounded by the surrounding frame size (frame.MaxFrameSize); a single string
// can never exceed the parent frame.
func AppendCompactString(dst []byte, s string) []byte {
	dst = AppendUvarint(dst, uint32(len(s)+1)) // #nosec G115 -- bounded by frame.MaxFrameSize
	return append(dst, s...)
}

// AppendCompactNullableString appends a COMPACT_NULLABLE_STRING.
func AppendCompactNullableString(dst []byte, s string, isNull bool) []byte {
	if isNull {
		return AppendUvarint(dst, 0)
	}
	return AppendCompactString(dst, s)
}

// AppendCompactBytes appends a COMPACT_BYTES.
//
// Bounded by frame.MaxFrameSize.
func AppendCompactBytes(dst, b []byte) []byte {
	dst = AppendUvarint(dst, uint32(len(b)+1)) // #nosec G115 -- bounded by frame.MaxFrameSize
	return append(dst, b...)
}

// AppendCompactNullableBytes appends a COMPACT_NULLABLE_BYTES.
func AppendCompactNullableBytes(dst, b []byte, isNull bool) []byte {
	if isNull {
		return AppendUvarint(dst, 0)
	}
	return AppendCompactBytes(dst, b)
}

// AppendArrayLen appends a non-flexible ARRAY length.
//
// Caller-supplied n is bounded by the producing object (e.g. number of
// brokers in a Metadata response) which is itself bounded by the upstream
// frame size. Negative n other than -1 (null) is a programming error.
func AppendArrayLen(dst []byte, n int, isNull bool) []byte {
	if isNull {
		return AppendInt32(dst, -1)
	}
	return AppendInt32(dst, int32(n)) // #nosec G115 -- bounded by frame.MaxFrameSize
}

// AppendCompactArrayLen appends a COMPACT_ARRAY length.
func AppendCompactArrayLen(dst []byte, n int, isNull bool) []byte {
	if isNull {
		return AppendUvarint(dst, 0)
	}
	return AppendUvarint(dst, uint32(n+1)) // #nosec G115 -- bounded by frame.MaxFrameSize
}

// AppendEmptyTaggedFields writes the encoding for an empty tagged-fields
// section (a single zero byte).
func AppendEmptyTaggedFields(dst []byte) []byte {
	return append(dst, 0x00)
}
