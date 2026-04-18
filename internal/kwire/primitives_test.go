package kwire

import (
	"errors"
	"fmt"
	"testing"
)

func TestPrimitivesRoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("ints", func(t *testing.T) {
		t.Parallel()
		var buf []byte
		buf = AppendInt8(buf, -7)
		buf = AppendInt16(buf, -12345)
		buf = AppendInt32(buf, -1234567890)
		buf = AppendInt64(buf, -1234567890123456789)

		c := NewCursor(buf)
		if v, err := c.ReadInt8(); err != nil || v != -7 {
			t.Fatalf("int8: got %d err=%v", v, err)
		}
		if v, err := c.ReadInt16(); err != nil || v != -12345 {
			t.Fatalf("int16: got %d err=%v", v, err)
		}
		if v, err := c.ReadInt32(); err != nil || v != -1234567890 {
			t.Fatalf("int32: got %d err=%v", v, err)
		}
		if v, err := c.ReadInt64(); err != nil || v != -1234567890123456789 {
			t.Fatalf("int64: got %d err=%v", v, err)
		}
		if c.Remaining() != 0 {
			t.Fatalf("expected drained, %d remaining", c.Remaining())
		}
	})

	t.Run("uvarint boundaries", func(t *testing.T) {
		t.Parallel()
		for _, v := range []uint32{0, 1, 127, 128, 16383, 16384, 1<<28 - 1, 1 << 28, ^uint32(0)} {
			buf := AppendUvarint(nil, v)
			c := NewCursor(buf)
			got, err := c.ReadUvarint()
			if err != nil {
				t.Fatalf("uvarint %d: err=%v", v, err)
			}
			if got != v {
				t.Fatalf("uvarint %d: got %d", v, got)
			}
			if c.Remaining() != 0 {
				t.Fatalf("uvarint %d: %d remaining", v, c.Remaining())
			}
		}
	})

	t.Run("string and nullable string", func(t *testing.T) {
		t.Parallel()
		buf := AppendString(nil, "hello")
		buf = AppendNullableString(buf, "", true)
		buf = AppendNullableString(buf, "world", false)

		c := NewCursor(buf)
		if s, err := c.ReadString(); err != nil || s != "hello" {
			t.Fatalf("string: got %q err=%v", s, err)
		}
		if s, isNull, err := c.ReadNullableString(); err != nil || !isNull || s != "" {
			t.Fatalf("null string: got %q isNull=%v err=%v", s, isNull, err)
		}
		if s, isNull, err := c.ReadNullableString(); err != nil || isNull || s != "world" {
			t.Fatalf("nullable string: got %q isNull=%v err=%v", s, isNull, err)
		}
	})

	t.Run("compact string and bytes", func(t *testing.T) {
		t.Parallel()
		buf := AppendCompactString(nil, "abc")
		buf = AppendCompactNullableString(buf, "", true)
		buf = AppendCompactBytes(buf, []byte{1, 2, 3})

		c := NewCursor(buf)
		if s, err := c.ReadCompactString(); err != nil || s != "abc" {
			t.Fatalf("compact string: got %q err=%v", s, err)
		}
		if s, isNull, err := c.ReadCompactNullableString(); err != nil || !isNull || s != "" {
			t.Fatalf("null compact string: got %q isNull=%v err=%v", s, isNull, err)
		}
		if b, err := c.ReadCompactBytes(); err != nil || string(b) != "\x01\x02\x03" {
			t.Fatalf("compact bytes: got %v err=%v", b, err)
		}
	})

	t.Run("array lengths", func(t *testing.T) {
		t.Parallel()
		buf := AppendArrayLen(nil, 3, false)
		buf = AppendArrayLen(buf, 0, true)
		buf = AppendCompactArrayLen(buf, 5, false)
		buf = AppendCompactArrayLen(buf, 0, true)

		c := NewCursor(buf)
		if n, isNull, err := c.ReadArrayLen(); err != nil || isNull || n != 3 {
			t.Fatalf("array len 3: got %d isNull=%v err=%v", n, isNull, err)
		}
		if n, isNull, err := c.ReadArrayLen(); err != nil || !isNull || n != 0 {
			t.Fatalf("array len null: got %d isNull=%v err=%v", n, isNull, err)
		}
		if n, isNull, err := c.ReadCompactArrayLen(); err != nil || isNull || n != 5 {
			t.Fatalf("compact array 5: got %d isNull=%v err=%v", n, isNull, err)
		}
		if n, isNull, err := c.ReadCompactArrayLen(); err != nil || !isNull || n != 0 {
			t.Fatalf("compact array null: got %d isNull=%v err=%v", n, isNull, err)
		}
	})

	t.Run("tagged fields", func(t *testing.T) {
		t.Parallel()
		// Two tagged fields: tag 5 with body "ab", tag 9 with body "xyz".
		var buf []byte
		buf = AppendUvarint(buf, 2) // count
		buf = AppendUvarint(buf, 5)
		buf = AppendUvarint(buf, 2)
		buf = append(buf, 'a', 'b')
		buf = AppendUvarint(buf, 9)
		buf = AppendUvarint(buf, 3)
		buf = append(buf, 'x', 'y', 'z')

		c := NewCursor(buf)
		if err := c.SkipTaggedFields(); err != nil {
			t.Fatalf("skip tagged: %v", err)
		}
		if c.Remaining() != 0 {
			t.Fatalf("tagged fields not fully consumed; %d remaining", c.Remaining())
		}

		// Empty section should consume one zero byte.
		c2 := NewCursor(AppendEmptyTaggedFields(nil))
		if err := c2.SkipTaggedFields(); err != nil {
			t.Fatalf("skip empty tagged: %v", err)
		}
		if c2.Remaining() != 0 {
			t.Fatalf("empty tagged: %d remaining", c2.Remaining())
		}
	})
}

func TestTruncationErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		buf  []byte
		read func(*Cursor) error
	}{
		{"int16", []byte{0x00}, func(c *Cursor) error { _, err := c.ReadInt16(); return err }},
		{"int32", []byte{0x00, 0x01}, func(c *Cursor) error { _, err := c.ReadInt32(); return err }},
		{"string len", []byte{}, func(c *Cursor) error { _, err := c.ReadString(); return err }},
		{"string body", []byte{0x00, 0x05, 'a'}, func(c *Cursor) error { _, err := c.ReadString(); return err }},
		{"uvarint", []byte{0x80}, func(c *Cursor) error { _, err := c.ReadUvarint(); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewCursor(tc.buf)
			if err := tc.read(&c); !errors.Is(err, ErrTruncated) {
				t.Fatalf("expected ErrTruncated, got %v", err)
			}
		})
	}
}

func FuzzPrimitivesDontPanic(f *testing.F) {
	f.Add([]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07})
	f.Fuzz(func(t *testing.T, data []byte) {
		c := NewCursor(data)
		// Hammer every reader with the same buffer; we only assert no panics
		// and that errors are well-formed.
		_, _ = c.ReadInt8()
		c = NewCursor(data)
		_, _ = c.ReadInt16()
		c = NewCursor(data)
		_, _ = c.ReadInt32()
		c = NewCursor(data)
		_, _ = c.ReadInt64()
		c = NewCursor(data)
		_, _ = c.ReadUvarint()
		c = NewCursor(data)
		if _, err := c.ReadString(); err != nil && !isExpected(err) {
			t.Fatalf("string: %v", err)
		}
		c = NewCursor(data)
		if _, _, err := c.ReadNullableString(); err != nil && !isExpected(err) {
			t.Fatalf("nullable string: %v", err)
		}
		c = NewCursor(data)
		_ = c.SkipTaggedFields()
	})
}

func isExpected(err error) bool {
	return errors.Is(err, ErrTruncated) || errors.Is(err, ErrMalformed)
}

// Confirm AppendUvarint is the inverse of ReadUvarint across an interesting
// range without allocating a fresh buffer per iteration.
func BenchmarkUvarintRoundTrip(b *testing.B) {
	var buf [8]byte
	for i := 0; i < b.N; i++ {
		out := AppendUvarint(buf[:0], uint32(i))
		c := NewCursor(out)
		v, err := c.ReadUvarint()
		if err != nil || v != uint32(i) {
			b.Fatalf("mismatch: %d != %d (%v)", v, i, err)
		}
	}
}

// Compile-time assertion that we wired %w correctly.
var _ = fmt.Errorf("%w", ErrTruncated)
