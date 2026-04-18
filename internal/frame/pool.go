package frame

import "sync"

// Buffer is a length-prefixed Kafka frame's payload — the bytes after the
// 4-byte length prefix. Buffers are pooled to avoid allocation on every frame.
//
// A Buffer is owned by exactly one goroutine at a time. Calling Release while
// another goroutine still references the underlying slice is a data race.
type Buffer struct {
	b []byte
}

// Bytes returns the payload bytes. The slice is valid only until Release is
// called.
func (b *Buffer) Bytes() []byte { return b.b }

// Len returns the payload length.
func (b *Buffer) Len() int { return len(b.b) }

// Reset truncates the payload to zero length but keeps the underlying capacity.
func (b *Buffer) Reset() { b.b = b.b[:0] }

// grow ensures the buffer has at least n bytes of length, allocating if the
// pooled capacity is too small.
func (b *Buffer) grow(n int) {
	if cap(b.b) >= n {
		b.b = b.b[:n]
		return
	}
	// Allocate a new backing slice. We deliberately do NOT round up here — the
	// caller already knows the exact frame length.
	b.b = make([]byte, n)
}

// pool tiers — buffers larger than maxCap are dropped on Release rather than
// returned to the pool, to bound steady-state memory.
const maxCap = 1 << 20 // 1 MiB; frames bigger than this are usually one-off Produce/Fetch

var pool = sync.Pool{
	New: func() any { return &Buffer{b: make([]byte, 0, 8<<10)} }, // 8 KiB seed
}

// Get returns a Buffer with its slice truncated to zero length. The caller
// must call Release exactly once when done.
func Get() *Buffer {
	b := pool.Get().(*Buffer)
	b.b = b.b[:0]
	return b
}

// Release returns a Buffer to the pool. Buffers whose capacity exceeds maxCap
// are dropped instead of pooled to keep memory bounded.
func Release(b *Buffer) {
	if b == nil {
		return
	}
	if cap(b.b) > maxCap {
		// Drop oversized buffers; the GC will reclaim them.
		return
	}
	b.b = b.b[:0]
	pool.Put(b)
}
