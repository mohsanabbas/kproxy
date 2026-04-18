# Go Performance — Measurement, Profiling, Optimization

A condensed performance reference. **The overriding rule: measure first.** Go is fast by
default; most code doesn't need optimization. When it does, `pprof` and benchmarks tell you
where — intuition is frequently wrong.

## Table of contents

1. [The measurement hierarchy](#1-the-measurement-hierarchy)
2. [Benchmarks with `benchstat`](#2-benchmarks-with-benchstat)
3. [`pprof` — CPU and memory profiling](#3-pprof--cpu-and-memory-profiling)
4. [Escape analysis](#4-escape-analysis)
5. [`sync.Pool` for allocation reduction](#5-syncpool-for-allocation-reduction)
6. [Struct field alignment](#6-struct-field-alignment)
7. [String and `[]byte` conversions](#7-string-and-byte-conversions)
8. [Slice growth and preallocation](#8-slice-growth-and-preallocation)
9. [Avoiding interface boxing](#9-avoiding-interface-boxing)
10. [PGO (profile-guided optimization)](#10-pgo-profile-guided-optimization)
11. [Common premature optimizations to avoid](#11-common-premature-optimizations-to-avoid)

---

## 1. The measurement hierarchy

Optimize in this order — don't skip:

1. **Algorithm.** O(n²) is O(n²) no matter how fast the instructions. Most "slow Go" is a
   linear search in a hot loop that should be a map lookup.
2. **Allocations.** GC pressure kills throughput. `-benchmem` shows allocations per op. Driving
   this toward zero usually gives bigger wins than micro-optimizing arithmetic.
3. **Concurrency.** I/O-bound code parallelizes well; CPU-bound less so past `GOMAXPROCS`
   cores. See `references/concurrency-patterns.md` for patterns.
4. **Cache locality.** Structs of arrays beat arrays of structs for streaming workloads.
   Rare, but occasionally the difference between 2 GB/s and 8 GB/s.
5. **Micro-optimizations.** Inline tiny functions, reuse buffers, strength-reduce operations.
   Last resort; often worth <10%.

## 2. Benchmarks with `benchstat`

Benchmark template:

```go
func BenchmarkEncode(b *testing.B) {
    data := makeData(1000)
    b.ResetTimer()
    b.ReportAllocs()
    for i := 0; i < b.N; i++ {
        _, _ = Encode(data)
    }
}
```

Run with:

```bash
go test -bench=. -benchmem -count=10 -run=^$ > results.txt
```

`-count=10` runs each benchmark 10× so `benchstat` can compute statistical significance.

Install `benchstat`:

```bash
go install golang.org/x/perf/cmd/benchstat@latest
```

Compare before/after:

```bash
benchstat old.txt new.txt
```

Output shows mean, variance, and p-value. **If p > 0.05, the difference isn't significant** —
the "improvement" might be noise. Always `-count=10` or higher.

**Sub-benchmarks for sweeps:**

```go
func BenchmarkEncode(b *testing.B) {
    for _, n := range []int{10, 100, 1000, 10000} {
        b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
            data := makeData(n)
            b.ResetTimer()
            for i := 0; i < b.N; i++ { _, _ = Encode(data) }
        })
    }
}
```

Reveals how your function scales. If `N=1000` is 200× slower than `N=100` instead of 10×,
you have a hidden quadratic.

## 3. `pprof` — CPU and memory profiling

Enable in tests or programs:

```go
import _ "net/http/pprof"

go func() { log.Println(http.ListenAndServe("localhost:6060", nil)) }()
```

Then:

```bash
# CPU profile for 30 seconds
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30

# Heap profile (allocations since last GC)
go tool pprof http://localhost:6060/debug/pprof/heap

# Goroutine snapshot (find leaks)
go tool pprof http://localhost:6060/debug/pprof/goroutine

# Mutex contention
go tool pprof http://localhost:6060/debug/pprof/mutex  # needs runtime.SetMutexProfileFraction
```

Inside pprof:

```
(pprof) top            # top 10 hot functions
(pprof) top -cum       # by cumulative (including callees)
(pprof) list FuncName  # annotated source with time per line
(pprof) web            # graph view in browser (needs graphviz)
(pprof) flame          # flame graph
```

**For benchmarks**, write profiles directly:

```bash
go test -bench=BenchmarkX -cpuprofile=cpu.out -memprofile=mem.out -run=^$
go tool pprof cpu.out
```

**The 3 profiles to start with:**

1. **CPU** — what's spending time. Start here if the program is slow.
2. **Allocs** — `/debug/pprof/allocs` (total allocations, not just live). Start here if GC
   shows up in CPU profile.
3. **Heap** — what's resident right now. Start here if the program uses too much memory.

**Don't guess.** Profile first, optimize what the profile says is hot.

## 4. Escape analysis

Go decides at compile time whether a variable lives on the stack or heap. Stack is free;
heap costs a GC cycle later.

```bash
go build -gcflags="-m=2" ./... 2>&1 | grep "escapes to heap"
```

Common causes of escape:

- **Taking the address of a local that outlives the function.** Returning `&x` forces `x` to
  the heap.
- **Storing a value in an interface.** Interface stores require heap allocation for most
  types (small ones might fit in the interface's inline storage).
- **Storing a value in a closure that outlives the scope.** The captured variable escapes.
- **Putting a value in a channel or slice that outlives the function.**

Sometimes escape is unavoidable (returning `*T` from a constructor is normal). Worry only
when the profiler shows allocation pressure.

**Reducing escape:**

- Return values, not pointers, for small structs (under ~128 bytes).
- Use `*bytes.Buffer` from a pool instead of `[]byte` literals in hot paths.
- Avoid storing large structs in `interface{}`/`any`.

## 5. `sync.Pool` for allocation reduction

Covered in `references/sync-primitives.md` section 5. The short version:

```go
var bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

buf := bufPool.Get().(*bytes.Buffer)
buf.Reset()   // CRITICAL
defer bufPool.Put(buf)
```

Appropriate for objects that are:

- Expensive to allocate (large buffers, complex structs).
- Short-lived (the pool is cleared each GC cycle — it's not a long-lived cache).
- Frequently reused in hot loops.

Not appropriate for tiny types or things you rarely allocate. Pooling overhead can exceed
allocation cost for simple cases — benchmark before committing.

## 6. Struct field alignment

Go aligns struct fields by their natural alignment (int64 on 8-byte boundary, etc.), which
can cause padding:

```go
type Wasteful struct {
    a bool   // 1 byte + 7 padding
    b int64  // 8 bytes
    c bool   // 1 byte + 7 padding
}
// Total: 24 bytes
```

Reorder by size, largest first:

```go
type Tight struct {
    b int64  // 8 bytes
    a bool   // 1 byte
    c bool   // 1 byte
    // 6 bytes padding at end
}
// Total: 16 bytes
```

Tooling:

```bash
go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@latest
fieldalignment -fix ./...
```

**Only matters at scale** — 10 million instances × 8 bytes saved = 80 MB. For a struct you
allocate twice, it's irrelevant.

Also enable via golangci-lint: `govet: enable: [fieldalignment]`.

## 7. String and `[]byte` conversions

`string([]byte)` and `[]byte(string)` conversions copy bytes. In hot paths, this shows up in
profiles as `runtime.stringtobytes` or `runtime.slicebytetostring`.

**Tricks to avoid copies:**

```go
// Read bytes from a string without conversion:
for i := 0; i < len(s); i++ {
    b := s[i]  // no allocation
}

// Unsafe zero-copy conversion (Go 1.20+, requires careful review):
import "unsafe"
b := unsafe.Slice(unsafe.StringData(s), len(s))   // []byte view into s
s := unsafe.String(unsafe.SliceData(b), len(b))   // string view into b
// WARNING: the original must outlive the alias; modifications to either
// violate Go's immutability guarantees for strings. Use with extreme caution.
```

**Avoid `fmt.Sprintf` for simple concatenation.** `a + b + c` or `strings.Builder` is faster:

```go
var sb strings.Builder
sb.Grow(len(prefix) + len(body) + len(suffix))  // preallocate
sb.WriteString(prefix)
sb.WriteString(body)
sb.WriteString(suffix)
result := sb.String()
```

## 8. Slice growth and preallocation

Appending past capacity reallocates. For known sizes, preallocate:

```go
// BAD: multiple reallocations as it grows
s := []int{}
for i := 0; i < n; i++ { s = append(s, i) }

// GOOD: one allocation
s := make([]int, 0, n)
for i := 0; i < n; i++ { s = append(s, i) }

// BETTER if you're filling densely:
s := make([]int, n)
for i := 0; i < n; i++ { s[i] = i }
```

The `append` growth strategy roughly doubles capacity up to ~1024 elements, then grows by
~25%. Knowing the final size saves 2-3 reallocations in typical cases.

Same pattern for maps:

```go
m := make(map[string]int, n)   // preallocate if n is known
```

## 9. Avoiding interface boxing

Storing a value in an interface requires a heap allocation for most types:

```go
var i interface{} = int(42)   // heap allocation to box the 42
```

In hot loops, this shows up as tens of millions of allocations. Fixes:

- **Use generics** (Go 1.18+) — type parameters don't box:

  ```go
  func Max[T cmp.Ordered](a, b T) T { if a > b { return a }; return b }
  ```

- **Use concrete types** in struct fields instead of `any`/`interface{}`. This is usually
  what you want anyway — `any` is the least-informative type in the language.

- **For serialization hotpaths**, custom encoders that avoid `fmt` and `encoding/json`
  reflection pay off. `go.uber.org/zap`-style encoders, code-generated marshalers, etc.

## 10. PGO (profile-guided optimization)

Since Go 1.21, you can feed a profile back to the compiler:

```bash
# 1. Run the program in production or under benchmark, capture CPU profile.
curl -o default.pgo http://localhost:6060/debug/pprof/profile?seconds=60

# 2. Place default.pgo in the main package.
mv default.pgo ./cmd/myservice/

# 3. Build — the compiler uses the profile to guide inlining and specialization.
go build -pgo=auto ./cmd/myservice
```

Typical wins: 2–10% across realistic workloads, with no code changes. Appropriate for
production binaries where you have representative profiles.

**Caveats:** the profile must be representative; an unrepresentative profile can make things
slower. Re-profile after major code changes.

## 11. Common premature optimizations to avoid

Go newcomers often reach for these thinking they'll matter. Usually they don't:

- **"I'll use `[64]byte` arrays instead of `[]byte` slices to avoid allocation."** Slices
  are pointers to a header; unless the slice escapes, there's no allocation. Benchmark
  before adding complexity.
- **"I'll use `sync.Pool` everywhere."** Pools have overhead. Pool only when the profile
  shows allocation pressure on that specific type.
- **"I'll use `unsafe` for speed."** `unsafe` breaks compiler guarantees and can be *slower*
  when it prevents optimizations. Use only for specific, measured wins (e.g. string/byte
  aliasing) and wall it off in a small, reviewable function.
- **"I'll avoid deferred functions — they're slow."** `defer` has a tiny cost (a few ns per
  call). It's almost never a hot-path concern. Use defer; the clarity is worth it.
- **"I'll inline everything manually."** The Go compiler inlines aggressively. Your manual
  inlining usually doesn't help and clutters the code.
- **"Channels are slow; I'll use mutexes everywhere."** Channels have overhead, but so do
  mutexes. Benchmark the specific case. Don't replace a clear channel design with a tangled
  mutex one without evidence.

**The Go philosophy on performance:** write clear code. Measure. Optimize the measured hot
spot. Then measure again to confirm the win. Most code ships as-written and is plenty fast.

---

## Quick reference

```bash
# Bench, with allocations and statistical significance
go test -bench=. -benchmem -count=10 -run=^$

# CPU profile from a benchmark
go test -bench=BenchmarkX -cpuprofile=cpu.out -run=^$
go tool pprof -http=: cpu.out

# Escape analysis
go build -gcflags="-m=2" ./... 2>&1 | grep escapes

# Race detector (always)
go test -race ./...

# Field alignment fix
fieldalignment -fix ./...

# Install helpful tools
go install golang.org/x/perf/cmd/benchstat@latest
go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@latest
```
