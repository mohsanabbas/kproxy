# Go Gotchas — The Complete Survival Guide

Gotchas curated from Matt Holiday's go-class "Gotchas" slides, the 50-Shades-of-Go compilation,
and years of production Go bug tickets. Each entry: symptom → cause → fix. Scan these before
shipping anything non-trivial.

## Table of contents

1. [Concurrency gotchas (the dangerous ones)](#concurrency-gotchas)
2. [Channel gotchas](#channel-gotchas)
3. [Slice and map gotchas](#slice-and-map-gotchas)
4. [Interface gotchas](#interface-gotchas)
5. [Error handling gotchas](#error-handling-gotchas)
6. [Defer gotchas](#defer-gotchas)
7. [Numeric and string gotchas](#numeric-and-string-gotchas)
8. [Module and build gotchas](#module-and-build-gotchas)

---

## Concurrency gotchas

### 1. Goroutine leak via unreceived send

```go
// BAD — if the caller never reads from ch, this goroutine lives forever.
go func() { ch <- expensiveValue }()
```

**Fix:** size the channel to guarantee the send can complete, or use `select` with
`<-ctx.Done()` on the send:

```go
select {
case ch <- v:
case <-ctx.Done():
    return
}
```

### 2. Loop variable capture (pre-Go 1.22)

```go
// BAD in go.mod < 1.22 — all goroutines see the final value of i.
for i, v := range items {
    go func() {
        process(i, v)  // captures loop vars by reference
    }()
}
```

**Fix (pre-1.22):** declare per-iteration copies or pass as arguments:

```go
for i, v := range items {
    i, v := i, v  // shadow with per-iteration copy
    go func() { process(i, v) }()
}
// OR
for i, v := range items {
    go func(i int, v Item) { process(i, v) }(i, v)
}
```

**Go 1.22+:** the loop variable is per-iteration by default. This bug is fixed — but verify
your `go.mod` says `go 1.22` or later. Older libraries may still have it.

### 3. `WaitGroup.Add` inside the spawned goroutine

```go
// BAD — Wait() can race past Add() if Wait runs before the goroutine starts
var wg sync.WaitGroup
for _, t := range tasks {
    go func() {
        wg.Add(1)  // WRONG
        defer wg.Done()
        work(t)
    }()
}
wg.Wait()
```

**Fix:** `Add` in the spawning goroutine before `go`, or use the Go 1.25+ `wg.Go`:

```go
var wg sync.WaitGroup
for _, t := range tasks {
    wg.Go(func() { work(t) })   // handles Add/Done for you
}
wg.Wait()
```

### 4. `sync.Mutex` copied by value

```go
// BAD — passing s by value copies the mutex; the two mutexes don't protect each other.
type Counter struct { mu sync.Mutex; n int }
func (c Counter) Inc() { c.mu.Lock(); c.n++; c.mu.Unlock() }  // wrong receiver
```

**Fix:** always use pointer receivers for types with a mutex. `go vet` catches this.

```go
func (c *Counter) Inc() { c.mu.Lock(); c.n++; c.mu.Unlock() }
```

### 5. Forgetting `cancel()` on `context.With*`

```go
// BAD — ctx leaks resources (timer, goroutine in WithTimeout) until parent is canceled.
ctx, _ := context.WithTimeout(parent, time.Second)
do(ctx)
```

**Fix:** always `defer cancel()` on the line immediately after:

```go
ctx, cancel := context.WithTimeout(parent, time.Second)
defer cancel()
do(ctx)
```

`golangci-lint` with `govet: enable: [lostcancel]` catches this.

### 6. `time.After` leak in a long-running loop

```go
// BAD — each iteration allocates a new timer that doesn't fire until d elapses.
for {
    select {
    case v := <-ch:
        handle(v)
    case <-time.After(d):
        return ErrTimeout
    }
}
```

**Fix:** one timer, reset on each iteration, or use `context.WithTimeout`:

```go
t := time.NewTimer(d)
defer t.Stop()
for {
    t.Reset(d)
    select {
    case v := <-ch:
        if !t.Stop() { <-t.C }  // drain before Reset (pre-Go 1.23)
        handle(v)
    case <-t.C:
        return ErrTimeout
    }
}
```

Go 1.23+: `Timer.Reset` is safe to call directly without the drain dance. Check your target
version.

### 7. Unbuffered channel → deadlock

```go
// BAD — no receiver, sender blocks forever.
ch := make(chan int)
ch <- 1   // deadlock
```

**Fix:** either buffer the channel or guarantee a concurrent receiver:

```go
ch := make(chan int, 1)
ch <- 1   // fits in buffer
```

### 8. `select { default: }` busy-loop

```go
// BAD — this busy-loops if no case is ready, burning CPU.
for {
    select {
    case v := <-ch:
        handle(v)
    default:
        // "do nothing and try again"
    }
}
```

**Fix:** remove `default` unless you specifically want non-blocking polling. If you do, add a
`time.Sleep` or event-driven wakeup.

### 9. Closing a channel twice panics

```go
close(ch); close(ch)  // second call: panic
```

**Fix:** have exactly one goroutine responsible for closing. If unavoidable, use `sync.Once`:

```go
var once sync.Once
closeFunc := func() { once.Do(func() { close(ch) }) }
```

### 10. Closing a channel someone else writes to

```go
// BAD — receiver can't know when senders are done.
close(ch) // in consumer
```

**Fix:** only the sender closes. If multiple senders, use a `sync.WaitGroup` + single closer.

### 11. `http.DefaultClient` with no timeout

```go
resp, err := http.Get(url)   // uses http.DefaultClient — no timeout!
```

**Fix:** always construct your own client:

```go
var client = &http.Client{
    Timeout: 10 * time.Second,
    Transport: &http.Transport{
        DialContext:           (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
        ResponseHeaderTimeout: 5 * time.Second,
        IdleConnTimeout:       90 * time.Second,
    },
}
```

### 12. Shared slice/map write race

```go
// BAD — concurrent map writes panic in Go's runtime.
m := map[string]int{}
for _, k := range keys { go func(k string) { m[k]++ }(k) }
```

**Fix:** `sync.Mutex`, `sync.RWMutex`, or `sync.Map` (only for specific access patterns —
read-mostly with disjoint keys per goroutine). For counter-heavy workloads, `atomic.Int64`.

### 13. Data race via slice header copy

```go
// BAD — both goroutines see the same backing array.
s := make([]int, 100)
go func() { s[0] = 1 }()
go func() { s[0] = 2 }()
```

Passing `s` to two goroutines doesn't "copy" it — slice headers share the backing array. Use a
mutex, or partition the work so each goroutine writes disjoint indices.

### 14. `sync.Pool.Get()` without reset

```go
// BAD — reusing a stale object contaminates the next caller.
buf := bufPool.Get().(*bytes.Buffer)
buf.Write(...)  // still has contents from the last user!
```

**Fix:**

```go
buf := bufPool.Get().(*bytes.Buffer)
buf.Reset()  // IMMEDIATELY after Get
defer bufPool.Put(buf)
```

Also: `sync.Pool` is cleared on every GC cycle. It is a hint, not a durable cache.

---

## Channel gotchas

### 15. Receive on nil channel blocks forever

```go
var ch chan int
<-ch   // forever
```

This is sometimes intentional inside a `select` to disable a case (see the subscription pattern
in `concurrency-patterns.md`). If unintentional, it's a bug.

### 16. `for range ch` doesn't exit if ch is never closed

```go
// Hangs forever — sender never closes.
for v := range ch {
    process(v)
}
```

**Fix:** always have a clear owner for closing, or use explicit `select` with `ctx.Done()`.

### 17. Buffered channel masking deadlocks

```go
ch := make(chan int, 1000)
for i := 0; i < 500; i++ { ch <- i }
// never read — seems fine in tests, blows up in prod with more inputs
```

A large buffer can hide that no one is reading. Default to unbuffered in tests; deadlocks then
surface immediately.

---

## Slice and map gotchas

### 18. `append` aliasing — modifying a slice mutates its source

```go
a := []int{1, 2, 3, 4, 5}
b := a[:2]           // len=2, cap=5, shares backing
b = append(b, 99)    // overwrites a[2]!
// a is now {1, 2, 99, 4, 5}
```

**Fix:** if you need an independent copy, use the full-slice expression or `slices.Clone`:

```go
b := slices.Clone(a[:2])        // Go 1.21+
// or
b := append([]int(nil), a[:2]...)
```

### 19. Nil slice vs empty slice

```go
var s []int    // nil slice, len=0
s2 := []int{}  // non-nil slice, len=0
fmt.Println(s == nil, s2 == nil)  // true false
```

Both iterate as empty in `range`. For JSON: nil becomes `null`, empty becomes `[]`. Choose
consciously; prefer nil for uninitialized ("no data yet") and empty for explicit "empty list".

### 20. Retaining a huge backing array via a small slice

```go
huge := readMegabyteFile()
tiny := huge[:10]   // tiny keeps the whole megabyte alive!
```

**Fix:** `slices.Clone(tiny)` or copy to a fresh slice before discarding `huge`.

### 21. Map iteration order is randomized

```go
for k, v := range m {
    // order is intentionally randomized per iteration
}
```

If you need deterministic order, collect keys into a slice and sort them:

```go
keys := make([]string, 0, len(m))
for k := range m { keys = append(keys, k) }
sort.Strings(keys)
for _, k := range keys { use(k, m[k]) }
```

### 22. Can't take the address of a map value

```go
m := map[string]S{"a": {...}}
p := &m["a"]    // compile error: cannot take address
m["a"].field = v  // compile error: cannot assign to struct field in map
```

**Fix:** use `map[string]*S`, or read-modify-write:

```go
v := m["a"]; v.field = x; m["a"] = v
```

---

## Interface gotchas

### 23. Nil interface ≠ interface containing nil

```go
func foo() error {
    var p *MyError = nil
    return p    // returns a NON-nil error!
}

if foo() != nil { ... }  // this is true! p is nil but the interface is not.
```

The `error` interface value has type `*MyError` and data `nil` — the interface itself is
non-nil because it has a type. **Fix:** return `nil` explicitly:

```go
func foo() error {
    var p *MyError = nil
    if p != nil { return p }
    return nil
}
```

### 24. Interface satisfaction is structural — no "implements" keyword

A type satisfies an interface if it has the right methods. Compile-time assertion:

```go
var _ io.Reader = (*MyType)(nil)
```

Put this near the type definition to get a compile error if the interface stops being
satisfied.

### 25. Defining interfaces at the producer, not the consumer

```go
// BAD — producer defines an interface clients might not need.
package userdb
type UserReader interface { Read(id int) User }
```

**Better:** producer exports concrete types; consumer defines the interface it needs.

```go
package report
type userSource interface { Read(id int) User }  // defined where it's used
func Generate(src userSource) Report { ... }
```

This is the Go idiom: "Accept interfaces, return structs."

---

## Error handling gotchas

### 26. String-comparing errors

```go
// BAD — fragile; any error string change breaks this.
if err.Error() == "record not found" { ... }
```

**Fix:** sentinel errors + `errors.Is`:

```go
// producer side
var ErrNotFound = errors.New("record not found")
return ErrNotFound

// consumer side
if errors.Is(err, ErrNotFound) { ... }
```

### 27. `fmt.Errorf` without `%w`

```go
return fmt.Errorf("load user: %v", err)   // stringifies, loses type
```

**Fix:** use `%w` to wrap — preserves the error chain for `errors.Is`/`errors.As`:

```go
return fmt.Errorf("load user: %w", err)
```

### 28. Ignoring errors with `_`

```go
f, _ := os.Open(path)   // now f may be nil; segfault imminent
```

**Fix:** always check. If the error is truly irrelevant, comment why:

```go
// Close errors are non-actionable here; the buffer is already flushed.
_ = writer.Close()
```

### 29. Panic across goroutine boundaries

```go
go work()  // if work() panics, the process dies — another goroutine can't recover it
```

**Fix:** if you're running untrusted or fallible work in a goroutine, add a recovery wrapper:

```go
func safely(fn func()) {
    defer func() {
        if r := recover(); r != nil {
            slog.Error("goroutine panic", "panic", r, "stack", string(debug.Stack()))
        }
    }()
    fn()
}

go safely(work)
```

---

## Defer gotchas

### 30. Arguments to deferred calls are evaluated at `defer` time

```go
i := 0
defer fmt.Println("i =", i)   // prints "i = 0" regardless of what happens later
i = 42
```

**Fix:** if you need the current value at return time, use a closure:

```go
defer func() { fmt.Println("i =", i) }()
```

### 31. `defer` in a loop stacks up until function return

```go
for _, f := range files {
    fp, _ := os.Open(f)
    defer fp.Close()   // BAD — all closes happen at function return, after processing all files
    process(fp)
}
```

**Fix:** extract the body into a function so `defer` runs per-iteration:

```go
for _, f := range files {
    func() {
        fp, _ := os.Open(f)
        defer fp.Close()
        process(fp)
    }()
}
```

### 32. `defer` doesn't run if the process is killed

`defer` runs on normal return and on panic, but NOT on `os.Exit` or a SIGKILL. For resource
cleanup that matters across process termination, use signal handlers (`signal.NotifyContext`).

---

## Numeric and string gotchas

### 33. Integer overflow is silent

```go
var i int8 = 127
i++   // now -128, no error, no panic
```

Go doesn't panic on integer overflow. For money, counters, or anything security-sensitive,
either use `math/big`, bound inputs, or check with `math/bits` helpers.

### 34. `int` vs `int32` vs `int64` on different platforms

`int` is 32-bit on 32-bit platforms, 64-bit elsewhere. For wire formats, database columns, and
protocol fields, always use explicit-width types (`int32`, `int64`, `uint64`).

### 35. String indexing returns bytes, not runes

```go
s := "résumé"
fmt.Println(s[0])   // 114 ('r'), fine
fmt.Println(s[1])   // 195, first byte of 'é' — not a rune
```

**Fix:** convert to `[]rune` for Unicode indexing, or iterate with `range` which yields
`(byteIndex, rune)`.

### 36. `len(s)` on a string returns bytes, not runes

```go
len("résumé")              // 8 bytes
utf8.RuneCountInString(...)  // 6 runes
```

---

## Module and build gotchas

### 37. `go.mod` `go` directive controls language semantics

`go 1.22` or later enables per-iteration loop variables. If your `go.mod` is `go 1.21`, you
still have the old behavior even on a 1.26 toolchain. Keep this up to date.

### 38. Vendoring vs modules — pick one

Don't mix vendor directory and module mode without understanding the precedence. `GOFLAGS=-mod=vendor`
forces vendor mode; `-mod=mod` forces module mode.

### 39. `internal/` packages are compiler-enforced

`foo/internal/bar` can only be imported by packages under `foo/`. Use this aggressively to
define public vs. implementation-detail APIs. No third-party tool required.

### 40. `_test.go` files are excluded from builds

Production code can't import test helpers. If you need test utilities across packages, put
them in a `testutil/` package (no `_test.go` suffix) — but keep it in an internal location so
it isn't shipped.

---

## How to actually catch these

Most of the gotchas above have tooling that catches them:

| Gotcha category         | Tool                                       |
|-------------------------|--------------------------------------------|
| Mutex copying           | `go vet`                                   |
| Lost cancel             | `golangci-lint` (`govet: lostcancel`)      |
| Loop variable (< 1.22)  | `golangci-lint` (`copyloopvar`, `loopclosure`) |
| Races                   | `go test -race ./...`                      |
| Goroutine leaks in tests | `testing/synctest` (Go 1.25+) or `go.uber.org/goleak` |
| Goroutine leaks in prod | Go 1.26 experimental `goroutineleak` profile |
| Unhandled errors        | `errcheck` via golangci-lint               |
| Security issues         | `gosec`                                    |
| Field alignment         | `fieldalignment -fix ./...`                |

Wire these into CI. Running the race detector + `golangci-lint` catches probably 80% of the
gotchas above before a human sees them.
