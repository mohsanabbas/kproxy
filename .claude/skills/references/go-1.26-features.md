# Go 1.22–1.26 Modernization Guide

Go ships every six months. Between Go 1.21 (August 2023) and Go 1.26 (February 2026), the
language and stdlib gained substantial features that change idiomatic patterns. This file is
the "stop writing it the old way" reference.

## Quick hit list what changed

| Feature                      | Version | Impact                                                  |
|------------------------------|---------|---------------------------------------------------------|
| Loop variable per-iteration  | 1.22    | Fixes the decade-old `for range + go func()` bug       |
| `slices` / `maps` stdlib     | 1.21    | Replaces hand-rolled helpers and `golang.org/x/exp`     |
| `sync.OnceValue/OnceValues`  | 1.21    | Cleaner than raw `sync.Once` + package var              |
| `clear(m)` builtin           | 1.21    | Empties maps and zeros slices                           |
| `min(a, b)` / `max(a, b)`    | 1.21    | No more local helpers                                   |
| `log/slog`                   | 1.21    | Stdlib structured logging                               |
| `range func` iterators       | 1.23    | Custom iteration without goroutines                     |
| `iter.Seq[T]` / `iter.Seq2`  | 1.23    | Canonical iterator types                                |
| `testing/synctest` (exp)     | 1.24    | Deterministic concurrency tests                         |
| `testing/synctest` stable    | 1.25    | Production-ready API                                    |
| `sync.WaitGroup.Go`          | 1.25    | Kills the "Add inside goroutine" bug                    |
| Generic type aliases         | 1.24    | `type Map[K, V] = map[K]V` etc.                         |
| Revamped `go fix`            | 1.26    | Automated modernization (`go fix ./...`)                |
| Experimental SIMD            | 1.26    | `simd/archsimd` for perf-critical code                  |
| Experimental `runtime/secret`| 1.26    | Secure-erase crypto temporaries                         |
| Experimental goroutineleak   | 1.26    | pprof profile for leaked goroutines in production       |
| `crypto/hpke`, `crypto/mlkem`| 1.26    | Post-quantum crypto primitives                          |

Below: deeper notes on the ones you'll actually use.

---

## Loop variables (Go 1.22)

**Before 1.22** classic gotcha:

```go
for _, v := range items {
    go func() { fmt.Println(v) }()   // all print the LAST value
}
```

**Go 1.22+** each iteration gets its own `v`:

```go
for _, v := range items {
    go func() { fmt.Println(v) }()   // prints each value correctly
}
```

**Caveat:** the semantics depend on your `go.mod` `go` directive. If it says `go 1.21`, you
get old semantics even on a 1.26 toolchain. Always bump your `go.mod` directive to match
your intent.

To check:

```bash
head -3 go.mod   # look for "go 1.22" or later
```

If you're modernizing a codebase, bump `go.mod` and then let `go vet` find the remaining
copies that were working around the old bug (`copyloopvar` in golangci-lint flags them as
now-redundant).

## `slices` and `maps` packages (Go 1.21)

Replace hand-rolled helpers. Top functions:

```go
// slices
slices.Contains(s, v)
slices.Index(s, v)
slices.Sort(s)
slices.SortFunc(s, cmp)
slices.IsSorted(s)
slices.Equal(a, b)
slices.Clone(s)               // safe independent copy
slices.Reverse(s)
slices.Delete(s, i, j)
slices.Insert(s, i, vs...)
slices.Concat(a, b, c)        // since 1.22

// maps
maps.Keys(m)                  // returns an iter.Seq since 1.23
maps.Values(m)
maps.Clone(m)
maps.Copy(dst, src)
maps.Equal(a, b)
maps.DeleteFunc(m, fn)
```

**Biggest win: `slices.Clone` / `maps.Clone`.** The number-one subtle slice bug is aliasing
(modifying a sub-slice mutates the source). `slices.Clone` is the fix, and it's one import
away.

## `clear`, `min`, `max` builtins (Go 1.21)

```go
clear(m)     // empty map; retains capacity
clear(s)     // zeros all elements in slice s (len unchanged)

x := min(a, b, c)
y := max(a, b, c)
```

Replace:
- `for k := range m { delete(m, k) }` with `clear(m)`
- Helper `func Min(a, b int) int { if a < b { return a }; return b }` with `min(a, b)`

## `log/slog` (Go 1.21)

Stdlib structured logging covered in `idiomatic-go.md` section 10. Replaces the
hodgepodge of `logrus`, `zap`, `zerolog` for 90% of projects.

```go
slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
slog.Info("server started", "addr", addr, "pid", os.Getpid())
```

Custom attributes, groups, handlers, redaction all in the package. Read the godoc once;
you'll rarely need anything else.

## `range`-over-func iterators (Go 1.23)

Custom iteration without goroutines. Define an `iter.Seq[T]` or `iter.Seq2[K, V]`:

```go
import "iter"

func Backwards[T any](s []T) iter.Seq2[int, T] {
    return func(yield func(int, T) bool) {
        for i := len(s) - 1; i >= 0; i-- {
            if !yield(i, s[i]) {
                return
            }
        }
    }
}

// Consumer:
for i, v := range Backwards([]string{"a", "b", "c"}) {
    fmt.Println(i, v)   // 2 c, 1 b, 0 a
}
```

**When to use iterators instead of channels:**

- Pure pull-based iteration, no I/O concurrency needed.
- You want zero goroutine overhead.
- The consumer may want to stop early (the `yield` returning `false`).

**When channels still win:**

- The producer needs to do concurrent work (I/O, CPU in parallel).
- Coordination with cancellation across goroutines.

`maps.Keys` and `maps.Values` now return `iter.Seq`, which is a breaking change if you had
code expecting a slice use `slices.Collect(maps.Keys(m))` to get a slice back.

## `testing/synctest` (stable in Go 1.25)

Deterministic concurrency tests. Covered in depth in `testing.md` section 5.

**Old API** (Go 1.24 under `GOEXPERIMENT=synctest`): `synctest.Run(func() { ... })`. Deprecated
in 1.26; don't use in new code.

**New API** (stable Go 1.25+):

```go
synctest.Test(t, func(tb *testing.T) {
    // code under test goroutines and time are deterministic
    synctest.Wait(tb)  // block until all goroutines in bubble are blocked
})
```

Inside the bubble: `time.Sleep`, `time.After`, `time.NewTimer` all use a virtual clock that
advances only when no goroutine is runnable. Net effect: your test runs instantly regardless
of the durations it simulates, and the outcome is deterministic.

**This changes how you write tests for anything timer- or scheduler-dependent.** If a test
previously took 500ms because of `time.Sleep` or similar, it now takes ~microseconds.

## `sync.WaitGroup.Go` (Go 1.25)

```go
// Before:
var wg sync.WaitGroup
for _, t := range tasks {
    wg.Add(1)
    go func(t Task) { defer wg.Done(); t.Run() }(t)
}
wg.Wait()

// After:
var wg sync.WaitGroup
for _, t := range tasks {
    wg.Go(func() { t.Run() })   // captures t correctly in 1.22+
}
wg.Wait()
```

`wg.Go` handles `Add(1)` + `go` + `defer wg.Done()` in one call, eliminating the common bug
of calling `Add` inside the spawned goroutine.

## Generic type aliases (Go 1.24)

```go
type StringMap[V any] = map[string]V

var users StringMap[*User] = make(StringMap[*User])
// identical to: map[string]*User
```

Useful for shortening verbose generic types in signatures.

## Revamped `go fix` (Go 1.26)

`go fix` is now a modernizer. It scans your code for outdated idioms and applies fixes:

```bash
go fix ./...
```

Things it fixes automatically (in 1.26):

- Replaces `interface{}` with `any`.
- Converts `for i := 0; i < len(s); i++ { use(s[i]) }` to `for _, v := range s { use(v) }`.
- Replaces manual min/max with the builtins.
- Uses `slices.Contains` instead of manual loops.
- Dozens more the list grows each release.

**Run `go fix ./...` on any codebase you're modernizing.** Review the diff, run tests, ship.
It's the lowest-effort way to bring old code up to modern idioms.

You can also publish your own fixers with `//go:fix inline` directives useful for API
migrations within a large codebase.

## Experimental: `runtime/pprof` `goroutineleak` profile (Go 1.26)

```bash
GOEXPERIMENT=goroutineleakprofile go build ./...
```

Then at runtime:

```go
prof := pprof.Lookup("goroutineleak")
prof.WriteTo(os.Stdout, 2)
```

Reports goroutines that are leaked started but can never make progress and can't be reached
by any path. Also exposed via `/debug/pprof/goroutineleak` endpoint if `net/http/pprof` is
imported.

**Use this in production** to catch leaks that slipped past tests. Pair with `synctest` in
tests for complete coverage.

## Experimental: SIMD (Go 1.26)

`simd/archsimd` package. For code where you're dropping down to asm or CGo for SIMD today,
you can now stay in pure Go:

```go
// Example sketch the actual API is architecture-gated
import "simd/archsimd"

// Use wide vector types to process 16+ elements per instruction
```

Realistic use: image/video codecs, cryptography, numerical kernels. For regular application
code, stick with the compiler's existing auto-vectorization.

## Experimental: `runtime/secret` (Go 1.26)

Securely erase temporary values holding secrets (keys, passwords). Zeroing a `[]byte` via
`for i := range b { b[i] = 0 }` can be optimized away by the compiler; `runtime/secret`
guarantees the zeroing.

```go
import "runtime/secret"

buf := make([]byte, 32)
defer secret.Erase(buf)  // guaranteed zero-ing at end of scope
// use buf for cryptographic material
```

Audience: cryptography libraries and applications that process PII and must not leak it into
heap dumps. Normal application code doesn't need this.

## New crypto packages (Go 1.26)

- **`crypto/hpke`** Hybrid Public Key Encryption (RFC 9180). Used in TLS 1.3 Encrypted
  Client Hello and MLS.
- **`crypto/mlkem`** Post-quantum key encapsulation (Kyber in its standardized form). Start
  considering hybrid schemes for long-lived secrets now.
- **`testing/cryptotest`** helpers for testing crypto code deterministically. Previously
  tests relied on overriding `rand.Reader`; now the idiomatic path is built in.

Also in 1.26: `crypto/rand` and similar no longer honor overridden `rand.Reader` by default —
they always use a secure source. Set `GODEBUG=cryptocustomrand=1` to restore old behavior for
tests (or use `testing/cryptotest`).

## Migration checklist

When modernizing an existing Go codebase:

1. **Bump `go.mod`** to the latest version your team supports. This enables the new semantics.
2. **Run `go fix ./...`** in 1.26+. Review and commit.
3. **Run `golangci-lint run` with recent lints enabled** `copyloopvar`, `intrange`,
   `modernize`, `loopclosure` will surface remaining old idioms.
4. **Replace `sync.Once` + package var with `sync.OnceValue`** where applicable.
5. **Replace custom logging with `slog`.** If you can't do it all at once, do it per-package.
6. **Rewrite flaky concurrency tests with `testing/synctest`.** Biggest quality-of-life win.
7. **Update deprecated APIs.** Check `deprecated` godoc comments in your current stdlib calls.

After migration, run the full test suite with `-race` to confirm nothing broke. Old code that
was working around now-fixed bugs occasionally breaks in subtle ways.
