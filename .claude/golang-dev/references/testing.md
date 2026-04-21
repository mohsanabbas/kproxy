# Testing Go Tactics for Coding Agents

Go has the best-integrated testing story of any mainstream language: `go test`, the race
detector, benchmarks, fuzzing, and (since 1.25) deterministic concurrency tests all ship with
the toolchain. This file covers every testing decision an agent faces.

## Table of contents

1. [Table-driven tests the default](#1-table-driven-tests--the-default)
2. [Subtests and parallel execution](#2-subtests-and-parallel-execution)
3. [Test naming conventions](#3-test-naming-conventions)
4. [Error assertions what to compare and how](#4-error-assertions--what-to-compare-and-how)
5. [`testing/synctest` deterministic concurrency tests](#5-testingsynctest--deterministic-concurrency-tests)
6. [Race detector always on](#6-race-detector--always-on)
7. [Mocks, fakes, and stubs](#7-mocks-fakes-and-stubs)
8. [Fuzz testing](#8-fuzz-testing)
9. [Benchmarks](#9-benchmarks)
10. [Coverage what's a good number?](#10-coverage--whats-a-good-number)
11. [Test helpers and golden files](#11-test-helpers-and-golden-files)
12. [Integration vs. unit tests](#12-integration-vs-unit-tests)

---

## 1. Table-driven tests the default

Unless you have a concrete reason not to, every test is table-driven. One test function per
logical unit, one struct per case, loop over cases calling `t.Run`.

```go
func TestParseAmount(t *testing.T) {
    t.Parallel()

    tests := []struct {
        name    string
        input   string
        want    decimal.Decimal
        wantErr error
    }{
        {"happy integer", "42", decimal.New(42, 0), nil},
        {"happy decimal", "3.14", decimal.New(314, -2), nil},
        {"error empty",   "",    decimal.Zero,       ErrEmptyAmount},
        {"error garbage", "abc", decimal.Zero,       ErrInvalidAmount},
    }

    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            t.Parallel()

            got, err := ParseAmount(tc.input)

            if !errors.Is(err, tc.wantErr) {
                t.Fatalf("err = %v, want %v", err, tc.wantErr)
            }
            if !got.Equal(tc.want) {
                t.Errorf("got %s, want %s", got, tc.want)
            }
        })
    }
}
```

**Why this structure:**

- Adding a new case is one struct literal, not a new function.
- `go test -run TestParseAmount/happy` runs just the happy paths.
- Each case has a descriptive `name` that shows up in verbose output.
- `t.Parallel()` lets the Go test framework interleave slow cases.
- When a case fails, the error message tells you *which* case no detective work.

**Reject this pattern only when:** the cases genuinely can't share a body (e.g. each needs a
radically different setup). Then write separate test functions.

## 2. Subtests and parallel execution

`t.Run(name, fn)` creates a subtest. `t.Parallel()` marks a test (or subtest) as parallel;
the framework pauses the current test, runs other parallel tests, then resumes.

**Three rules for parallel subtests:**

1. **Call `t.Parallel()` at the top of both** the outer function and the subtest. Just the
   outer isn't enough the subtests still run sequentially among themselves.
2. **Don't share mutable state across parallel cases.** Each case must own its own inputs and
   outputs. A `tests` slice of structs satisfies this naturally (each `tc` is a value copy).
3. **Pre-Go 1.22: capture the loop variable.** `tc := tc` inside the outer loop. Go 1.22+
   handles this automatically, but verify your `go.mod`.

Parallel tests expose race conditions the race detector can then catch it's a two-for-one.

## 3. Test naming conventions

- `TestFunctionName` unit test for `FunctionName`.
- `TestTypeName_MethodName` for methods.
- Subtest names are sentences, lowercase, describing the scenario: `"returns error on empty
  input"` or `"happy two digits"`. These end up in failure messages, so they should read
  well.

Go proverbs: **"A test should have one clear reason to fail."** Each subtest exercises one
behavior. If a case needs three assertions, that's fine they all describe the one behavior.
If a case exercises three independent behaviors, split it.

## 4. Error assertions what to compare and how

Never compare error strings. Use `errors.Is` for sentinel checks, `errors.As` for custom
types:

```go
if !errors.Is(err, ErrNotFound) {
    t.Fatalf("err = %v, want ErrNotFound", err)
}

var pe *PathError
if !errors.As(err, &pe) {
    t.Fatalf("err = %v, want *PathError", err)
}
if pe.Path != "expected.txt" {
    t.Errorf("Path = %s, want expected.txt", pe.Path)
}
```

For "any error" vs. "no error":

```go
if (err != nil) != tc.wantErr {
    t.Fatalf("err = %v, wantErr = %t", err, tc.wantErr)
}
```

**`t.Errorf` vs `t.Fatalf`:**
- `Errorf` logs and continues use when the test can still produce useful info afterward.
- `Fatalf` logs and stops the current test use when subsequent assertions would be
  meaningless (e.g. "got nil, can't dereference").

## 5. `testing/synctest` deterministic concurrency tests

**This is the single biggest testing improvement in recent Go.** Stable in Go 1.25.

Concurrency tests have historically been awful: `time.Sleep(100ms)` to "let the goroutine run,"
racy assertions, flaky CI. `testing/synctest` fixes this by running your test in a *bubble*
where:

- Time is virtualized. `time.Sleep(24 * time.Hour)` returns instantly.
- The scheduler is deterministic. The same test produces the same result every time.
- `synctest.Wait(tb)` blocks until all goroutines in the bubble are blocked (quiesced).

```go
import (
    "testing"
    "testing/synctest"
    "time"
)

func TestDebounce(t *testing.T) {
    synctest.Test(t, func(tb *testing.T) {
        d := NewDebouncer(100 * time.Millisecond)

        d.Trigger()
        time.Sleep(50 * time.Millisecond)
        d.Trigger()                            // resets the window
        time.Sleep(75 * time.Millisecond)
        if got := d.Fires(); got != 0 {
            tb.Errorf("early fire: %d", got)
        }
        time.Sleep(50 * time.Millisecond)      // past the 100ms since last Trigger
        synctest.Wait(tb)                      // wait for all goroutines to settle
        if got := d.Fires(); got != 1 {
            tb.Errorf("Fires = %d, want 1", got)
        }
    })
}
```

**When to use `synctest`:** any test involving timers, `context.WithTimeout`, goroutine
coordination, or concurrent scheduling. It's a massive win for both speed (tests run in
microseconds instead of seconds) and reliability (zero flakiness).

**What doesn't work in a bubble:** real I/O (files, network). The bubble is for goroutine +
time coordination logic; mock out the I/O.

**Pre-1.25:** `GOEXPERIMENT=synctest` + `synctest.Run`. Pre-1.24: use `go.uber.org/goleak`
for leak detection and accept that pure time-based tests will be flaky.

## 6. Race detector always on

```bash
go test -race ./...
```

The race detector instruments memory accesses. When two goroutines access the same memory
without synchronization and at least one is a write, it reports the race with stack traces.

**Rules:**

- Run `-race` on every CI build.
- Run it locally before committing any code that spawns goroutines.
- 2-10× slower runtime fine for tests; don't ship race-enabled binaries to production.
- Catches *observed* races only. If a race never happens during the test run, it's not
  reported. So high code coverage + parallel tests + race detector together are what give
  confidence.

**What the race detector doesn't catch:** deadlocks (use `-race` with `go test -timeout`),
goroutine leaks (use `synctest` or `go.uber.org/goleak`), logic errors (that's what tests
are for).

## 7. Mocks, fakes, and stubs

Go's interfaces make test doubles simple. **Prefer hand-written fakes over generated mocks**
for clarity an agent can write a three-line fake in seconds, and the reader doesn't have to
chase magic.

**Fake (stateful, behaves like a simplified real):**

```go
type fakeUserStore struct {
    users map[int]*User
}

func (f *fakeUserStore) Get(ctx context.Context, id int) (*User, error) {
    u, ok := f.users[id]
    if !ok { return nil, ErrNotFound }
    return u, nil
}
```

**Stub (returns canned values):**

```go
type stubClock struct{ now time.Time }
func (s stubClock) Now() time.Time { return s.now }
```

**Mock (records calls for assertion):** avoid unless you specifically need to verify *that*
a method was called, not just *what effect* it had. Behavior verification is almost always
better than interaction verification.

### Where to put test doubles

For types used in several tests in the same package, put the fake in a `_test.go` file in that
package. For cross-package doubles, a `<pkg>/mocks` or `<pkg>/testutil` package works but
keep it under `internal/` to avoid shipping test scaffolding.

### Mocking concrete types

Go doesn't let you mock a concrete struct directly. If you need to mock something, refactor
the consumer to take an interface. **Remember: define the interface at the consumer**, not the
producer:

```go
// In the consumer package:
type userFetcher interface {
    Get(ctx context.Context, id int) (*User, error)
}

func NewReport(f userFetcher) *Report { ... }
```

Now the test passes `fakeUserStore`, production passes the real `*UserClient`.

## 8. Fuzz testing

`go test -fuzz=.` runs fuzz tests generates random inputs to find edge cases.

```go
func FuzzParseAmount(f *testing.F) {
    // Seed corpus
    f.Add("42")
    f.Add("3.14")
    f.Add("")

    f.Fuzz(func(t *testing.T, input string) {
        got, err := ParseAmount(input)
        if err == nil && got.IsZero() && input != "0" {
            t.Errorf("parsed %q as zero without error", input)
        }
        // Invariant: re-parsing the stringified result must match
        if err == nil {
            got2, err2 := ParseAmount(got.String())
            if err2 != nil || !got.Equal(got2) {
                t.Errorf("round-trip failed for %q: %v -> %v", input, got, got2)
            }
        }
    })
}
```

**When fuzz tests pay off:** parsers, encoders/decoders, anything that takes bytes from the
outside world. Run in CI with a time budget (`-fuzztime=30s`).

**The fuzz corpus lives in `testdata/fuzz/FuzzName/`.** Commit it each discovered bug is a
regression test.

## 9. Benchmarks

```go
func BenchmarkParse(b *testing.B) {
    input := "3.14159265358979"
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, _ = ParseAmount(input)
    }
}
```

Run with `go test -bench=. -benchmem -run=^$`. The `-run=^$` skips normal tests; `-benchmem`
shows allocations per op.

**Sub-benchmarks** for comparing strategies:

```go
func BenchmarkMerge(b *testing.B) {
    for _, size := range []int{10, 100, 1000, 10000} {
        b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
            data := makeData(size)
            b.ResetTimer()
            for i := 0; i < b.N; i++ {
                _ = merge(data)
            }
        })
    }
}
```

**Use `benchstat`** for statistical comparison of before/after:

```bash
go test -bench=. -count=10 > old.txt
# ... make changes ...
go test -bench=. -count=10 > new.txt
benchstat old.txt new.txt
```

Look for "p<0.05" rows those are statistically significant.

## 10. Coverage what's a good number?

```bash
go test -cover ./...
go test -coverprofile=cover.out ./...
go tool cover -html=cover.out    # visualize in browser
```

**Thresholds:**

- **Pure logic packages** (parsers, algorithms, business rules): aim for 90–100%. Anything
  uncovered should have a reason.
- **Infrastructure packages** (HTTP handlers, DB adapters): 70–85% is realistic. Some code
  paths only trigger on cascading failures that are hard to mock.
- **`main` and wiring code**: 30–50% is fine. Most of this is configuration glue tested via
  integration tests.

**What coverage doesn't tell you:** whether the tests assert meaningful things. 100% coverage
with only "does not panic" assertions is worse than 70% with real behavior checks. Agents
should write assertions that would fail if the implementation became wrong, not ones that
only fail on crashes.

## 11. Test helpers and golden files

**Test helpers** should call `t.Helper()` so failure messages point to the caller, not the
helper:

```go
func mustParse(t *testing.T, s string) time.Time {
    t.Helper()
    tm, err := time.Parse(time.RFC3339, s)
    if err != nil { t.Fatalf("parse %q: %v", s, err) }
    return tm
}
```

**Golden files** are committed expected outputs, typically for tests that produce structured
text (JSON, HTML, rendered reports). Update with a `-update` flag:

```go
var update = flag.Bool("update", false, "update golden files")

func TestRender(t *testing.T) {
    got := render(input)
    goldenPath := filepath.Join("testdata", "render.golden")
    if *update {
        os.WriteFile(goldenPath, got, 0644)
        return
    }
    want, err := os.ReadFile(goldenPath)
    if err != nil { t.Fatal(err) }
    if !bytes.Equal(got, want) {
        t.Errorf("mismatch:\n%s", diff(got, want))
    }
}
```

Run `go test -update` to regenerate. Review the diff carefully before committing updated
goldens.

## 12. Integration vs. unit tests

**Unit tests:** in the same package as the code, suffix `_test.go`, no external dependencies.
Run on every save, in CI, always.

**Integration tests:** typically in a separate `_integration_test.go` file, guarded by a build
tag:

```go
//go:build integration

package userdb_test

func TestAgainstRealPostgres(t *testing.T) { ... }
```

Run with `go test -tags=integration ./...`. Skip by default in local dev and unit-test CI;
run in a separate CI stage with real dependencies.

**The test pyramid in Go:** lots of unit tests, some integration tests, a few end-to-end
tests. In Go, this is easy because the language rewards small, testable functions you rarely
need complex fixtures.

### `testmain` for setup/teardown

```go
func TestMain(m *testing.M) {
    // setup: start test container, seed DB, etc.
    db := startTestDB()
    defer db.Shutdown()

    code := m.Run()
    os.Exit(code)
}
```

One `TestMain` per package. Use for expensive setup shared by all tests in the package.

---

## Recipe: testing a concurrent pipeline

Combine everything:

```go
func TestPipeline(t *testing.T) {
    synctest.Test(t, func(tb *testing.T) {
        ctx, cancel := context.WithCancel(context.Background())
        defer cancel()

        in := make(chan int)
        out := pipeline(ctx, in)

        go func() {
            defer close(in)
            for i := 0; i < 5; i++ { in <- i }
        }()

        var got []int
        for v := range out {
            got = append(got, v)
        }
        synctest.Wait(tb)

        want := []int{0, 1, 4, 9, 16}
        if !slices.Equal(got, want) {
            tb.Errorf("got %v, want %v", got, want)
        }
    })
}
```

Run with `go test -race`. If the pipeline has a race, detector catches it. If it has a leak,
`synctest.Wait` hangs until the test times out (which is a loud, useful failure).
