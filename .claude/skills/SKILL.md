---
name: golang-dev
description: >
  Expert Go (Golang) development skill for coding agents, targeting Go 1.26+. Deeply focused on
  CSP concurrency (channels, goroutines, select, pipelines, fan-in/fan-out, worker pools, bounded
  parallelism, context cancellation) and the gotchas that trip up agents — goroutine leaks, loop
  variable capture, unbuffered-channel deadlocks, `select` default misuse, `sync.Pool` traps, race
  conditions. Also enforces idiomatic Go per Effective Go and the "10x" commandments: useful zero
  values, wrapped errors (`%w`), sentinel errors, directional channels, structured concurrency
  (`errgroup`, `sync.WaitGroup`), table-driven tests, race detector, `testing/synctest` for
  deterministic concurrency tests, `slog` for structured logging, `go vet` + `golangci-lint` +
  `gosec` as mandatory gates.
  USE THIS SKILL whenever the user asks to write, review, refactor, test, debug, or design ANY Go
  code — even casually: "add a worker that...", "why does this goroutine hang?", "parallelize
  this...", "speed up this loop", ".go file", "channel", "goroutine", "golang", "concurrent",
  "pipeline", "fan-out", "context cancel", "race condition", "deadlock", "go test", "go mod",
  "errgroup", "sync.WaitGroup". Trigger it even when the user says "just a quick Go snippet".
---

# Golang Expert Skill

## What this skill is

This skill turns you into a senior Go engineer with deep CSP instincts. It condenses Rob Pike's
2012 Go Concurrency Patterns, Sameer Ajmani's Pipelines and cancellation, Effective Go, the
"10x" commandments of highly effective Go, Matt Holiday's Go class series on gotchas and
channels-in-detail, and Go 1.22–1.26 modernizations into a decision framework you can apply
turn-by-turn.

**Target: Go 1.26+.** That means loop variables are per-iteration (Go 1.22+), `sync.WaitGroup.Go`
exists (Go 1.25+), `testing/synctest` is stable (Go 1.25+), and the modernizing `go fix` is in
(Go 1.26). Write code that assumes these — don't regress to older idioms.

## The three laws (non-negotiable)

1. **Share memory by communicating** — Pike's mantra. When two goroutines need to agree on
   something, the first design to reach for is a channel, not a mutex. Mutexes are legitimate
   for small problems (reference counts, caches) — see `references/sync-primitives.md` — but
   channel-based designs compose better and race less.
2. **The zero value is useful** — `sync.Mutex{}`, `bytes.Buffer{}`, `strings.Builder{}`, and
   `slog.Logger{}` work out of the box. Design your own types so `var x T` is immediately valid.
   Only write a constructor (`NewX`) when the zero value can't work (e.g. needs a non-nil map,
   a socket, or validation).
3. **Errors are values, not signals** — return them, wrap them with `%w`, match them with
   `errors.Is`/`errors.As`, never compare with `==` or string-match. `panic` is for *impossible*
   states; never for expected failure. See `references/errors.md`.

## Concurrency: the CSP mindset (this is the skill's heart)

### When NOT to use concurrency — read this first

Goroutines look cheap, so the temptation is to sprinkle `go` everywhere. Resist. **Start with a
sequential solution and measure.** Add concurrency only when you have a concrete reason:
(a) the work is I/O-bound and you can overlap waits, (b) the work is CPU-bound and divisible
across cores, or (c) the *structure* of the problem is inherently concurrent (a server handling
independent requests, a UI event loop, etc.).

For a script that processes 1,000 items in 5 seconds sequentially, concurrency usually adds bugs
without adding speed. A goroutine is lightweight, but *reasoning about* a concurrent program is
not.

### The four rules every goroutine must obey

Every `go func(){...}()` you write must satisfy all four — if any is unclear, refactor:

1. **Clear owner.** Someone (usually the caller that spawned it) is responsible for its
   lifetime. "Just run in the background forever" is a leak waiting to happen.
2. **Clear termination path.** It must exit on a `done`/`ctx.Done()` signal, when its input
   channel closes, or when its work list is empty. "Runs until the program dies" is fine for
   `main`-owned workers, but must be explicit.
3. **Bounded count.** Never spawn one goroutine per item from an unbounded source. Use a worker
   pool or `errgroup.SetLimit`. A queue of 10 million items + `go process(item)` is how
   production outages happen.
4. **Errors reach a caller.** A goroutine that swallows errors silently is worse than no
   goroutine. Use `errgroup.Group`, or a dedicated error channel, or `sync.WaitGroup` + error
   slice.

### Channel idioms — memorize these

Directional channels in function signatures document intent and prevent deadlocks:

```go
func produce(out chan<- Event)   // send-only; callers know this goroutine only writes
func consume(in  <-chan Event)   // receive-only; callers know this goroutine only reads
```

**Closing responsibility:** the sender closes, and only when *only one* goroutine sends. If many
goroutines send to one channel, wrap them in a `sync.WaitGroup` and have a *separate* goroutine
`wg.Wait(); close(ch)`. Sending on a closed channel panics; receiving on one yields the zero
value immediately.

**The `done` channel as broadcast:** `close(done)` is a broadcast to every goroutine `select`-ing
on `<-done`. This is the cheapest, most idiomatic cancellation signal. `context.Context` is
`done` + metadata; use `ctx` for anything that crosses an API boundary.

**Buffered vs. unbuffered:** unbuffered = handshake (rendezvous between sender and receiver).
Buffered = mailbox (sender doesn't wait unless buffer is full). **Default to unbuffered** — it
surfaces design problems loudly via deadlock; buffered channels can hide them.

### The canonical patterns

For the full catalog with runnable code, read `references/concurrency-patterns.md`. The most
important patterns and when to pick each:

| Pattern             | When to reach for it                                                 |
|---------------------|----------------------------------------------------------------------|
| Generator           | Produce a stream lazily; function returns `<-chan T`                 |
| Fan-out             | Parallelize CPU or I/O across N workers reading from one channel     |
| Fan-in (merge)      | Combine N streams into one; use `select` or a goroutine per input    |
| Pipeline            | Multi-stage streaming transform; each stage is a `<-chan T` → `<-chan U` |
| Worker pool         | Bounded parallelism over a known pool size                           |
| Bounded parallelism | Same, but explicitly rate-limiting memory/FDs/connections            |
| Timeout             | `select` with `<-time.After(d)` or `ctx, _ := WithTimeout(...)`      |
| Quit / cancellation | `close(done)` or `cancel()` to tell everything downstream to stop    |
| First-responder     | N concurrent attempts, take the first; others are abandoned          |

A template pipeline stage (copy this, don't rewrite):

```go
// stage is a pipeline stage: read from in, transform, write to out.
// It closes out when in is drained or ctx is canceled.
func stage(ctx context.Context, in <-chan Input) <-chan Output {
    out := make(chan Output)
    go func() {
        defer close(out)
        for v := range in {
            result, err := transform(v)
            if err != nil { /* handle or forward via error channel */ continue }
            select {
            case out <- result:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}
```

See `assets/templates/pipeline.go`, `assets/templates/worker-pool.go`, and
`assets/templates/errgroup.go` for drop-in starting points.

### Pipeline construction — the two rules

From Sameer Ajmani's Pipelines and cancellation:

1. **Stages close their outbound channels when all sends are done.** This is how downstream
   consumers know to exit their `range` loop.
2. **Stages keep receiving from inbound channels until those channels are closed OR a cancel
   signal arrives.** This is how you avoid leaking upstream goroutines when the downstream
   bails early.

If you forget rule 2, the classic leak happens: consumer returns early → producer blocks forever
on a send nobody will receive → goroutine and its heap references live until process exit. The
`done` channel / `ctx` + `select`-on-send pattern (shown in the template above) is the fix.

### Context — the standard cancellation vehicle

Every function that does I/O, calls a downstream service, or spawns a goroutine takes
`ctx context.Context` as its **first** argument. Never store `ctx` in a struct (one exception:
long-lived per-request objects). Check `ctx.Err()` or `<-ctx.Done()` in any loop that might run
longer than a few milliseconds. Propagate `ctx` — don't create `context.Background()` inside a
library function.

```go
func (s *Service) Fetch(ctx context.Context, id string) (Record, error) {
    ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel() // ALWAYS on the line after WithX — never skip
    // ... use ctx for HTTP, DB, downstream calls
}
```

## Gotchas that burn coding agents

These are the ones I've seen trip up AI-written Go most often. Full list with code in
`references/gotchas.md` — these are the must-knows:

1. **Goroutine leak via unreceived send.** `go func(){ ch <- v }()` where nobody reads `ch` =
   leak forever. Always pair with cancellation or buffered channel sized to guarantee delivery.
2. **`range` over channel that's never closed** = goroutine waits forever. Pair `range` with a
   guaranteed close, or switch to explicit `select` with `<-ctx.Done()`.
3. **`select` with `default` when you meant to block.** `default` converts a blocking select
   into a polling busy-loop. Only use `default` when non-blocking is explicitly desired.
4. **Capturing loop variables in pre-1.22 code.** Fixed in Go 1.22+, but if your `go.mod` says
   `go 1.21` or earlier, `for _, v := range s { go f(v) }` captures `v` by reference and every
   goroutine sees the last value. Fix: `v := v` inside the loop, or pass as parameter. Since
   we target 1.26+, this should not bite you — but you'll read a lot of older code. Verify the
   `go.mod` directive.
5. **`time.After` in a loop leaks timers** until they fire. Use `time.NewTimer` + `Stop()` or
   `context.WithTimeout` for repeating timeouts inside a long-running `select`.
6. **Unbuffered channel + single goroutine = deadlock.** `ch := make(chan int); ch <- 1` in one
   goroutine with no other goroutine to receive is an instant deadlock. Buffer it or split the
   work.
7. **Double-close panics.** Closing a channel twice panics. Closing a nil channel panics.
   Sending on a closed channel panics. Use `sync.Once` if there's any chance of double-close,
   or redesign so there's exactly one closer.
8. **`WaitGroup.Add` inside the goroutine.** `wg.Add(1)` must happen in the *spawning*
   goroutine before `go f()`; otherwise `wg.Wait()` can race past. Go 1.25's `wg.Go(f)`
   sidesteps this entirely — prefer it.
9. **Mutex copied by value** (e.g. passing a struct containing `sync.Mutex` by value) silently
   creates two independent mutexes. Always pointer-receive methods on types with a mutex.
   `go vet` catches this — run it.
10. **Forgetting `cancel()` on `context.WithCancel/WithTimeout/WithDeadline`.** The linter
    `govet`'s `lostcancel` catches this. Always `defer cancel()` on the next line.
11. **`http.DefaultClient` / `http.DefaultTransport` without timeout.** In production, these
    can hang forever. Make your own `http.Client{Timeout: ...}`.
12. **Sending on a channel owned by another goroutine.** The owner closes. If non-owners need
    to signal "please stop accepting", use a separate `done` channel flowing the other way.
13. **`sync.Pool.Get()` without `Reset()`.** `sync.Pool` reuses objects across goroutines; any
    stale state contaminates the next user. Reset immediately after `Get`. Also, `sync.Pool` is
    cleared on GC; it's a hint, not a cache.
14. **Struct field-order padding.** `struct{ b bool; n int64; f bool }` wastes ~14 bytes per
    instance on 64-bit. Run `fieldalignment -fix` for bulk-allocated types. Matters at scale.
15. **Interface boxing on hot paths.** Assigning a large struct to `any` or to an interface
    forces a heap copy. Use pointer receivers for interface-satisfying types on hot paths;
    diagnose with `go build -gcflags="-m"`.
16. **`any`/`interface{}` in domain types.** Go 1.18+ has generics. Use `[T any]` constraints
    instead of untyped `any` — the compiler will catch more bugs.
17. **Ignoring errors with `_`** outside of truly intentional cases (and always comment why).
    `errcheck` / `gosec G104` catch these.

## Testing

Three non-negotiable habits:

**Table-driven tests.** One test function per unit under test; cases as a slice of structs with
`name`, inputs, expected outputs, and expected error. Run each case in a `t.Run(tc.name, ...)`
sub-test with `t.Parallel()`. This is so standard that deviating from it is a signal you should
reconsider. Template in `assets/templates/table_test.go`.

**`go test -race` on every test run.** Every CI pipeline should run `go test -race ./...`. The
race detector is the single highest-leverage tool Go ships. Write code that passes it.

**`testing/synctest` for concurrency tests (Go 1.25+, stable).** Deterministic goroutine
scheduling and fake time inside a "bubble" — your `time.Sleep(1 * time.Hour)` runs
instantaneously and predictably. This replaces flaky, timing-dependent concurrency tests with
fast, deterministic ones. Pattern:

```go
func TestDebouncer(t *testing.T) {
    synctest.Test(t, func(tb *testing.T) {
        d := NewDebouncer(100 * time.Millisecond)
        d.Trigger()
        time.Sleep(50 * time.Millisecond); d.Trigger() // resets window
        time.Sleep(150 * time.Millisecond)             // fires
        synctest.Wait(tb)                              // bubble quiesces
        if got := d.Fires(); got != 1 {
            tb.Errorf("Fires = %d, want 1", got)
        }
    })
}
```

For deeper testing strategy (mocks, fakes, integration tests, fuzzing), read
`references/testing.md`.

## Errors

Summary (full treatment in `references/errors.md`):

- Wrap with context every time an error crosses a layer boundary: `fmt.Errorf("fetchUser %d: %w", id, err)`.
- Define sentinel errors in the package that owns the concept: `var ErrNotFound = errors.New("user: not found")`.
- Match with `errors.Is(err, ErrNotFound)` and `errors.As(err, &pathErr)`.
- For richer errors, make a struct that implements `Error()` and `Unwrap()`.
- `panic` only for truly-impossible states (invariant violations). Never for user input, I/O
  failures, or missing resources — those are expected errors.
- `recover` has a legitimate role at goroutine boundaries in long-running servers (so one
  crashing handler doesn't take the process down). Don't use `recover` as general error handling.

## Idiomatic Go in one page

Drawn from Effective Go + the 10x commandments:

- **Write packages, not programs.** `main` parses flags and calls a library package that does
  the real work. This makes your code testable and reusable.
- **Write code for reading.** Use conventional short names: `ctx`, `err`, `buf`, `req`, `resp`,
  `f` for `*os.File`, `i` for index. Flatten cognitive speed bumps.
- **Receiver naming:** use the same 1–2 letter name for every method on a type (e.g. `func (s *Server)...`
  everywhere). Never `this` or `self`.
- **Interfaces:** small (one or two methods), defined by the *consumer* not the producer.
  `io.Reader`, `io.Writer`, `fmt.Stringer` are the templates. Don't export interfaces just
  because you can.
- **Constructors:** `NewT` returns `*T` or `T`. Prefer returning the concrete type, not an
  interface, unless the package genuinely needs polymorphism for its clients.
- **Avoid `init()`** outside tests. It runs at import time and is a hidden dependency.
- **Avoid mutable package-level state.** It races, it hides dependencies, and it makes testing
  miserable. If you think you need it, pass it as a parameter or wrap it in a struct.
- **Logging:** `log/slog` with JSON handler in production. Never `fmt.Println` in library code.
  Log actionable errors only; use metrics for counts and tracing for flows.
- **No magic numbers or strings:** `const` blocks, `iota`, or typed constants.
- **Use `go:embed`** for static assets instead of reading files at runtime.

## Quality gates — run before calling a task done

```bash
gofmt -s -w ./...                        # formatting (MUST be clean)
go vet ./...                             # free static analysis
go test -race -cover ./...               # race detector + coverage
golangci-lint run                        # strict linter (config in assets/.golangci.yml)
gosec ./...                              # security scan
```

Thresholds:

- `go vet`, `golangci-lint`, `gosec`: zero findings.
- Test coverage: aim for ≥ 80% on application code, ≥ 95% on pure-logic packages.
- Cyclomatic complexity: ≤ 10 per function (`gocyclo` via golangci-lint).
- Race detector: zero races. Ever.

## Agentic workflow for Go tasks

When the user hands you a Go task, follow this loop — don't skip steps:

1. **Clarify or commit.** Either ask one targeted question (only if the task is genuinely
   ambiguous), or commit to an interpretation and state it explicitly before coding. Mohsan's
   preference: commit + state assumption inline.
2. **Sequential first.** Sketch or write the non-concurrent version first. If the user hasn't
   asked for concurrency, often you're done.
3. **Tests before implementation** for anything non-trivial. Table-driven. At minimum a happy
   path and one error path.
4. **If concurrency is needed, pick the smallest pattern that solves it.** Consult the patterns
   table above. Don't reach for a pipeline when a single `errgroup` does it.
5. **Every goroutine must satisfy the four rules.** Re-read them before writing `go`.
6. **Run the quality gates.** Don't claim "done" without `go vet`, `go test -race`, and at
   least formatting cleanup.
7. **Review for gotchas.** Scan your diff against the gotchas list above; the top 5 (leaks,
   loop-var, select-default, forgotten-cancel, unbuffered-deadlock) are most common.

## Reference files

| File                                 | Read when...                                                                         |
|--------------------------------------|--------------------------------------------------------------------------------------|
| `references/concurrency-patterns.md` | Writing any concurrent code; contains full code for the 18 canonical patterns        |
| `references/gotchas.md`              | Debugging hangs, races, leaks; before shipping concurrent code                       |
| `references/errors.md`               | Designing error types, error boundaries, recover usage                               |
| `references/testing.md`              | Writing tests, using synctest, mocks, benchmarks, fuzzing                            |
| `references/idiomatic-go.md`         | Style questions: naming, packaging, interfaces, embedding                            |
| `references/sync-primitives.md`      | When mutex is better than channels; `sync.Once`, `sync.Map`, `atomic`                |
| `references/go-1.26-features.md`     | Using features new in Go 1.22–1.26: `wg.Go`, `synctest`, `go fix`, `range-over-func` |
| `references/performance.md`          | `sync.Pool`, escape analysis, struct alignment, `pprof`                              |

## Asset files

| File | Purpose |
|------|---------|
| `assets/.golangci.yml`                 | Strict linter config — drop into any project |
| `assets/templates/pipeline.go`         | Copy-paste pipeline stage with cancellation |
| `assets/templates/worker-pool.go`      | Copy-paste bounded worker pool |
| `assets/templates/errgroup.go`         | Copy-paste errgroup-based parallel tasks |
| `assets/templates/table_test.go`       | Copy-paste table-driven test template |
| `assets/templates/synctest_test.go`    | Copy-paste synctest concurrency test template |
