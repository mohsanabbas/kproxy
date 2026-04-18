# Go Concurrency Patterns — Complete Catalog

This is the deep reference for Go concurrency, synthesized from Rob Pike's 2012 Google I/O talk,
Sameer Ajmani's Pipelines and cancellation blog post, and the lotusirous/go-concurrency-patterns
repository. Every pattern here is runnable — copy, adapt, ship.

## Table of contents

1. [Goroutines: the mental model](#1-goroutines-the-mental-model)
2. [Channels: the full rulebook](#2-channels-the-full-rulebook)
3. [Pattern: Generator](#3-pattern-generator)
4. [Pattern: Fan-in (merge)](#4-pattern-fan-in-merge)
5. [Pattern: Fan-out](#5-pattern-fan-out)
6. [Pattern: Pipeline](#6-pattern-pipeline)
7. [Pattern: Bounded parallelism](#7-pattern-bounded-parallelism)
8. [Pattern: Worker pool](#8-pattern-worker-pool)
9. [Pattern: Timeout](#9-pattern-timeout)
10. [Pattern: Cancellation via `done`/`context`](#10-pattern-cancellation-via-donecontext)
11. [Pattern: First-responder (replicated request)](#11-pattern-first-responder-replicated-request)
12. [Pattern: Rate limiting](#12-pattern-rate-limiting)
13. [Pattern: Subscription / publish-subscribe](#13-pattern-subscription--publish-subscribe)
14. [Pattern: Tee (broadcast to multiple consumers)](#14-pattern-tee-broadcast-to-multiple-consumers)
15. [Pattern: Semaphore via buffered channel](#15-pattern-semaphore-via-buffered-channel)
16. [Pattern: Ring buffer](#16-pattern-ring-buffer)
17. [`errgroup` — structured concurrency for the common case](#17-errgroup--structured-concurrency-for-the-common-case)
18. [Choosing a pattern — decision tree](#18-choosing-a-pattern--decision-tree)

---

## 1. Goroutines: the mental model

A goroutine is an independently executing function, scheduled cooperatively by the Go runtime
onto a smaller pool of OS threads (controlled by `GOMAXPROCS`). Key properties that change how
you design:

- **Cheap** — ~2 KB initial stack, grows as needed. Having 100,000 is normal; having 10 million
  is not — goroutines have scheduler and memory overhead that adds up.
- **Multiplexed** — the runtime moves blocked goroutines off their OS thread so others can run.
  This is why I/O-bound Go programs scale so well without explicit async/await.
- **Not garbage-collected individually** — a goroutine that blocks forever leaks until process
  exit, along with every heap object its stack references.
- **No inherent identity** — there's no goroutine ID you can rely on. Don't build systems that
  need per-goroutine state outside of goroutine-local arguments and return values.

> "Concurrency is the composition of independently executing computations. It is a way to
> structure software, particularly as a way to write clean code that interacts well with the
> real world." — Rob Pike, 2012.

### The go statement has no error handling

```go
go doWork()  // if doWork panics, the whole process dies unless doWork recovers internally
```

For a pattern to recover from panics in worker goroutines without crashing the process, see
`references/errors.md` section on `recover`.

## 2. Channels: the full rulebook

### Creation

```go
unbuffered := make(chan int)        // rendezvous (handshake)
buffered   := make(chan int, 10)    // mailbox with capacity 10
directional := make(chan<- int)     // rarely useful; usually you narrow via function signature
```

### Operations and what blocks

| Operation           | Unbuffered                                | Buffered (cap N, len L)                 |
|---------------------|-------------------------------------------|-----------------------------------------|
| `ch <- v` (send)    | Blocks until a receive is ready           | Blocks only if `L == N`                 |
| `<-ch` (receive)    | Blocks until a send is ready              | Blocks only if `L == 0`                 |
| `close(ch)`         | Never blocks. Panics if already closed.   | Never blocks. Panics if already closed. |
| Send on closed ch   | **Panics**                                | **Panics**                              |
| Recv on closed ch   | Returns zero value, `ok=false`            | Drains buffered values first, then zero |
| `ch == nil` send/recv | **Blocks forever** (useful in `select`!)| Same                                    |

### Directional channels — use them in signatures

```go
func produce(out chan<- Event) { ... }   // this function can only send
func consume(in  <-chan Event) { ... }   // this function can only receive
```

The compiler enforces the direction, which doubles as documentation and prevents a class of
deadlocks where a function accidentally both sends and receives on the same channel.

### Closing — who and when

**Only the sender closes, and only when no more values will be sent.**

- **Exactly one sender:** that sender closes after its last send.
- **Multiple senders, one channel:** wrap in a `sync.WaitGroup`; a dedicated goroutine waits
  for all senders, then closes.

```go
var wg sync.WaitGroup
out := make(chan T)
for _, w := range workers {
    wg.Add(1)
    go func(w Worker) {
        defer wg.Done()
        for v := range w.Produce() {
            out <- v
        }
    }(w)
}
go func() { wg.Wait(); close(out) }()  // sole closer
```

Alternatively in Go 1.25+:

```go
var wg sync.WaitGroup
out := make(chan T)
for _, w := range workers {
    wg.Go(func() {
        for v := range w.Produce() { out <- v }
    })
}
go func() { wg.Wait(); close(out) }()
```

### The closed-channel broadcast trick

```go
done := make(chan struct{})
// ... many goroutines doing: select { case ...: case <-done: return }
close(done)  // every goroutine waiting on <-done wakes up with the zero value, immediately
```

This is how `context.Context.Done()` works under the hood. For anything that crosses an API
boundary, use `context.Context` instead of a raw `done` channel — it carries deadlines and
values too.

### `select` — the concurrency `switch`

```go
select {
case v := <-ch1:          // receive case
case ch2 <- x:            // send case
case <-time.After(d):     // timeout case
case <-ctx.Done():        // cancellation case
default:                  // only add if NON-BLOCKING is desired
}
```

Semantics:

- All cases are evaluated. A case is "ready" if its channel op wouldn't block.
- If multiple are ready, **one is chosen pseudo-randomly**. Don't rely on order.
- If none are ready and there's a `default`, `default` runs.
- If none are ready and there's no `default`, `select` blocks until one is ready.

A **nil channel case blocks forever** — this lets you dynamically disable a case:

```go
var in <-chan T = source  // start with real channel
for {
    select {
    case v, ok := <-in:
        if !ok { in = nil; continue }  // disable this case after channel closes
        process(v)
    case <-ctx.Done():
        return
    }
    if in == nil { return }  // all inputs exhausted
}
```

## 3. Pattern: Generator

A function that returns a receive-only channel of values produced by an internal goroutine.
Good for lazy sequences, async iteration, or exposing a stream as a first-class value.

```go
func fibonacci() <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        a, b := 0, 1
        for {
            out <- a
            a, b = b, a+b
        }
    }()
    return out
}

// Usage:
fib := fibonacci()
for i := 0; i < 10; i++ {
    fmt.Println(<-fib)
}
```

**Leak warning:** the goroutine above runs forever. For a bounded consumer, add a `done` or
`ctx`:

```go
func fibonacci(ctx context.Context) <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        a, b := 0, 1
        for {
            select {
            case out <- a:
                a, b = b, a+b
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}
```

Go 1.23+ has `range-over-func` iterators (`iter.Seq[T]`) which are cheaper and don't require a
goroutine — prefer iterators for pure pull-based sequences without I/O.

## 4. Pattern: Fan-in (merge)

Combine multiple input streams into one output stream.

**Two-goroutine approach (one per input):**

```go
func merge[T any](cs ...<-chan T) <-chan T {
    out := make(chan T)
    var wg sync.WaitGroup
    wg.Add(len(cs))
    for _, c := range cs {
        go func(c <-chan T) {
            defer wg.Done()
            for v := range c {
                out <- v
            }
        }(c)
    }
    go func() { wg.Wait(); close(out) }()
    return out
}
```

**Select-based fan-in (no extra goroutines, but only handles a fixed number of inputs):**

```go
func fanIn2[T any](a, b <-chan T) <-chan T {
    out := make(chan T)
    go func() {
        defer close(out)
        for a != nil || b != nil {
            select {
            case v, ok := <-a:
                if !ok { a = nil; continue }
                out <- v
            case v, ok := <-b:
                if !ok { b = nil; continue }
                out <- v
            }
        }
    }()
    return out
}
```

**With cancellation:** every send to `out` should be in a `select` alongside `<-ctx.Done()` —
otherwise `merge` leaks when the consumer gives up.

## 5. Pattern: Fan-out

One producer, N consumers reading from the same channel. Because channel receives are
synchronized, N goroutines can safely `range` over one channel and each value goes to exactly
one of them.

```go
func fanOut[In, Out any](in <-chan In, n int, work func(In) Out) []<-chan Out {
    outs := make([]<-chan Out, n)
    for i := 0; i < n; i++ {
        ch := make(chan Out)
        outs[i] = ch
        go func() {
            defer close(ch)
            for v := range in {
                ch <- work(v)
            }
        }()
    }
    return outs
}
```

Typical combo: **fan-out + fan-in**. Spawn N workers to parallelize a stage, then merge their
outputs back into a single stream for the next stage.

## 6. Pattern: Pipeline

A series of stages connected by channels. From Sameer Ajmani's canonical post:

```go
func gen(ctx context.Context, nums ...int) <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        for _, n := range nums {
            select {
            case out <- n:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}

func sq(ctx context.Context, in <-chan int) <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        for n := range in {
            select {
            case out <- n * n:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}

// Usage:
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
for v := range sq(ctx, sq(ctx, gen(ctx, 2, 3))) {
    fmt.Println(v)  // 16, 81
}
```

**The two pipeline rules, restated:**

1. Every stage **closes its outbound channel when done sending** — downstream `range` loops
   exit cleanly.
2. Every stage **keeps receiving until the inbound channel is closed OR ctx is canceled** —
   otherwise upstream goroutines block forever on their sends.

The `select { case out <- v: case <-ctx.Done(): return }` pattern on every send is what makes
rule #2 work even when the consumer bails early.

## 7. Pattern: Bounded parallelism

When fan-out to an *unbounded* number of goroutines is wrong (e.g. you have 10 million files
to hash and can't spawn 10M goroutines), fix the worker count. This is the MD5All example from
the Go blog:

```go
func MD5All(ctx context.Context, root string) (map[string][md5.Size]byte, error) {
    // Stage 1: walk the tree, emit file paths
    paths, errc := walkFiles(ctx, root)

    // Stage 2: fixed-size pool of digesters
    const numDigesters = 20
    c := make(chan result)
    var wg sync.WaitGroup
    wg.Add(numDigesters)
    for i := 0; i < numDigesters; i++ {
        go func() {
            defer wg.Done()
            digester(ctx, paths, c)
        }()
    }
    go func() { wg.Wait(); close(c) }()

    // Stage 3: collect results
    m := make(map[string][md5.Size]byte)
    for r := range c {
        if r.err != nil { return nil, r.err }
        m[r.path] = r.sum
    }
    if err := <-errc; err != nil { return nil, err }
    return m, nil
}
```

Choose pool size by the bottleneck:

- CPU-bound: `runtime.GOMAXPROCS(0)` or slightly more.
- Disk I/O: typically 4–32; past that, disk seek overhead wins.
- Network I/O: 50–200 is common; higher risks exhausting FDs and overwhelming the server.
- Always benchmark; don't guess.

## 8. Pattern: Worker pool

Closely related to bounded parallelism, but typically the pool is long-lived and the task
stream is open-ended (e.g. a server processing incoming jobs). The worker function keeps
reading jobs until its input channel closes.

```go
type Job struct { ID int; Payload []byte }
type Result struct { ID int; Data []byte; Err error }

func worker(ctx context.Context, jobs <-chan Job, results chan<- Result) {
    for j := range jobs {
        select {
        case <-ctx.Done():
            return
        default:
        }
        out, err := process(ctx, j)
        select {
        case results <- Result{ID: j.ID, Data: out, Err: err}:
        case <-ctx.Done():
            return
        }
    }
}

func Pool(ctx context.Context, workers int, jobs <-chan Job) <-chan Result {
    results := make(chan Result)
    var wg sync.WaitGroup
    for i := 0; i < workers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            worker(ctx, jobs, results)
        }()
    }
    go func() { wg.Wait(); close(results) }()
    return results
}
```

## 9. Pattern: Timeout

**Per-operation timeout:**

```go
select {
case v := <-ch:
    handle(v)
case <-time.After(100 * time.Millisecond):
    return ErrTimeout
}
```

**Pitfall:** `time.After` in a tight loop leaks timers until they fire (up to the duration).
For timeouts inside a long-running `select` loop, use a `*time.Timer` and `Reset` it, or use
`context.WithTimeout`.

**Whole-conversation timeout:**

```go
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
for {
    select {
    case v := <-ch:
        handle(v)
    case <-ctx.Done():
        return ctx.Err()  // context.DeadlineExceeded
    }
}
```

## 10. Pattern: Cancellation via `done`/`context`

The `done` channel as broadcast signal:

```go
func Produce(done <-chan struct{}) <-chan int {
    out := make(chan int)
    go func() {
        defer close(out)
        for i := 0; ; i++ {
            select {
            case out <- i:
            case <-done:
                return
            }
        }
    }()
    return out
}

func main() {
    done := make(chan struct{})
    defer close(done)  // broadcast cancellation on any return
    for v := range Produce(done) {
        if v >= 10 { return }  // done is closed; Produce cleans up
        fmt.Println(v)
    }
}
```

In production code, prefer `context.Context` — it carries deadlines and is the standard
vehicle for request-scoped values. Use raw `done` only inside a package where `ctx` would be
noise, or in concurrency primitives where no metadata is needed.

## 11. Pattern: First-responder (replicated request)

Ask N replicas in parallel, take the first answer; others are wasted but latency wins. Classic
for tail-latency reduction.

```go
func First[T any](ctx context.Context, replicas ...func(context.Context) (T, error)) (T, error) {
    c := make(chan T, len(replicas))    // buffer sized so late senders don't block
    errc := make(chan error, len(replicas))
    for _, r := range replicas {
        go func(r func(context.Context) (T, error)) {
            v, err := r(ctx)
            if err != nil { errc <- err; return }
            c <- v
        }(r)
    }
    select {
    case v := <-c: return v, nil
    case <-ctx.Done():
        var zero T
        return zero, ctx.Err()
    }
}
```

**Buffer size matters here.** If `c` is unbuffered, late replicas block their goroutines
forever — a classic leak. Size the buffer to `len(replicas)` so any replica that finishes can
deposit its result and exit cleanly.

## 12. Pattern: Rate limiting

**Token bucket via ticker:**

```go
rate := time.Tick(10 * time.Millisecond)   // 100 req/sec
for req := range requests {
    <-rate            // wait for token
    go handle(req)
}
```

**Semaphore-based concurrency cap:**

```go
sem := make(chan struct{}, 50)  // max 50 in flight
for req := range requests {
    sem <- struct{}{}           // acquire
    go func(req Request) {
        defer func() { <-sem }()  // release
        handle(req)
    }(req)
}
```

For production use `golang.org/x/time/rate` (leaky bucket with burst support).

## 13. Pattern: Subscription / publish-subscribe

Classic from Pike's "Advanced Go Concurrency Patterns":

```go
type Sub interface {
    Updates() <-chan Item
    Close() error
}

type sub struct {
    fetcher Fetcher
    updates chan Item
    closing chan chan error
}

func (s *sub) Updates() <-chan Item { return s.updates }

func (s *sub) Close() error {
    errc := make(chan error)
    s.closing <- errc
    return <-errc
}

// The loop goroutine owns all shared state — no mutexes needed.
func (s *sub) loop() {
    var pending []Item
    var next time.Time
    var err error
    for {
        var fetchDelay time.Duration
        if now := time.Now(); next.After(now) {
            fetchDelay = next.Sub(now)
        }
        startFetch := time.After(fetchDelay)

        var first Item
        var updates chan Item
        if len(pending) > 0 {
            first = pending[0]
            updates = s.updates  // enable send case
        }

        select {
        case errc := <-s.closing:
            errc <- err
            close(s.updates)
            return
        case <-startFetch:
            var fetched []Item
            fetched, next, err = s.fetcher.Fetch()
            pending = append(pending, fetched...)
        case updates <- first:   // only fires when updates != nil
            pending = pending[1:]
        }
    }
}
```

The key trick: `updates` is `nil` when there's nothing to send, which disables that `select`
case. When there's something to send, we set it to the real channel, enabling the case. This
is a powerful idiom for building state machines in a single goroutine.

## 14. Pattern: Tee (broadcast to multiple consumers)

Duplicate every value from one channel to two (or more).

```go
func tee[T any](ctx context.Context, in <-chan T) (<-chan T, <-chan T) {
    out1, out2 := make(chan T), make(chan T)
    go func() {
        defer close(out1); defer close(out2)
        for v := range in {
            var a, b = out1, out2
            for i := 0; i < 2; i++ {
                select {
                case a <- v: a = nil
                case b <- v: b = nil
                case <-ctx.Done(): return
                }
            }
        }
    }()
    return out1, out2
}
```

The double-`select` with `nil`-assignment ensures every value goes to *both* outputs before the
loop advances. If you only want "broadcast to whichever is ready" semantics, drop the inner
loop.

## 15. Pattern: Semaphore via buffered channel

Cap concurrent access:

```go
sem := make(chan struct{}, 10)  // 10 concurrent max

func serve(req *Request) {
    sem <- struct{}{}       // acquire (blocks if 10 already in flight)
    defer func() { <-sem }() // release
    process(req)
}
```

The buffer size is the cap. Simple, allocation-free, cancellation-friendly (wrap the acquire
in a `select` with `ctx.Done()`).

## 16. Pattern: Ring buffer

When a slow consumer shouldn't block a fast producer and it's OK to drop old values:

```go
func ringBuffer[T any](in <-chan T, size int) <-chan T {
    out := make(chan T, size)
    go func() {
        defer close(out)
        for v := range in {
            select {
            case out <- v:
                // room; just enqueue
            default:
                // full; drop oldest
                <-out
                out <- v
            }
        }
    }()
    return out
}
```

Know what semantics you want — dropping old, dropping new, or blocking — before reaching for
this. "Silent data loss" is a legitimate choice for telemetry; never for financial transactions.

## 17. `errgroup` — structured concurrency for the common case

`golang.org/x/sync/errgroup` is the highest-leverage package for 80% of concurrent Go. Use it
whenever you have a fixed set of parallel tasks that can each fail:

```go
import "golang.org/x/sync/errgroup"

func fetchAll(ctx context.Context, ids []int) ([]Record, error) {
    g, ctx := errgroup.WithContext(ctx)   // derived ctx cancels on first error
    records := make([]Record, len(ids))
    g.SetLimit(10)                         // cap concurrent goroutines
    for i, id := range ids {
        g.Go(func() error {
            rec, err := fetchOne(ctx, id)
            if err != nil { return err }
            records[i] = rec   // safe: each i is unique
            return nil
        })
    }
    if err := g.Wait(); err != nil { return nil, err }
    return records, nil
}
```

Why this is so good:

- **First error cancels the derived `ctx`**, so all sibling goroutines see `<-ctx.Done()` and can
  bail out early — no wasted work.
- **`g.Wait()` returns the first non-nil error**, exactly what you want 95% of the time.
- **`g.SetLimit(n)`** caps concurrency — no separate semaphore needed.
- **The loop variable is safe** in Go 1.22+; each goroutine captures its own `i, id`.

Reach for `errgroup` unless you specifically need streaming/pipeline semantics, in which case
use raw channels.

## 18. Choosing a pattern — decision tree

Walk this tree top-down:

1. **Is concurrency even needed?** If the sequential version meets latency/throughput goals,
   stop here. Goroutines aren't free.

2. **Is it a fixed set of parallel tasks (N known, each fallible)?**
   → `errgroup.Group` with `SetLimit`. Done.

3. **Is it a streaming transform (source → stage → stage → sink)?**
   → Pipeline. Each stage is a `<-chan In → <-chan Out` function. Apply the two pipeline rules.

4. **Is the work-source unbounded and you need bounded parallelism?**
   → Worker pool or bounded parallelism (pattern 7/8).

5. **Is it "race N replicas for fastest response"?**
   → First-responder (pattern 11) with a buffered result channel.

6. **Is it "broadcast one value to many listeners"?**
   → Tee (pattern 14) or subscription (pattern 13), depending on dynamic vs. static listeners.

7. **Is it "protect a shared data structure"?**
   → This is the mutex sweet spot. See `references/sync-primitives.md`. Channels would be
   over-engineering.

8. **Is it "coordinate many goroutines to stop cleanly"?**
   → Close a `done` channel (broadcast) or cancel a `context.Context` (standard way).

When in doubt: start with `errgroup` or a simple channel. Complexity should be earned by a
concrete requirement, not assumed.
