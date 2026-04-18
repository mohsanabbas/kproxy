# `sync` Primitives — When Channels Aren't the Answer

Pike's mantra is "share memory by communicating" — but he also said, in the same talk:
**"Don't overdo it. Sometimes all you need is a reference counter."** This file covers the
cases where mutexes, atomics, and other `sync` primitives are the right tool.

## Table of contents

1. [The decision: channel vs. mutex](#1-the-decision-channel-vs-mutex)
2. [`sync.Mutex` and `sync.RWMutex`](#2-syncmutex-and-syncrwmutex)
3. [`sync.Once` and `sync.OnceValue`/`OnceValues`](#3-synconce-and-synconcevalueoncevalues)
4. [`sync.WaitGroup` and `WaitGroup.Go` (1.25+)](#4-syncwaitgroup-and-waitgroupgo-125)
5. [`sync.Pool`](#5-syncpool)
6. [`sync.Map` — the narrow use case](#6-syncmap--the-narrow-use-case)
7. [`sync.Cond` — rarely the answer](#7-synccond--rarely-the-answer)
8. [`atomic` — for counters and flags](#8-atomic--for-counters-and-flags)
9. [`errgroup.Group` — structured concurrency](#9-errgroupgroup--structured-concurrency)

---

## 1. The decision: channel vs. mutex

Use a **channel** when:
- Data passes through a pipeline or between goroutines.
- Coordination matters (signal, cancel, join).
- The design is naturally a sequence of steps or a set of parallel workers.

Use a **mutex** when:
- You're guarding access to a single piece of shared state (a counter, a cache, a map).
- Readers and writers are brief and there's no natural "message" to pass.
- Performance measurements show a channel is the bottleneck for a simple shared-state case.

Rule of thumb: if you'd write it as "goroutines talking to each other," use channels. If
you'd write it as "one object protecting its state from concurrent access," use a mutex.

## 2. `sync.Mutex` and `sync.RWMutex`

`Mutex` provides exclusive access. `RWMutex` allows many concurrent readers OR one writer.

```go
type Counter struct {
    mu sync.Mutex
    n  int
}

func (c *Counter) Inc() {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.n++
}

func (c *Counter) Value() int {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.n
}
```

**Rules:**

- **Zero value is usable.** `sync.Mutex{}` is an unlocked mutex. No `NewMutex()`.
- **Pointer receivers only** on methods of types containing a `Mutex`. Value receivers copy
  the mutex silently — `go vet` catches this, run it.
- **`defer mu.Unlock()`** on the next line after `mu.Lock()`. Without defer, any return path
  or panic leaves the mutex locked forever.
- **Minimize the critical section.** Lock, do the minimum, unlock. Don't make network calls
  or long computations while holding a lock.

**`RWMutex`:**

```go
func (c *Cache) Get(k string) (Value, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    v, ok := c.store[k]
    return v, ok
}

func (c *Cache) Set(k string, v Value) {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.store[k] = v
}
```

Use `RWMutex` when reads dominate writes (10:1 or more). With more balanced workloads, plain
`Mutex` is usually faster — `RWMutex` has overhead for tracking readers.

**Pitfalls:**

- **Recursive locking deadlocks.** Go mutexes aren't re-entrant. If `A.Foo()` locks and calls
  `A.Bar()` which also locks, you'll deadlock. Fix: split into locked and unlocked versions,
  typically a public `Bar()` that locks + a private `bar()` that doesn't and assumes the lock
  is held.
- **Lock ordering deadlock.** If goroutine G1 holds lock A and waits for B, while G2 holds B
  and waits for A — deadlock. Convention: always acquire locks in the same order throughout
  the program. Document the order.

## 3. `sync.Once` and `sync.OnceValue`/`OnceValues`

`sync.Once.Do(fn)` runs `fn` exactly once, no matter how many goroutines call `Do`
concurrently. The others block until `fn` completes.

```go
var (
    once sync.Once
    db   *sql.DB
)

func GetDB() *sql.DB {
    once.Do(func() {
        db = connectToDB()
    })
    return db
}
```

**Since Go 1.21, prefer `OnceValue`/`OnceValues`:**

```go
var getDB = sync.OnceValue(func() *sql.DB {
    return connectToDB()
})

// Usage:
db := getDB()   // returns the cached value on subsequent calls
```

Cleaner: no global state, no second variable. `OnceValues` returns two values (e.g. value +
error).

**When to use `Once`:**
- Expensive one-time initialization (DB connections, regex compilation, config parsing).
- Lazy initialization to avoid `init()` side effects.
- Idempotent shutdown: `once.Do(close)` makes double-close safe.

**When NOT to use `Once`:**
- Init at startup — just do it in `main` before spawning goroutines.
- When the computed value might change — `Once` is forever.

## 4. `sync.WaitGroup` and `WaitGroup.Go` (1.25+)

Classic `WaitGroup`:

```go
var wg sync.WaitGroup
for _, task := range tasks {
    wg.Add(1)
    go func(t Task) {
        defer wg.Done()
        t.Run()
    }(t)
}
wg.Wait()
```

**Rules:**
- **`Add` in the spawning goroutine**, before `go`. Never inside the goroutine itself.
- **`Done` in the goroutine**, typically via `defer`.
- **`Wait` in a third goroutine** (or the caller) — not in one of the workers.
- **No errors.** `WaitGroup` doesn't collect errors. For that, use `errgroup` (section 9).

**Go 1.25+ `wg.Go(fn)`:** single method that handles `Add`/`Done` correctly:

```go
var wg sync.WaitGroup
for _, task := range tasks {
    wg.Go(func() { task.Run() })  // captures task correctly in 1.22+
}
wg.Wait()
```

Use `wg.Go` by default — it eliminates the most common `WaitGroup` bug (calling `Add` inside
the goroutine).

## 5. `sync.Pool`

A pool of reusable objects to reduce GC pressure. Appropriate for short-lived allocations in
hot loops (e.g. request handlers).

```go
var bufPool = sync.Pool{
    New: func() any { return new(bytes.Buffer) },
}

func handle(req *Request) {
    buf := bufPool.Get().(*bytes.Buffer)
    buf.Reset()                 // CRITICAL: clear stale state
    defer bufPool.Put(buf)

    // use buf ...
}
```

**Rules:**

- **`Reset()` immediately after `Get()`.** The pool returns the object another goroutine
  previously used. Stale state is a silent contamination bug.
- **`sync.Pool` is cleared on every GC cycle.** It's a short-term reuse mechanism, not a
  durable cache.
- **Only pool objects with expensive allocation.** For a 16-byte struct, pooling is slower
  than allocating — the pool's own bookkeeping costs more.
- **Put must release references.** Putting a `*bytes.Buffer` with a 1 GB underlying array
  back in the pool pins that memory. Truncate or reset to a small size before Put.

**When to pool:** high-throughput request handlers, serialization buffers, connection wrappers,
any allocation that shows up in pprof as a hot spot.

**When not to pool:** anything where correctness beats performance, or where the object is
expensive to construct AND cheap to hold in a long-lived cache. `sync.Pool` is for the
"expensive to allocate, cheap to discard" quadrant.

## 6. `sync.Map` — the narrow use case

`sync.Map` is **not a replacement for `map[K]V` + mutex.** Benchmarks consistently show the
mutex-guarded map is faster for most workloads.

`sync.Map` wins in two specific cases (from its godoc):

1. When the entry for a key is only written once but read many times (e.g. caches that grow
   monotonically).
2. When multiple goroutines read, write, and overwrite entries for **disjoint sets of keys**
   (i.e. no key contention).

If neither applies, use `map[K]V` protected by `sync.RWMutex`. The API is also more
ergonomic:

```go
// Plain map + mutex: normal Go
m[key] = value
v, ok := m[key]

// sync.Map: loses type safety, uglier API
sm.Store(key, value)
v, ok := sm.Load(key)
```

## 7. `sync.Cond` — rarely the answer

Condition variables. If you find yourself reaching for this, ask first: would a channel be
clearer?

Legitimate uses are narrow — coordinating many goroutines waiting for the same condition to
become true, where broadcast is more efficient than per-waiter channels. Even then, many Go
engineers have shipped entire careers without using `sync.Cond`.

If you decide you need it: use `NewCond(&mu)` with the guarding mutex, always hold the mutex
while calling `Wait`, always check the condition in a loop (spurious wakeups possible):

```go
cond := sync.NewCond(&mu)
mu.Lock()
for !ready { cond.Wait() }  // mu released inside Wait, reacquired before return
mu.Unlock()
```

## 8. `atomic` — for counters and flags

`sync/atomic` provides atomic operations — lock-free concurrent access to single values.

**Since Go 1.19, use the typed atomic wrappers** — `atomic.Int64`, `atomic.Bool`, `atomic.Pointer[T]`.
They're safer than the loose functions:

```go
var count atomic.Int64
count.Add(1)
n := count.Load()

var closed atomic.Bool
closed.Store(true)
if closed.Load() { ... }

var config atomic.Pointer[Config]
config.Store(newCfg)
cur := config.Load()
```

**When atomic is right:**
- Simple counters, flags, stats.
- Copy-on-write config updates (`atomic.Pointer.Store` a new `*Config`).
- Cases where mutex overhead is measurable.

**When it's wrong:**
- Protecting multiple related fields — use a mutex to keep them consistent.
- Anything requiring compare-and-set of a compound state (struct with several fields).

**The CAS (compare-and-swap) loop:**

```go
for {
    old := counter.Load()
    newVal := transform(old)
    if counter.CompareAndSwap(old, newVal) {
        break  // succeeded
    }
    // someone else raced us; retry
}
```

Use sparingly. Most code doesn't need CAS — it needs a mutex.

## 9. `errgroup.Group` — structured concurrency

Not in `sync`, but paired conceptually. From `golang.org/x/sync/errgroup`:

```go
import "golang.org/x/sync/errgroup"

g, ctx := errgroup.WithContext(ctx)
g.SetLimit(10)  // at most 10 goroutines concurrently

for _, url := range urls {
    g.Go(func() error {
        return fetch(ctx, url)
    })
}
if err := g.Wait(); err != nil {
    return err
}
```

**`errgroup` vs `WaitGroup`:**

| Feature                   | `WaitGroup` | `errgroup.Group`        |
|---------------------------|-------------|-------------------------|
| Wait for all to finish    | ✓           | ✓                       |
| First error cancels ctx   | ✗           | ✓ (with `WithContext`)  |
| Bound concurrency         | ✗           | ✓ (`SetLimit`)          |
| Return first error        | ✗           | ✓                       |
| Simplest API              | ✗           | ✓                       |

**Use `errgroup` by default.** Reach for plain `WaitGroup` only when you specifically need
no error handling, no cancellation, and no concurrency cap.

---

## Cheat sheet

| Problem                                      | Tool                                         |
|----------------------------------------------|----------------------------------------------|
| Protect a counter                            | `atomic.Int64`                               |
| Protect a small struct with 2-3 fields       | `sync.Mutex`                                 |
| Protect a read-mostly map                    | `sync.RWMutex` + `map`                       |
| One-time initialization                      | `sync.OnceValue`                             |
| Run N parallel tasks, wait for all           | `errgroup.Group`                             |
| Run goroutines, don't care about results     | `sync.WaitGroup` + `wg.Go` (1.25+)           |
| Pool expensive objects                       | `sync.Pool` (with immediate Reset)           |
| Broadcast to many waiters                    | `close(chan)` or `context.Cancel`            |
| Multiple goroutines writing disjoint keys    | `sync.Map`                                   |
| Rate-limit concurrent operations             | Buffered channel semaphore or `rate.Limiter` |
| Send values between goroutines in a pipeline | `chan T` (always)                            |
