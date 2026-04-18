# Modernization Review — 2019 Code → Go 1.26

Walking through the issues first, then the rewrite.

## What's wrong (in order of severity)

### 1. Data race in `RunAll` — the loop-variable capture bug

```go
for i, item := range items {
    wg.Add(1)
    go func() {
        defer wg.Done()
        time.Sleep(time.Second)
        results[i] = "processed: " + item  // captures i and item by reference
    }()
}
```

Pre-Go 1.22, `i` and `item` were shared across iterations. Every goroutine ended up writing
to `results[len(items)-1]` with the last item string — some results would be overwritten,
others never written, race detector fires immediately. In 2019 code, this is almost certainly
latent (probably masked by the 1-second sleep ordering).

**Two possible fixes depending on the Go version:**

- **`go.mod` says `go 1.22` or later:** the bug is fixed by the language. `i` and `item` are
  per-iteration automatically. Just bump `go.mod`.
- **`go.mod` says `go 1.21` or earlier:** shadow explicitly (`i, item := i, item`) or pass as
  arguments. But the right answer is: bump the `go.mod` directive.

For a "current Go 1.26 project," set `go 1.26` in `go.mod` and this problem dissolves.

### 2. Mutable package-level state (`jobs`, `mu`)

```go
var jobs []Job
var mu sync.Mutex
```

Package globals are a code smell in Go:

- Can't test in isolation (every test shares the same state).
- No clear ownership — who resets it? When?
- Invites data races (the mutex is right there next to the slice for a reason).
- Makes the package hard to use from two contexts in the same program.

Also, `mu` is declared but never used in this snippet. If real code locks around `jobs`
access, fine — but it belongs inside a struct.

**Fix:** encapsulate in a struct with its own mutex, constructed via `NewStore()` or similar.
Or, better, if the caller needs a collection of jobs, let them pass one in.

### 3. Needless `init()`

```go
func init() {
    jobs = make([]Job, 0)
}
```

This is a no-op: the zero value of `[]Job` is already `nil`, which is semantically equivalent
to an empty slice for every operation that matters (range, append, len). Delete the `init`
entirely.

More broadly: avoid `init()` in almost all cases. It's a hidden side effect that runs at
import time, fights testability, and can't return errors. The JetBrains 10x Commandments
explicitly flag `init` avoidance as a "be safe by default" item.

### 4. No `context.Context` in `RunAll`

A function that does concurrent work and contains `time.Sleep(time.Second)` must be
cancelable. If a caller's deadline expires or they want to bail out early, right now they
can't — they're stuck waiting at least a second. The caller can't even enforce a timeout
from the outside.

**Every function that spawns goroutines or does I/O takes `ctx context.Context` as its first
argument.**

### 5. Missed `sync.WaitGroup.Go` (Go 1.25+)

```go
wg.Add(1)
go func() {
    defer wg.Done()
    // ...
}()
```

This is the classic 3-line boilerplate that's also the source of a common bug (calling
`wg.Add(1)` inside the goroutine — which is subtly broken because `Wait()` can race past it).
Go 1.25 added `wg.Go(fn)` that does `Add+go+defer Done` in one call.

### 6. `fmt.Errorf` without `%w` in `getFirst`

```go
return Job{}, fmt.Errorf("no jobs")
```

Works, but misses a trick. If "no jobs" is a condition callers might match on, define a
sentinel: `var ErrNoJobs = errors.New("no jobs")`. Then `getFirst` returns `ErrNoJobs`
directly, and callers use `errors.Is(err, ErrNoJobs)`. Adding `%w` isn't useful here (no
underlying cause), but a sentinel is.

### 7. `errgroup` would be better than `sync.WaitGroup`

If `RunAll` could fail (e.g. the work could return an error — even if `time.Sleep` can't,
realistic replacements will), `errgroup.WithContext` gives you first-error cancellation,
concurrency limits, and error propagation in one construct. Worth the three lines it saves.

### 8. Minor: `"processed: " + item` allocates more than necessary

In a hot path, prefer `strings.Builder` or `fmt.Sprintf("processed: %s", item)` with a
preallocated buffer. For this example, the `time.Sleep(time.Second)` dominates anyway, so
don't bother — noting for completeness.

## Modernized rewrite

```go
// Package work provides concurrent job processing.
package work

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrNoJobs is returned by (*Store).First when the store is empty.
var ErrNoJobs = errors.New("work: no jobs")

// Job is a unit of work identified by Name.
type Job struct {
	Name string
}

// Store holds a list of jobs with safe concurrent access. The zero value is
// usable — no constructor needed.
type Store struct {
	mu   sync.Mutex
	jobs []Job
}

// Add appends a job to the store.
func (s *Store) Add(j Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = append(s.jobs, j)
}

// First returns the oldest job, or ErrNoJobs if the store is empty.
func (s *Store) First() (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.jobs) == 0 {
		return Job{}, ErrNoJobs
	}
	return s.jobs[0], nil
}

// RunAll processes every item concurrently, returning the transformed
// results in the same order as the input. Each transform takes roughly the
// same time; if any fail, the first error is returned and remaining work is
// canceled.
func RunAll(ctx context.Context, items []string) ([]string, error) {
	results := make([]string, len(items))

	// errgroup gives us structured concurrency: first error cancels the
	// derived ctx, which causes in-flight goroutines to notice and bail.
	// SetLimit caps concurrent work — tune to your workload.
	g, gctx := errgroupWithContext(ctx)
	g.SetLimit(8)

	for i, item := range items {
		// Go 1.22+: i and item are per-iteration, no shadowing needed.
		g.Go(func() error {
			return process(gctx, i, item, results)
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("RunAll: %w", err)
	}
	return results, nil
}

// process handles one item. Each goroutine writes to a distinct index in
// `results`, so no synchronization is needed for that write.
func process(ctx context.Context, i int, item string, results []string) error {
	// Example placeholder for real work. Honor ctx so a caller's timeout
	// or cancellation takes effect here, not just between items.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	results[i] = "processed: " + item
	return nil
}
```

### Why I'd also reach for `sync.WaitGroup.Go` in simpler cases

If the work genuinely can't fail (`RunAll` *does* only `time.Sleep + assign`), you don't need
`errgroup`. Use `sync.WaitGroup.Go` (Go 1.25+):

```go
func RunAll(ctx context.Context, items []string) []string {
	results := make([]string, len(items))
	var wg sync.WaitGroup
	for i, item := range items {
		wg.Go(func() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			results[i] = "processed: " + item
		})
	}
	wg.Wait()
	return results
}
```

But the moment `process` can fail, `errgroup` wins. My preference: start with `errgroup`
because expansion is easier than migration.

## `go.mod` and tooling

Make sure `go.mod` declares the modern version:

```
module example.com/work

go 1.26
```

Wire in:

- `go vet ./...` — catches the mutex-copy and lost-cancel bugs by default.
- `golangci-lint run` with `copyloopvar`, `loopclosure`, `errcheck`, `errorlint` enabled —
  flags the old patterns if any snuck back in. (See `assets/.golangci.yml` in the skill for a
  ready config.)
- `go test -race ./...` — proves the loop-var bug is dead.

And once, on this codebase, run:

```bash
go fix ./...
```

Since Go 1.26, `go fix` is a modernizer — it'll apply dozens of idiom updates automatically.
Review the diff, run tests, commit.

## Summary of changes

| Before                                 | After                                           |
|----------------------------------------|-------------------------------------------------|
| Mutable package globals `jobs`, `mu`   | `Store` struct with encapsulated state          |
| Pointless `init()` zeroing a slice     | Removed; zero value of slice is already useful  |
| `RunAll(items)` — no cancellation      | `RunAll(ctx, items)` — cancelable               |
| `wg.Add + go + defer wg.Done`          | `errgroup.Go` (or `wg.Go` when no errors)       |
| Implicit data race on `results[i]`     | Fixed by Go 1.22+ loop semantics + `go.mod` bump|
| `fmt.Errorf("no jobs")`                | `var ErrNoJobs = errors.New("work: no jobs")`   |
| No concurrency cap                     | `g.SetLimit(8)` — bounded                       |

(Note: the `errgroupWithContext` referenced above is shorthand for
`errgroup.WithContext` from `golang.org/x/sync/errgroup` — inline that import.)
