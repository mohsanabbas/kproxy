# Why `ProcessAll` leaks goroutines

This is the canonical pipeline leak. Here's what happens in production but not in tests.

## The leak, traced

Imagine `items` has 100 entries and we break after 10 results. At the point of `break`:

1. **The consumer (main goroutine) returns.** No more reads from `out`.
2. **The transform goroutine** is partway through the stream. It calls `out <- transform(it)` for
   the 11th time. Nothing is reading `out`, so this send blocks forever — `out` is unbuffered,
   and we just lost our only receiver.
3. **The transform goroutine being stuck means it never reads the next value from `in`.**
4. **The feeder goroutine** is partway through the loop. Its next `in <- it` blocks forever,
   because the transform is no longer reading.
5. **Both goroutines leak, along with every heap object their stacks reference.**

In tests, `items` is probably short (say, 10 elements), so the break condition is exactly
satisfied and there's nothing left to enqueue — no block. In production, `items` is 1,000+
and the leak triggers every call.

This is the exact bug Sameer Ajmani's *Pipelines and cancellation* post was written to
prevent. The skill's two pipeline rules state it directly:

1. Stages close their outbound channels when all sends are done.
2. **Stages keep receiving until the inbound channel closes OR a cancel signal arrives.**

`ProcessAll` violates rule 2 because there's no cancel signal — when the consumer bails early,
upstream stages have no way to know.

## The fix

Add a cancellation signal and `select`-on-send everywhere a goroutine writes to a channel it
doesn't own. Using `context.Context` since it's the standard vehicle and the function should
take a ctx anyway.

```go
func ProcessAll(ctx context.Context, items []Item) []Result {
	const limit = 10 // only want first 10

	// Derived ctx so we can signal upstream to stop the moment we're done.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	in := make(chan Item)
	out := make(chan Result)

	// Feeder: closes `in` when done (R1) and bails on cancel (R2).
	go func() {
		defer close(in)
		for _, it := range items {
			select {
			case in <- it:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Transform: closes `out` when done (R1) and bails on cancel (R2).
	go func() {
		defer close(out)
		for it := range in {
			select {
			case out <- transform(it):
			case <-ctx.Done():
				return
			}
		}
	}()

	var results []Result
	for r := range out {
		results = append(results, r)
		if len(results) >= limit {
			cancel() // broadcast "stop" to both upstream goroutines
			// Keep draining `out` until transform closes it — otherwise the
			// transform's send could race with our cancel. The range continues
			// but transform's select will pick ctx.Done() next iteration.
			for range out {
			}
			break
		}
	}
	return results
}
```

The key additions:

- **`ctx` parameter.** A library function doing concurrent work must take a ctx. Without it,
  there's no way for the caller to cancel, enforce a deadline, or propagate their own
  cancellation.
- **Derived `ctx, cancel := context.WithCancel(ctx)`.** Creating a new scope we own, so our
  `cancel()` doesn't affect the caller's parent context. `defer cancel()` on the next line is
  the standard discipline.
- **`select { case out <- v: case <-ctx.Done(): return }`** on every send to a channel the
  goroutine doesn't own. This is rule 2 in code form. Without it, a stuck send is a leak.
- **`cancel()` then drain `out`.** When we've collected enough results, we broadcast the stop
  signal. The drain loop `for range out {}` ensures we see the `out` channel close before
  returning — otherwise there's a brief window where the transform goroutine might be mid-send
  and technically leak until it notices ctx.

## Shorter alternative: `done` channel

If you really don't want to take a `ctx` (you should, but), the classic pattern uses a raw
`done` channel:

```go
func ProcessAll(items []Item) []Result {
	done := make(chan struct{})
	defer close(done) // broadcast to every goroutine watching <-done

	in := make(chan Item)
	out := make(chan Result)

	go func() {
		defer close(in)
		for _, it := range items {
			select {
			case in <- it:
			case <-done:
				return
			}
		}
	}()

	go func() {
		defer close(out)
		for it := range in {
			select {
			case out <- transform(it):
			case <-done:
				return
			}
		}
	}()

	var results []Result
	for r := range out {
		results = append(results, r)
		if len(results) >= 10 {
			break // defer close(done) signals upstream
		}
	}
	return results
}
```

`close(done)` is the channel-native broadcast. Every goroutine selecting on `<-done` wakes up
immediately with the zero value. `context.Context` is just this plus deadlines and values.

## How to verify the fix

**Before:** run with `-race` and spawn 1000 concurrent `ProcessAll` calls with a large
`items` slice. Then `runtime.NumGoroutine()` stays high after all calls return. Or, better,
use the Go 1.26 experimental `goroutineleak` profile:

```bash
GOEXPERIMENT=goroutineleakprofile go run .
```

```go
prof := pprof.Lookup("goroutineleak")
prof.WriteTo(os.Stdout, 2) // shows leaked goroutines with stacks
```

**In tests:** `testing/synctest` (Go 1.25+) is purpose-built for this. Wrap the test in
`synctest.Test(...)`. If any goroutine is still blocked when the test function returns,
`synctest` reports it as a leak. No real-time delays, no flakiness.

```go
func TestProcessAll_NoLeak(t *testing.T) {
	synctest.Test(t, func(tb *testing.T) {
		items := make([]Item, 100)
		for i := range items { items[i] = Item{ID: i} }

		results := ProcessAll(context.Background(), items)
		if len(results) != 10 {
			tb.Fatalf("got %d results, want 10", len(results))
		}
		// Any unterminated goroutine gets caught as the bubble closes.
		synctest.Wait(tb)
	})
}
```

## Summary of what else I'd change

- **Return an error**, not just `[]Result`. If `ctx` is canceled, the caller should know
  whether they got the full 10 or a partial answer.
- **Consider `errgroup`** if `transform` can fail. `errgroup.WithContext` cancels siblings on
  the first error, which is the parallel-tasks analog of what we did manually here.
- **Bound concurrency** if `transform` is expensive. Right now there's exactly one transform
  goroutine; if `transform` is CPU-bound, consider a worker pool (see
  `assets/templates/worker-pool.go`). If it's fine as-is (transform is fast), leave it simple.
