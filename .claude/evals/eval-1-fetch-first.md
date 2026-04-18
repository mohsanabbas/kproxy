# FetchFirst Concurrent URL Fetcher

This is a first-responder pattern: N parallel fetches, return the first success, cancel the rest.

## Solution

```go
// Package fetch provides concurrent URL fetching.
package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// ErrAllFailed is returned when every URL failed before any succeeded.
var ErrAllFailed = errors.New("fetch: all URLs failed")

// defaultClient is a shared, timeout-bounded http.Client.
// Never use http.DefaultClient it has no timeouts and can hang forever.
var defaultClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   10,
	},
}

// FetchFirst fetches every URL in parallel and returns the body of the first
// one that responds with 200 OK. The remaining requests are canceled as soon
// as a winner is chosen.
//
// If every URL fails (non-2xx or transport error), returns a joined error
// wrapped with ErrAllFailed.
// If ctx is canceled or expires before any success, returns ctx.Err().
func FetchFirst(ctx context.Context, urls []string) ([]byte, error) {
	if len(urls) == 0 {
		return nil, fmt.Errorf("fetch: %w", errors.New("no URLs provided"))
	}

	// Derived context so we can cancel siblings the moment we have a winner.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // ALWAYS cancels any still-running goroutines on return

	// Buffer sized to len(urls) so that a late goroutine (arriving after we
	// already picked a winner) can still deposit its result and exit cleanly.
	// With an unbuffered channel, late senders would block forever a leak.
	type result struct {
		body []byte
		url  string
	}
	wins := make(chan result, len(urls))
	errs := make(chan error, len(urls))

	for _, u := range urls {
		// Go 1.22+: loop var is per-iteration; no explicit shadow needed.
		go func() {
			body, err := fetchOne(ctx, u)
			if err != nil {
				select {
				case errs <- fmt.Errorf("%s: %w", u, err):
				case <-ctx.Done():
				}
				return
			}
			select {
			case wins <- result{body: body, url: u}:
			case <-ctx.Done():
				// We lost the race; somebody else already won. That's fine.
			}
		}()
	}

	// Wait for the first winner, or for every URL to have failed, or for ctx.
	var failures []error
	for i := 0; i < len(urls); i++ {
		select {
		case r := <-wins:
			return r.body, nil // cancel() in defer stops siblings
		case err := <-errs:
			failures = append(failures, err)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Every URL produced an error. Join them so the caller sees each cause.
	return nil, fmt.Errorf("%w: %w", ErrAllFailed, errors.Join(failures...))
}

// fetchOne performs a single GET, returning the body on 2xx or an error.
// The request respects ctx for cancellation (critical without this the
// http.Client's internal timeout is the only escape).
func fetchOne(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}

	resp, err := defaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer func() {
		// Drain before Close so the connection can be reused by the pool.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	// Bound the read to avoid a malicious or buggy server streaming
	// unbounded data and exhausting memory.
	const maxBytes = 32 << 20 // 32 MB
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return body, nil
}
```

## Design notes (what this gets right)

**Pattern choice.** This is *Pattern 11: First-responder* from the concurrency patterns reference.
Not `errgroup` we don't want "first error cancels all"; we want "first **success** cancels all".
The semantics are inverted, so hand-rolled `chan` + `select` is the right fit.

**Critical: buffered result channels.** Both `wins` and `errs` are sized to `len(urls)`. If they
were unbuffered, here's the leak: we pick a winner, cancel ctx, return but one goroutine is
mid-flight and its HTTP call just finished. It calls `wins <- result{...}`, nobody's reading,
and it blocks forever. The ctx.Done() case in the `select` is the belt; the buffer is the
suspenders. With both, late goroutines always make progress and exit.

**`defer cancel()` on the next line.** Not doing this is `govet`'s `lostcancel` warning a
classic bug. This cancel propagates to every in-flight `http.Client.Do` call via the request
context, which aborts the underlying TCP connection.

**No `http.DefaultClient`.** `DefaultClient` has no timeout, so a hanging server would pin a
goroutine indefinitely. The package-level `defaultClient` here sets transport-level timeouts
in addition to the overall `Timeout`, so a slow DNS, slow TLS handshake, or slow response
header all have their own escape routes.

**`http.NewRequestWithContext`.** The request ctx is what makes the cancel propagate into the
transport layer. `http.Get(url)` (no ctx) is uncancelable.

**Bounded response size.** `io.LimitReader` caps memory per response. Without it, a 10 GB
response file would OOM your process. 32 MB is a reasonable default; make it configurable if
clients need bigger payloads.

**Error aggregation with `errors.Join`.** When every URL fails, the caller gets every cause —
not just the last one. `errors.Is(err, ErrAllFailed)` still works for type checks, and
`errors.Unwrap` walks the chain.

**Status code check.** A 404 or 500 response is a "success" at the transport layer but not a
"success" in our domain. We return an error for any non-2xx.

## Quality gates

```bash
go vet ./...                   # catches the obvious
golangci-lint run              # catches everything else (see assets/.golangci.yml)
go test -race ./...            # prove there are no races
```

## Test (table-driven, with synctest for cancellation scenarios)

```go
package fetch_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"example.com/fetch"
)

func TestFetchFirst(t *testing.T) {
	t.Parallel()

	// Fast 200
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("fast"))
	}))
	t.Cleanup(fast.Close)

	// Slow 200 (should lose the race)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
			w.Write([]byte("slow"))
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(slow.Close)

	// Always fails
	fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(fail.Close)

	tests := []struct {
		name    string
		urls    []string
		want    string
		wantErr error
	}{
		{"fast wins over slow", []string{fast.URL, slow.URL}, "fast", nil},
		{"skips failure, takes success", []string{fail.URL, fast.URL}, "fast", nil},
		{"all fail", []string{fail.URL, fail.URL}, "", fetch.ErrAllFailed},
		{"empty input", []string{}, "", errors.New("no URLs provided")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := fetch.FetchFirst(ctx, tc.urls)
			if tc.wantErr != nil {
				if err == nil {
					t.Fatalf("err = nil, want %v", tc.wantErr)
				}
				// Use errors.Is for sentinels
				if errors.Is(tc.wantErr, fetch.ErrAllFailed) && !errors.Is(err, fetch.ErrAllFailed) {
					t.Errorf("err = %v, want wrap of ErrAllFailed", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
```

For leak-free verification, wrap the test in `testing/synctest` (Go 1.25+) or use
`go.uber.org/goleak` in `TestMain`.

## What I'd push back on if this were a code review

If the caller has a specific notion of "success" richer than 2xx (e.g. must contain a valid
JSON payload), I'd lift that up. As written, the first 2xx wins even if it's an empty body.
In many production scenarios you want to validate the body before declaring a winner.

Also, if the URLs target the *same* logical resource (classic first-responder: hit three
replicas of the same data), this is perfect. If they target *different* resources and you
actually want N independent fetches with error isolation per URL, that's a different pattern
(use `errgroup` with collected results).
