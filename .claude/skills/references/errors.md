# Errors in Go — The Complete Treatment

Go's error philosophy: **errors are values, not control flow.** You return them, inspect them,
wrap them, and match them — you don't throw them, and you rarely panic on them. This file
covers every decision point an agent faces when designing error-handling for a Go codebase.

## Table of contents

1. [The core rules](#1-the-core-rules)
2. [Sentinel errors](#2-sentinel-errors)
3. [Wrapping with `%w`](#3-wrapping-with-w)
4. [Custom error types](#4-custom-error-types)
5. [`errors.Is` vs `errors.As`](#5-errorsis-vs-errorsas)
6. [Multiple errors: `errors.Join`](#6-multiple-errors-errorsjoin)
7. [When to panic (almost never)](#7-when-to-panic-almost-never)
8. [`recover` — the three legitimate uses](#8-recover--the-three-legitimate-uses)
9. [Error messages — the style guide](#9-error-messages--the-style-guide)
10. [Design patterns for error-rich APIs](#10-design-patterns-for-error-rich-apis)

---

## 1. The core rules

1. **Return errors; don't ignore them.** Every function that can fail returns `error` as its
   last return value. Callers check it immediately or deliberately explain why they don't.
2. **Add context when an error crosses a layer boundary.** `fmt.Errorf("loadUser %d: %w", id, err)`
   preserves the cause and adds the who/why.
3. **Match errors by identity, not by string.** `errors.Is(err, ErrNotFound)` — never
   `err.Error() == "not found"`.
4. **Don't log AND return.** Pick one. Logging the same error at every layer produces duplicate
   noise. Log at the top-level handler; return everywhere else.
5. **`panic` is for impossible states.** An unreachable case, a broken invariant, a nil pointer
   that can never be nil. Not for "file not found" or "unauthorized."

## 2. Sentinel errors

Package-level `var` declarations that callers can match against. Name them with `Err` prefix:

```go
package userdb

import "errors"

var (
    ErrNotFound      = errors.New("user: not found")
    ErrAlreadyExists = errors.New("user: already exists")
    ErrInvalidEmail  = errors.New("user: invalid email")
)
```

**When to use sentinels:** the caller needs to behave differently based on which specific
error occurred. If the caller just logs and returns, you don't need a sentinel — any error
value will do.

**When NOT to use sentinels:** when the error carries context (a filename, a line number, a
stack). Use a custom error type instead (next section).

**Don't over-export sentinels.** Each exported `ErrX` is a piece of API that your callers can
depend on. Think twice before adding one.

## 3. Wrapping with `%w`

`fmt.Errorf` with `%w` produces an error that implements `Unwrap() error`, linking to the
cause. `errors.Is` and `errors.As` walk this chain.

```go
func loadUser(id int) (*User, error) {
    row, err := db.Query("SELECT ... WHERE id = ?", id)
    if err != nil {
        return nil, fmt.Errorf("loadUser %d: %w", id, err)
    }
    // ...
}
```

**One `%w` per `Errorf` call** (the stdlib accepts multiple since Go 1.20, but it's confusing
— prefer `errors.Join` for that case, section 6).

**Use `%v` when you want to report but not expose the cause to `errors.Is`.** For example, a
parser wrapping an internal scanner error where you don't want callers to depend on the
scanner's identity:

```go
return fmt.Errorf("parse config: %v", scanErr)  // flattens; cause hidden
```

This is rare — usually `%w` is right. But know the distinction.

## 4. Custom error types

When an error needs to carry structured context (more than a string), define a struct:

```go
type PathError struct {
    Op   string  // "open", "unlink"
    Path string
    Err  error   // cause
}

func (e *PathError) Error() string {
    return e.Op + " " + e.Path + ": " + e.Err.Error()
}

func (e *PathError) Unwrap() error { return e.Err }
```

Callers can then extract the struct with `errors.As`:

```go
var pe *PathError
if errors.As(err, &pe) {
    fmt.Printf("failed %s on %s: %v\n", pe.Op, pe.Path, pe.Err)
}
```

**Receiver convention:** use a pointer receiver for `Error()` unless the error is a tiny value
type (like `errors.New`'s underlying struct). Pointer receivers mean two separate `*MyError`
values are distinct even if their fields are equal — usually what you want for errors.

## 5. `errors.Is` vs `errors.As`

- `errors.Is(err, target)` — "is `err` (or any error it wraps) equal to `target`?" Use for
  **sentinel matching**.
- `errors.As(err, &dst)` — "is `err` (or any error it wraps) of the type of `dst`? If so, set
  `dst` to it." Use for **custom type extraction**.

```go
if errors.Is(err, io.EOF) { /* sentinel match */ }

var ne *net.OpError
if errors.As(err, &ne) { /* now ne is populated, inspect its fields */ }
```

Custom `Is` method — define when two distinct values should match:

```go
type HTTPError struct { Status int }
func (e *HTTPError) Error() string { return fmt.Sprintf("http %d", e.Status) }
func (e *HTTPError) Is(target error) bool {
    te, ok := target.(*HTTPError)
    return ok && te.Status == e.Status
}
```

Now `errors.Is(err, &HTTPError{Status: 404})` matches any 404 regardless of struct identity.

## 6. Multiple errors: `errors.Join`

When a function genuinely has multiple independent failures (e.g. closing several resources
where each might fail), use `errors.Join`:

```go
func Close() error {
    var errs []error
    if err := f1.Close(); err != nil { errs = append(errs, err) }
    if err := f2.Close(); err != nil { errs = append(errs, err) }
    return errors.Join(errs...)  // returns nil if errs is empty
}
```

`errors.Is` and `errors.As` walk joined errors — callers don't need to know whether it's a
single or joined error.

Don't abuse this for "here are 3 validation errors, here's the cause, here's a stack trace."
That's what custom types are for.

## 7. When to panic (almost never)

Legitimate panics:

- **Unrecoverable startup failure** in an `init()` or `main`: `panic("config.yaml missing")`.
  Even here, `log.Fatal` is more idiomatic; it logs and exits without a stack dump.
- **Unreachable code** in a switch: `default: panic("unreachable: " + x.Kind())`. Signals a
  bug in the program's internal logic.
- **Broken invariant**: `if x.size < 0 { panic("size went negative") }`. The code has a bug
  worth crashing over.

**Never panic for:**

- User input errors (return error).
- I/O failures (return error).
- Missing resources at runtime (return error).
- Rate limit / unauthorized / not found (return error, often a sentinel).

**Library authors especially:** `panic` in a library is a betrayal of the caller's trust —
they can't easily guard against every panic without `recover` at every call site. Export an
error.

## 8. `recover` — the three legitimate uses

1. **Goroutine-boundary safety in long-running servers.** One request panicking shouldn't
   take down the entire server. HTTP middleware and similar frameworks recover at the boundary,
   log the panic, and return a 500.

   ```go
   func Recover(next http.Handler) http.Handler {
       return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
           defer func() {
               if rec := recover(); rec != nil {
                   slog.Error("panic in handler",
                       "panic", rec,
                       "stack", string(debug.Stack()),
                       "method", r.Method,
                       "path", r.URL.Path)
                   http.Error(w, "internal error", http.StatusInternalServerError)
               }
           }()
           next.ServeHTTP(w, r)
       })
   }
   ```

2. **Internal `panic` as non-local exit in a deeply recursive parser.** Convert the panic back
   to an error at the public API boundary. Used in `encoding/json`, `regexp`, and the stdlib
   parser itself. Keep it internal — never let the panic escape the package.

3. **Test helpers that expect specific panics.** `assert.Panics`-style helpers.

**That's it.** `recover` is not a general error-handling mechanism. A code base with
`recover` scattered throughout is usually misusing it.

**`recover` must be called directly in a deferred function** — not from a function called by
a deferred function. The stdlib catches some misuse at compile time, but not all.

## 9. Error messages — the style guide

- **Lowercase, no trailing punctuation.** `errors.New("user not found")`, not
  `"User not found!"`. Reason: errors are typically wrapped, and `"outer: User not found!"`
  reads poorly.
- **No "failed to" prefix.** The return type `error` already implies failure. Write what the
  operation was trying to do.
  - Bad: `"failed to open file: x.txt: no such file"`
  - Good: `"open x.txt: no such file"`
- **Include identifiers.** `"loadUser 42: ..."` beats `"loadUser: ..."` by a lot when you're
  in a debugger.
- **Context at the boundary, not at the source.** The low-level `os.Open` returns just
  `"open x.txt: no such file"`; callers add `"loadConfig: open x.txt: no such file"` as they
  wrap.

## 10. Design patterns for error-rich APIs

### Validator error aggregation

```go
type ValidationError struct {
    Field   string
    Message string
}

func (v *ValidationError) Error() string { return v.Field + ": " + v.Message }

func ValidateUser(u User) error {
    var errs []error
    if u.Email == "" { errs = append(errs, &ValidationError{"email", "required"}) }
    if u.Age < 0    { errs = append(errs, &ValidationError{"age", "must be non-negative"}) }
    return errors.Join(errs...)
}
```

Callers can iterate all violations with a `for`/unwrap loop or display the first one. UI layers
can walk the joined chain with `errors.As` to extract all `ValidationError`s.

### Retryable / terminal errors

Define an interface your errors can satisfy:

```go
type Retryable interface { Retryable() bool }

type transientError struct{ cause error }
func (e *transientError) Error() string     { return e.cause.Error() }
func (e *transientError) Unwrap() error     { return e.cause }
func (e *transientError) Retryable() bool   { return true }
```

Caller:

```go
for attempt := 0; attempt < 3; attempt++ {
    err := do(ctx)
    if err == nil { return nil }
    var r Retryable
    if !errors.As(err, &r) || !r.Retryable() {
        return err  // terminal — give up
    }
    time.Sleep(backoff(attempt))
}
```

### HTTP status mapping

```go
func StatusFor(err error) int {
    switch {
    case errors.Is(err, ErrNotFound):     return http.StatusNotFound
    case errors.Is(err, ErrUnauthorized): return http.StatusUnauthorized
    case errors.Is(err, context.DeadlineExceeded): return http.StatusGatewayTimeout
    default:                              return http.StatusInternalServerError
    }
}
```

This keeps HTTP concerns out of domain code while giving a single place to update status
mappings.

### The "handle error once" rule

One of Rob Pike's proverbs: **"Errors are handled exactly once."** Handling means: log, retry,
return to user, fall back, translate. Don't do two. Don't return AND log. Don't retry AND
wrap-and-return. Pick the one place that owns the decision.

This single discipline eliminates most "I see this error three times in my logs" noise.
