# Idiomatic Go Style and Design

A condensed synthesis of *Effective Go*, the "10x Commandments," and Rob Pike's talks on
naming and simplicity. This is the style reference an agent should consult whenever the
question is "what's the Go way to do X?"

## Table of contents

1. [Naming](#1-naming)
2. [Package structure](#2-package-structure)
3. [Interfaces](#3-interfaces)
4. [Struct design](#4-struct-design)
5. [Constructors](#5-constructors)
6. [Functional options (`WithX`)](#6-functional-options-withx)
7. [Composition via embedding](#7-composition-via-embedding)
8. [Control flow idioms](#8-control-flow-idioms)
9. [The `init()` function don't](#9-the-init-function--dont)
10. [Logging with `slog`](#10-logging-with-slog)
11. [File and directory conventions](#11-file-and-directory-conventions)
12. [Go proverbs the short list](#12-go-proverbs--the-short-list)

---

## 1. Naming

Go naming is terse but consistent. Once you learn the vocabulary, code reads like prose.

**Packages:** short, lowercase, single word, no underscores or mixedCaps. `http`, `bytes`,
`json` not `http_client`, `bytesutil`, `JSON`. The package name isn't for disambiguation;
it's a namespace for the types inside it. `bufio.Reader` works because `bufio` + `Reader` is
specific; the package doesn't need to be called `bufioreader`.

**Types:** upper-case (exported) or lower-case (package-private). One word when possible,
`MixedCaps` for multi-word. `Buffer`, `Reader`, `UserID`, `HTTPClient` (not `HttpClient` —
acronyms are all-caps or all-lower).

**Functions & methods:** same casing rules. Short when in scope, longer when exported and
context-free:

```go
// Inside the package, "s" is fine because context narrows the meaning:
func (b *Buffer) grow(n int) { ... }

// Exported, it needs to stand alone:
func (b *Buffer) WriteString(s string) (int, error) { ... }
```

**Variables the shorter the scope, the shorter the name:**

```go
for i, v := range items { ... }              // tight scope: one letter

func Parse(src []byte) ([]Token, error) {     // function-scope: 3-4 letters
    buf := bytes.NewBuffer(src)
    ...
}

var DefaultConfig = &Config{...}              // package-scope: descriptive
```

**Conventional names memorize these:**

| Name    | Type                      |
|---------|---------------------------|
| `ctx`   | `context.Context`         |
| `err`   | `error`                   |
| `ok`    | `bool` (comma-ok idiom)   |
| `buf`   | `*bytes.Buffer`, `[]byte` |
| `b`     | `byte`, `*bytes.Buffer`   |
| `r`     | `io.Reader`               |
| `w`     | `io.Writer`               |
| `f`     | `*os.File`                |
| `i`, `j`, `k` | loop indices        |
| `n`     | count, bytes written/read |
| `req`   | `*http.Request`           |
| `resp`, `res` | `*http.Response`    |
| `path`  | file path string          |
| `s`     | `string`                  |
| `data`  | arbitrary `[]byte`        |
| `opts`  | options struct / slice    |

Reusing the same 1-2 letter name for every method on the same type is required:

```go
func (s *Server) Start() { ... }
func (s *Server) Stop()  { ... }
func (s *Server) Reload() { ... }  // always "s", never "server" or "srv" in one and "s" in another
```

**Don't `Get` in getters.** Go convention: `obj.Owner()` not `obj.GetOwner()`. The upper-case
name already signals export.

**Interface names** end in `-er` for one-method interfaces: `Reader`, `Writer`, `Stringer`,
`Formatter`, `Closer`. Compose them: `ReadWriter`, `ReadCloser`.

**Error sentinel names** start with `Err`: `ErrNotFound`, `ErrInvalidInput`. Custom error
types end with `Error`: `PathError`, `ValidationError`.

## 2. Package structure

**Write packages, not programs.** Your `main` should parse flags, set up logging, and call a
library package. Everything testable goes in the library.

```
myservice/
├── cmd/myservice/main.go       # entrypoint: flags + wiring only
├── internal/                   # compiler-enforced private to this module
│   ├── api/                    # HTTP handlers
│   ├── core/                   # domain logic
│   └── store/                  # DB adapters
├── go.mod
└── go.sum
```

**One concept per package.** A package should have a single, cohesive purpose you can
describe in one sentence. If you can't, split it.

**`internal/`** is compiler-enforced: `foo/internal/bar` can only be imported by packages
under `foo/`. Use it aggressively if a package is an implementation detail of your module,
put it in `internal/`. This lets you refactor freely without breaking external users.

**Avoid circular imports.** Go disallows them at compile time. The cycle is almost always a
sign that your package boundaries are wrong, not that you need a new language feature. Pull
the shared types into a third, lower-level package.

**`pkg/` is optional and controversial.** Some communities use `pkg/` for "public API meant
for import by other modules." If your module is a library, put code at the root. `pkg/` is
only useful in monorepos that mix library and binary packages.

## 3. Interfaces

**Define interfaces at the consumer, not the producer.** This is the single most important
interface rule in Go.

```go
// BAD: package userstore exports an interface, forcing every consumer to import it.
package userstore
type Store interface { Get(id int) (*User, error); Put(u *User) error }
type Postgres struct { ... }
func (p *Postgres) Get(...) ... { ... }

// GOOD: package userstore exports the concrete type. Consumers define what they need.
package userstore
type Postgres struct { ... }
func (p *Postgres) Get(id int) (*User, error) { ... }

package report
type userGetter interface { Get(id int) (*User, error) }  // minimal; unexported
func Generate(g userGetter) Report { ... }
```

**Why:** the consumer knows what it needs; the producer doesn't. Producer-defined interfaces
almost always end up with too many methods. Consumer-defined interfaces are always minimal —
they contain exactly what one specific caller uses.

**Small interfaces compose; large ones don't.** `io.Reader` (one method) can be satisfied by
a file, a network socket, a byte buffer, a compressed stream, a limited reader, a teeing
reader, and on and on. A `ReadWriteCloserSeeker` interface can't be satisfied by any of
those alone.

**Prefer returning concrete types from constructors.** `func NewStore() *Postgres` is better
than `func NewStore() Store`. Callers can then assign to a narrower interface locally if
needed.

**`var _ Iface = (*Impl)(nil)`** is a compile-time check that `*Impl` satisfies `Iface`.
Place it near the type definition. If the interface changes and `Impl` no longer satisfies
it, compilation fails with a clear message.

## 4. Struct design

**Zero value useful whenever possible.** Design so `var x T` gives a working value:

```go
// GOOD
var b bytes.Buffer       // ready to use
b.WriteString("hello")

// BAD design (though this type doesn't exist in stdlib illustrative)
var b BrokenBuffer       // have to remember b.Init() first
b.Init()
b.WriteString("hello")
```

When the zero value can't work (needs a map, a channel, a connection), hide the type behind
a constructor and return a pointer.

**Keep structs small.** 3–7 fields is comfortable. 15+ is a code smell either it's really
several types mashed together, or it's an "entity" in which case you're fine but consider
grouping related fields into sub-structs.

**Field ordering for alignment:** group fields by size, largest first, to minimize padding.
Use `fieldalignment -fix ./...` to automate this on bulk-allocated types. Example:

```go
// Wastes padding: bool + 7 bytes + int64 + bool + 7 bytes = 24 bytes
type Bad struct {
    enabled bool
    size    int64
    active  bool
}

// Tight: int64 + bool + bool + 6 bytes = 16 bytes
type Good struct {
    size    int64
    enabled bool
    active  bool
}
```

**Export fields sparingly.** If you export a field, callers can read and write it you've
coupled to the field layout. Prefer unexported fields + methods (`.Owner()`, `.SetOwner(u)`)
for anything that might gain validation or side-effects later.

## 5. Constructors

**`NewX` is the convention.** Returns `*X` or `X` depending on whether the type is usable as
a value:

```go
func NewBuffer(buf []byte) *Buffer { ... }
func NewTicker(d time.Duration) *Ticker { ... }
```

**Validate aggressively in the constructor.** A constructor that returns a value should
guarantee that value is usable. If validation fails, return an error alongside a zero value:

```go
func NewConfig(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil { return nil, fmt.Errorf("read %s: %w", path, err) }
    var c Config
    if err := yaml.Unmarshal(data, &c); err != nil {
        return nil, fmt.Errorf("parse %s: %w", path, err)
    }
    if err := c.validate(); err != nil {
        return nil, fmt.Errorf("config %s: %w", path, err)
    }
    return &c, nil
}
```

Now every `*Config` anywhere in the program is known-valid. Invariants are easier to reason
about when they can't be violated after construction.

**When a package has exactly one type of interest and it's named `Ring`, `NewRing` becomes
`ring.New`.** This is a minor idiom but reads well:

```go
r := ring.New(5)           // instead of ring.NewRing(5)
tm := time.Now()           // instead of time.NewTime()
```

## 6. Functional options (`WithX`)

For types with many optional configuration knobs, the functional-options pattern beats
struct-init constructors:

```go
type Server struct {
    addr    string
    timeout time.Duration
    tlsConfig *tls.Config
}

type Option func(*Server)

func WithTimeout(d time.Duration) Option  { return func(s *Server) { s.timeout = d } }
func WithTLS(cfg *tls.Config) Option      { return func(s *Server) { s.tlsConfig = cfg } }

func NewServer(addr string, opts ...Option) *Server {
    s := &Server{addr: addr, timeout: 30 * time.Second}  // defaults
    for _, opt := range opts { opt(s) }
    return s
}

// Usage:
srv := NewServer(":8080", WithTimeout(5*time.Second))
```

**When to reach for this:**
- Three or more optional parameters.
- New options expected over time (adding one is backward-compatible).
- Some options have interdependencies (an option function can inspect `s` before setting).

**When NOT to:**
- 1-2 required parameters with no options just take them as arguments.
- Options that must be set together make them required arguments instead.

## 7. Composition via embedding

Go has no inheritance. It has **embedding**: putting one type inside another promotes its
methods.

```go
type Logger struct { ... }
func (l *Logger) Log(msg string) { ... }

type Server struct {
    *Logger   // embedded
    addr string
}

s := &Server{Logger: NewLogger(), addr: ":8080"}
s.Log("starting")   // promoted from Logger
```

**When to embed:** the outer type genuinely "is-a" extension of the inner, and you want the
inner's methods to appear on the outer. Common cases: middleware wrapping `http.Handler`,
decorators wrapping `io.Reader`, types enriching a mutex or logger.

**When NOT to embed:** when you just want access to some methods prefer a named field.
Embedding exports all the inner's methods, which might not be what you want.

**Overriding works via shadowing:** define a method with the same name on the outer type and
it takes precedence. Call the inner via the embedded-field name: `s.Logger.Log(...)`.

## 8. Control flow idioms

**Early return with guard clauses** beats nested ifs:

```go
// IDIOMATIC
func handle(r *Request) error {
    if r == nil { return ErrNil }
    if !r.Valid() { return ErrInvalid }
    user, err := load(r.ID)
    if err != nil { return fmt.Errorf("load: %w", err) }
    // happy path here, at indentation level 1
    return save(user)
}
```

The successful flow runs down the page; errors eliminate themselves as they arise. No
rightward drift, no `else` branches, no tracking which branch you're in.

**Use `if` with initializer:**

```go
if err := validate(x); err != nil {
    return err
}
// err is scoped to the if; doesn't leak
```

**`switch` over if-else chains** when you have 3+ cases:

```go
switch {
case x < 0:  return negative
case x == 0: return zero
default:     return positive
}
```

**Type switch** for interface type discrimination:

```go
switch v := i.(type) {
case string: return "s=" + v
case int:    return fmt.Sprintf("i=%d", v)
default:     return fmt.Sprintf("unknown %T", v)
}
```

**Avoid labels and `goto`** except for breaking out of nested loops, which is occasionally
unavoidable:

```go
Outer:
for i := 0; i < n; i++ {
    for j := 0; j < m; j++ {
        if done { break Outer }
    }
}
```

## 9. The `init()` function don't

`init()` runs at package import time. It's a hidden side effect: reading a file, setting a
global, phoning home. Some problems:

- Untestable without reaching into package internals.
- Hard to reason about import order.
- `init()` failures crash the program before `main`.
- Running tests imports your package, running your `init()` with test environment assumptions.

**Legitimate uses** (rare):

- Registering a driver with `database/sql.Register` (pattern is forced by the stdlib).
- Registering an HTTP handler with `http.Handle` also legacy pattern; prefer building your
  own `http.ServeMux`.
- Compiling a regex used throughout the package but `sync.OnceValue` is better for this.

For everything else, do setup in `main` or a `Setup()` function the caller invokes
explicitly.

## 10. Logging with `slog`

`log/slog` (stdlib since 1.21) is the logging facility. Use it, not `log.Println`,
third-party loggers, or `fmt.Println`.

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
}))
slog.SetDefault(logger)

slog.Info("starting server", "addr", addr, "env", env)
slog.Error("request failed",
    "err", err,
    "method", r.Method,
    "path", r.URL.Path,
    "duration_ms", time.Since(start).Milliseconds())
```

**Structured, not formatted.** Don't build strings pass key-value pairs. This keeps logs
machine-parseable (grep, Elasticsearch, Datadog all love structured logs).

**Log at actionable levels.** A log line should tell the reader either (a) that something
went wrong and here's what, or (b) a major lifecycle event (startup, shutdown, config
reload). Don't log "entering function foo" that's what traces are for.

**Never log secrets.** Passwords, API keys, PII. If an internal type has sensitive fields,
make it implement `slog.LogValuer` to redact them:

```go
func (u *User) LogValue() slog.Value {
    return slog.GroupValue(
        slog.String("id", u.ID),
        slog.String("email", "[REDACTED]"),
    )
}
```

**Pass the context.** When available, `slog.InfoContext(ctx, ...)` lets handlers extract
request IDs and trace info.

## 11. File and directory conventions

**One logical unit per file.** Not one function, not one type a group of closely related
types/functions. Split when a file exceeds ~500 lines or when two halves of it are about
different concerns.

**Test files next to the code:** `user.go` + `user_test.go` in the same directory. Test files
can be in `package foo` (white-box tests) or `package foo_test` (black-box tests only
exported API). Prefer black-box when testing the public interface; white-box only when you
need to test an unexported helper directly.

**Generated code** in its own file, marked with a header comment: `// Code generated by X.
DO NOT EDIT.`. Check generated code in so it's reviewable, but keep the generator in a
`//go:generate` directive for reproducibility.

**`doc.go`** holds the package's doc comment:

```go
// Package userdb provides a persistence layer for user accounts.
// ... more description ...
package userdb
```

## 12. Go proverbs the short list

From Rob Pike's 2015 talk, these have aged well:

- **Don't communicate by sharing memory; share memory by communicating.** (CSP, channels.)
- **Concurrency is not parallelism.** (Concurrency is a design tool; parallelism is a
  performance outcome.)
- **Channels orchestrate; mutexes serialize.** (Different tools, different jobs.)
- **The bigger the interface, the weaker the abstraction.** (Small interfaces rule.)
- **Make the zero value useful.** (No constructor when possible.)
- **`interface{}` says nothing.** (Use generics or concrete types.)
- **A little copying is better than a little dependency.** (Avoid adding a dependency for a
  20-line helper.)
- **Clear is better than clever.** (Go style is deliberately boring. Lean in.)
- **Errors are values.** (Treat them as data, not exceptions.)
- **Don't just check errors, handle them gracefully.** (Wrapping with context is the minimum.)
- **Design the architecture, name the components, document the details.** (In that order.)
- **Documentation is for users.** (Write godoc for the reader, not yourself.)

These sit above any specific rule. When a style choice is unclear, check against the
proverbs most questions answer themselves.
