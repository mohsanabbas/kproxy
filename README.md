# kproxy

A transparent, in-protocol proxy for Apache Kafka. `kproxy` sits between
clients and brokers, speaks the Kafka wire protocol on both sides, and
intercepts a small set of group-management requests
(`Metadata`, `FindCoordinator`, `JoinGroup`, `SyncGroup`) to:

- rewrite broker advertised addresses (multi-network topologies);
- compute and inject **globally-fair partition assignments** across
  consumer groups, weighted by lag and per-broker fan-in;
- expose Prometheus metrics, pprof, and `/healthz` from a built-in
  admin server.

Everything else flows byte-for-byte unchanged. The binary is **stdlib-only
Go**, no third-party runtime dependencies.

---

## Status

Private repository. Built and tested on Go 1.26, verified end-to-end against
`confluentinc/cp-kafka:7.9.0` and `twmb/franz-go v1.18.0`.

## Quick start

```bash
# Run kafka (KRaft) locally
docker compose up -d kafka kafka-init

# Build
go build -o bin/kproxy ./cmd/kproxy

# Run kproxy in front of the local broker
./bin/kproxy \
  -broker localhost:9094 \
  -listen :19094 \
  -admin 127.0.0.1:9099 \
  -topology 1=localhost:9094=127.0.0.1:19094

# Smoke-test
curl -s http://127.0.0.1:9099/healthz
curl -s http://127.0.0.1:9099/metrics | head
```

Then point any Kafka client at `127.0.0.1:19094`. See
[`example/`](example/) for a franz-go producer, consumer and probe.

## Documentation

- [docs/overview.md](docs/overview.md) — design overview
- [docs/production-use-cases.md](docs/production-use-cases.md) — real-world use cases
- [docs/protocols-and-problems.md](docs/protocols-and-problems.md) — protocol and problem statement
- [docs/go-libraries-compatibility.md](docs/go-libraries-compatibility.md) — Go client compatibility
- [docs/plan.md](docs/plan.md) — planner internals
- [SECURITY.md](SECURITY.md) — security posture and reporting



## Development

```bash
make help     # see Makefile targets
go test -race ./...
go vet ./...
govulncheck ./...
```

## License

Private  not for external distribution. See [LICENSE](LICENSE).
