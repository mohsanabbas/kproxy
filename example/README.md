# kproxy demo

End-to-end demo of the Kafka rebalance proxy against a local docker-compose
Kafka, exercising the production edge cases:

- Topology rewriting (Metadata + FindCoordinator) so consumers reconnect
  through the proxy after the second hop, not directly to the broker.
- Cooperative-sticky rebalance protocol (the franz-go default for this demo).
- SyncGroup leader assignment intercept: the proxy's planner replaces the
  leader's `Assignments[]` upstream so every member receives the planned
  partition layout in their own SyncGroup response.
- Lag-aware planning: the telemetry poller reads `__consumer_offsets` natively
  and feeds per-member lag into the planner score function.
- Graceful shutdown: SIGTERM commits offsets and leaves the group; the
  remaining members rebalance via the proxy.
- Crash simulation: a consumer process is killed mid-flight; the broker
  detects session timeout and the proxy plans a new assignment.
- Operational surface: `/metrics` (Prometheus), `/healthz`, `/debug/pprof/*`
  on the admin port.

The example lives in its own Go module so the kproxy core stays std-lib only.
It depends on [franz-go](https://github.com/twmb/franz-go) for the Kafka
client and shells out to the compiled `cmd/kproxy` binary (the production
deployment model is sidecar-process, not embedded library).

## Layout

```
example/
├── go.mod                   # separate module, depends on franz-go
├── producer/main.go         # steady event producer
├── consumer/main.go         # cooperative-sticky consumer with edge-case flags
└── scenario/main.go         # spawns kproxy + N consumers + kill-one-mid-flight
```

## Prerequisites

- Docker (for the Kafka broker)
- Go 1.26+

## One-time setup

```bash
# 1. Start Kafka + create demo topics
docker compose up -d kafka kafka-init kafka-ui
# kafka-ui is on http://localhost:8080

# 2. Build the kproxy binary
make build              # produces bin/kproxy (or `go build -o bin/kproxy ./cmd/kproxy`)
```

## Demo 1 — Manual three-terminal walkthrough

The simplest demo: kproxy, producer, consumer, each in its own terminal.

```bash
# Terminal 1 — kproxy
./bin/kproxy \
  -broker localhost:9094 \
  -listen 127.0.0.1:19094 \
  -admin  127.0.0.1:9099 \
  -topology 1=localhost:9094=127.0.0.1:19094 \
  -telemetry 5s -refresh 10s

# Terminal 2 — producer (talks to kproxy, not the broker)
cd example && go run ./producer \
  -brokers 127.0.0.1:19094 \
  -topic event-tracking_track-events-approved \
  -rate 200

# Terminal 3 — consumer
cd example && go run ./consumer \
  -brokers 127.0.0.1:19094 \
  -group  demo-group \
  -topic  event-tracking_track-events-approved
```

Watch the `[c?]` log lines: the consumer's assignment grows on join.
Inspect proxy state at any time:

```bash
curl -s http://127.0.0.1:9099/metrics | grep ^kproxy
curl -s http://127.0.0.1:9099/healthz
```

### Trigger a rebalance

Open a second consumer terminal with the same `-group`. The proxy intercepts
the JoinGroup, captures the new subscription, and on the leader's SyncGroup
re-plans assignments. Both consumers should end up with disjoint partition
sets summing to the full topic.

### Demonstrate lag-driven planning

Add `-slow 50ms` to one consumer to make it artificially slow. After the
telemetry poll picks up the lag, the next rebalance shifts partitions toward
the faster consumer.

```bash
go run ./consumer -brokers 127.0.0.1:19094 -group demo-group \
  -topic event-tracking_track-events-approved -slow 50ms -client-id slowpoke
```

### Demonstrate crash recovery

```bash
go run ./consumer -brokers 127.0.0.1:19094 -group demo-group \
  -topic event-tracking_track-events-approved \
  -client-id will-die -crash-after 15s
```

After 15s the consumer `os.Exit(1)`s without committing. The broker hits
session timeout (~15s, configurable), surviving members get a rebalance
through the proxy, and the unowned partitions are redistributed.

## Demo 2 — Scripted scenario

The `scenario` runner spawns kproxy + N consumers in one process and kills
one of them mid-run, all with structured logging.

```bash
docker compose up -d kafka kafka-init
make build

# Optional: keep the producer running in another terminal so there's traffic
cd example && go run ./producer -brokers 127.0.0.1:19094 -rate 300 &

# Run the scenario
cd example && go run ./scenario \
  -kproxy-bin ../bin/kproxy \
  -broker localhost:9094 \
  -listen 127.0.0.1:19094 \
  -consumers 4 \
  -duration 90s \
  -kill-after 30s
```

You'll see:

```
[scenario] kproxy ready on 127.0.0.1:19094 (admin 127.0.0.1:9099)
[scenario] [c0] +map[event-tracking_track-events-approved:[0]]
[scenario] [c1] +map[event-tracking_track-events-approved:[1]]
[scenario] [c2] +map[event-tracking_track-events-approved:[2]]
[scenario] [c3] +map[...]   # 3 partitions, 4 consumers — c3 idle
[scenario] totals: [c0=812 c1=796 c2=833 c3=0] sum=2441
           kproxy_intercepts_total{outcome="any"}=14
           kproxy_plan_count_total=3
           kproxy_unmapped_brokers_total=0
           kproxy_conn_active=4
[scenario] KILL c0 → expect rebalance
[scenario] [c0] -map[event-tracking_track-events-approved:[0]]
[scenario] [c3] +map[event-tracking_track-events-approved:[0]]
```

## Verifying topology rewrite

Without kproxy's topology rewrite, the franz-go client would issue a Metadata
request through the proxy, then dial the *real* broker for everything else,
bypassing the proxy entirely. To prove the rewrite is working:

```bash
# In a separate shell, observe TCP connections
lsof -nP -iTCP:9094  -sTCP:ESTABLISHED   # 1 conn from kproxy itself
lsof -nP -iTCP:19094 -sTCP:ESTABLISHED   # N conns from your consumers
```

Every consumer connection is on the proxy port. None go directly to 9094.

## Cleanup

```bash
docker compose down -v
```

## Edge cases this demo covers

| Edge case                                | How to trigger                                  | Where to look                                                |
|------------------------------------------|-------------------------------------------------|--------------------------------------------------------------|
| Cooperative-sticky JoinGroup intercept   | Start any consumer                              | `kproxy_intercepts_total{outcome="any"}` increments          |
| SyncGroup leader plan rewrite            | Start ≥2 consumers in same group                | `kproxy_plan_count_total` increments                         |
| Metadata host/port rewrite               | Start a consumer through the proxy              | `lsof` shows no direct broker connections from consumers     |
| FindCoordinator host/port rewrite        | Same                                            | Same                                                         |
| Lag-driven re-planning                   | Add `-slow 50ms` to one consumer                | Slow consumer loses partitions on next rebalance             |
| Graceful shutdown                        | `Ctrl-C` a consumer                             | Clean leave-group; remaining members rebalance               |
| Crash recovery                           | `-crash-after 15s` on a consumer                | Session timeout triggers rebalance via the proxy             |
| Backpressure / limits                    | Increase `-consumers` past `-conn-limit`        | New consumers wait until a slot frees                        |
| Static membership (no rebalance on bounce) | Run consumer with `-instance c0`              | Quick restart with same instance id avoids a full rebalance  |

## Troubleshooting

- **`failed to dial broker`** — Kafka isn't ready yet. `docker compose ps`,
  wait for `kwire-kafka` to be `(healthy)`.
- **Consumers hang at startup** — Topology mapping wrong. The advertised host
  in the mapping must be the host:port your consumers use as bootstrap.
- **`unmapped_brokers_total > 0`** — A broker came back in the Metadata
  response with no mapping entry; consumers will try to dial it directly.
  Add an entry to `-topology`.
