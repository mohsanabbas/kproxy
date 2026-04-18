# kproxy — Technical Overview, Use Cases & Local Setup

## What is kproxy?

kproxy is a **transparent TCP sidecar proxy** that sits between Kafka consumer
clients and a Kafka broker. It operates at the Kafka wire-protocol level,
forwarding all traffic byte-for-byte except for one targeted intercept: the
**SyncGroup leader request** that is sent during a consumer group rebalance.

When a rebalance happens, Kafka elects one consumer in the group as the
*leader*. The leader is responsible for proposing which partitions each member
gets. kproxy intercepts that proposal, discards it, and computes a better one
— one that accounts for each consumer's actual processing lag and message
latency, read live from Redis.

Everything else (produce, fetch, metadata, heartbeats, offset commits) flows
through unchanged. Consumers and producers do not know kproxy exists.

```
consumers ──► kproxy :9093 ──► Kafka broker :9092
                  │
                  └──► assign.Engine ◄── telemetry.RedisFeeder ◄── Redis
```

---

## The problem it solves

Kafka's built-in partition assignors (range, round-robin, sticky) distribute
partitions evenly by **count**, not by **processing capacity**. This causes
predictable problems in production:

| Situation | What Kafka does | What happens |
|---|---|---|
| One consumer is slow (GC pause, DB contention, slow downstream) | Keeps its existing partitions | That consumer's lag grows unbounded while others sit idle |
| One consumer has higher message latency | Equal partition count | It falls further and further behind |
| After a crash+restart, a consumer rejoins cold | Gets the same share back immediately | It floods itself before warming up |

The result is **uneven lag across the group**, which translates to higher
end-to-end latency and increased risk of consumer-side timeouts or
re-processing.

kproxy fixes this by weighting the assignment: consumers with lower lag and
lower latency receive more partitions; consumers that are already struggling
receive fewer. Every partition is always assigned — no work is lost.

---

## Use cases

### 1. Latency-sensitive event pipelines
Groups processing payment authorisations, fraud signals, or real-time
recommendations where tail latency matters. A single slow consumer dragging
the group average down is unacceptable.

### 2. Mixed-capacity consumer fleets
When consumers run on heterogeneous hardware (e.g., a mix of VM sizes in
Kubernetes) or when JVM-based consumers have unpredictable GC pause profiles,
a count-based assignment is a poor fit.

### 3. Consumer restarts and rolling deploys
A newly started consumer has zero lag but also zero warm cache. kproxy
can be extended to give it a lighter initial assignment while it warms up,
without any changes to the consumers themselves.

### 4. Multi-tenant topic sharing
Several teams sharing a single high-throughput topic with consumers that have
different downstream latency profiles. Without kproxy, the slowest team's
consumer determines the group's overall lag.

### 5. Zero-code consumer instrumentation
Consumer teams do not need to adopt a custom assignor or change their Kafka
client configuration. They point at kproxy instead of the broker and get
adaptive assignment for free.

---

## Business value

| Value | Mechanism |
|---|---|
| **Lower end-to-end latency** | Partitions flow toward consumers that can actually process them fast |
| **More predictable SLAs** | Lag stays bounded even when individual consumers slow down temporarily |
| **No application changes** | Transparent proxy — consumers change one config line (`bootstrap.servers`) |
| **Safe degradation** | If the telemetry path is slow or Redis is unavailable, kproxy falls back to the original Kafka-computed assignment within 500 ms; rebalances never stall |
| **Decoupled telemetry** | Any process that can write a JSON blob to Redis can feed the system — existing monitoring agents, consumer sidecars, or dedicated lag exporters |

---

## Architecture in one page

```
┌──────────────────────────────────────────────────────────┐
│  cmd/kproxy/main.go                                      │
│  · parses flags                                          │
│  · wires Engine, RedisFeeder, Proxy together             │
│  · handles SIGINT / SIGTERM                              │
└────────────┬─────────────────────────────────────────────┘
             │
     ┌───────▼────────┐          ┌──────────────────────┐
     │ internal/proxy │◄─────────│ internal/assign      │
     │                │ PlanReq  │                      │
     │ · TCP listener │─────────►│ · Engine (actor)     │
     │ · frame reader │  Plan    │ · computePlan (pure) │
     │ · SyncGroup    │◄─────────│                      │
     │   rewriter     │          └──────────┬───────────┘
     └────────────────┘                     │ Snapshot
                                   ┌────────▼──────────┐
                                   │ internal/telemetry │
                                   │                    │
                                   │ · RedisFeeder      │
                                   │ · scans keys every │
                                   │   -refresh         │
                                   └────────────────────┘
```

**Concurrency guarantees (no mutexes, no atomics):**

- Each TCP connection owns its goroutines exclusively — no shared state across
  connections.
- `assign.Engine` is a single goroutine that owns the telemetry snapshot.
  All communication with it is via channels.
- The feeder→engine snapshot channel is buffered size 1, so stale snapshots
  are coalesced rather than queued.

---

## Telemetry schema

Any process writes a JSON blob to Redis using this key pattern:

```
SET kproxy:telemetry:<group>:<memberID> \
    '{"group":"orders-group","member":"consumer-7","lag":4230,"latency":0.015}'
```

| Field | Type | Meaning |
|---|---|---|
| `group` | string | Kafka consumer group name |
| `member` | string | Kafka member ID (`clientId + "-" + instanceId` or similar) |
| `lag` | float | Uncommitted messages (sum across assigned partitions) |
| `latency` | float | Seconds per message, rolling average |

kproxy scans `kproxy:telemetry:*` on every `-refresh` tick. Keys can be
written by the consumers themselves, by a Kafka lag exporter, or by any
monitoring agent.

---

## Running locally

### Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Go | 1.26+ | Build kproxy |
| Docker + Docker Compose | any recent | Run Kafka + Redis locally |

### 1. Clone and verify the build

```bash
git clone https://github.com/mohsan/kproxy
cd kproxy
go test ./...
```

Expected output:

```
ok  github.com/mohsan/kproxy/internal/assign   0.3s
```

### 2. Start the local stack

Create `docker-compose.yml` at the repo root:

```yaml
services:
  kafka:
    image: apache/kafka:3.9.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
```

```bash
docker compose up -d
```

Wait ~10 seconds for Kafka to finish its KRaft initialisation, then confirm:

```bash
docker compose ps          # both services should be "running"
docker compose logs kafka  # look for "Kafka Server started"
```

### 3. Create a test topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic orders --partitions 8 --replication-factor 1
```

### 4. Start kproxy

kproxy listens on `:9093` by default and forwards to the broker on `:9092`.
Note: use a different listen port from the controller listener inside the
container; the ports here are on your host machine.

```bash
go run ./cmd/kproxy \
    -listen  :19093 \
    -broker  localhost:9092 \
    -redis   localhost:6379 \
    -refresh 2s
```

You should see:

```json
{"time":"...","level":"INFO","msg":"listening","addr":":19093","broker":"localhost:9092"}
```

### 5. Point a consumer at kproxy

Any Kafka client that supports `bootstrap.servers` works. Using the bundled
Kafka console tools:

```bash
# Producer — talks directly to the broker (no interception needed for produce)
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic orders

# Consumer — talks through kproxy
kafka-console-consumer \
    --bootstrap-server localhost:19093 \
    --topic orders \
    --group orders-group \
    --from-beginning
```

> **Tip:** Open two or three consumer terminals in the same group. Each time
> you start or stop one, a rebalance triggers and you will see kproxy log a
> `rewrote SyncGroup` line.

### 6. Inject synthetic telemetry

Push fake lag/latency data into Redis to see kproxy bias the assignment:

```bash
# consumer-1 is healthy
redis-cli SET kproxy:telemetry:orders-group:consumer-1 \
    '{"group":"orders-group","member":"consumer-1","lag":10,"latency":0.001}'

# consumer-2 is lagging badly
redis-cli SET kproxy:telemetry:orders-group:consumer-2 \
    '{"group":"orders-group","member":"consumer-2","lag":95000,"latency":3.5}'
```

Trigger a rebalance (start or stop a consumer), then inspect which partitions
each member received. `consumer-1` will hold the majority.

### 7. Tear down

```bash
docker compose down -v
```

---

## Configuration reference

| Flag | Default | Description |
|---|---|---|
| `-listen` | `:9093` | Address kproxy binds on (Kafka clients connect here) |
| `-broker` | `localhost:9092` | Upstream Kafka broker address |
| `-redis` | `localhost:6379` | Redis server for telemetry reads |
| `-refresh` | `2s` | How often to poll Redis for updated telemetry |

---

## Known limitations

- **Single broker only.** kproxy acts as a proxy to one broker address.
  Multi-broker clusters work because Kafka clients re-connect to the correct
  broker after the metadata response, but kproxy must be in the path for the
  coordinator connection specifically. In practice, set `-broker` to the
  group coordinator's address or use a load balancer in front of the brokers.
- **KIP-482 flex headers** are handled for SyncGroup v4+, but only lightly
  tested end-to-end.
- **Cooperative-sticky rebalances** are supported at the protocol level
  (UserData is preserved), but the planner does not yet minimise partition
  movement across rebalances.
- **No TLS.** The proxy speaks plain TCP. TLS termination should be handled
  by an upstream load balancer.
