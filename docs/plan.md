
Reviewed code and provided engineering critique
kproxy — Engineering Review & Production-Grade Redesign
A frank review of the current prototype, the design flaws hiding inside it, and a proposed architecture that is dependency-free at the consumer side, broker-agnostic, and suitable for production.

1. What works in the current design
Before tearing it down, the parts that are actually right:

Wire-level interception of SyncGroup is the correct integration point. It's the only place in the Kafka protocol where assignment is decided and is the same across every client library (Java, librdkafka, Sarama, franz-go, kafka-python, node-rdkafka, …). This is the central insight and it's correct.
Actor-based assign.Engine with channel boundaries is a sound concurrency model.
Best-effort fallback to original bytes on timeout is the right safety posture — never stall a rebalance.
Pure computePlan with table-driven tests is the right shape for the domain core.
Everything below is what's wrong, what's missing, and how to fix it.

2. Critical design flaws (correctness)
2.1 The proxy is not actually transparent — it breaks multi-broker clusters
The Proxy.handle function dials a single -broker address for every accepted connection. This only works for single-broker clusters.

In a real Kafka cluster:

Client opens TCP connection → asks for Metadata.
Broker responds with the full broker list (host:port pairs).
Client opens new TCP connections directly to each broker for partition leadership, group coordinator, etc.
Those subsequent connections bypass kproxy entirely unless every broker is also fronted by kproxy and the metadata response is rewritten to advertise kproxy's addresses.

Fix: kproxy must run as one instance per broker, and must rewrite MetadataResponse and FindCoordinatorResponse to substitute its own host:port for each BrokerId. This is a dedicated subsystem (call it topology).

2.2 The downstream pump uses io.Copy — responses are not parsed

go func() {    _, err := io.Copy(client, upstream)   // ← opaque byte pump    done <- err}()
This means kproxy cannot:

Rewrite MetadataResponse (required for §2.1).
Detect when the broker rejects our rewritten SyncGroup with ILLEGAL_GENERATION / REBALANCE_IN_PROGRESS and react.
Implement KIP-848 (the new consumer rebalance protocol) at all — there is no SyncGroup in KIP-848; assignment is broker-driven via ConsumerGroupHeartbeat.
Fix: both directions must be framed and parsed.

2.3 Header version table is hardcoded and will silently rot
requestHeaderVersion in intercept.go hardcodes the flex-version threshold per API key. New Kafka releases add APIs and bump flex versions — this table is a maintenance landmine.

Fix: either depend on a maintained table (kmsg's internal RequestHeaderVersion is now exported in newer versions; or generate from MessageDef JSON shipped with Kafka), or — better — only parse the API key + version, and use a real codec library only for the SyncGroup body.

2.4 Subscription detection is structurally wrong
membersFromSyncGroup infers each member's subscription from the topics in the leader's proposed assignment:


for _, t := range ma.Topics {    topics = append(topics, t.Topic)}out = append(out, assign.Member{ID: a.MemberID, Topics: topics})
But the proposed assignment is already filtered by the leader's intended distribution — it's not the subscription. A member subscribed to [A, B] whose leader-proposed assignment only contains A partitions will be wrongly recorded as subscribed to [A] only. We will then refuse to give it B partitions even when we should.

Fix: the actual subscription lives in the JoinGroup request's protocolMetadata (encoded ConsumerProtocolSubscription). kproxy must observe JoinGroup requests and remember each member's subscription, keyed by (group, generation, memberId). This is the only piece of cross-frame state we genuinely need.

2.5 Plan is computed without partition ownership history
computePlan is stateless. Every rebalance moves partitions freely. For cooperative-sticky and KIP-848 this is catastrophic:

Cooperative-sticky requires the assignment to converge across two rebalances; arbitrary moves cause infinite revocation cycles.
Even for eager assignors, partition movement implies state loss for stateful consumers (in-memory caches, DB connections to specific shards).
Fix: the planner needs previous Plan as input. The UserData blob in ConsumerProtocolSubscription already carries each member's currently owned partitions — decode it, feed it into the planner, and minimise movement subject to the load-balance constraint.

2.6 Frame buffer is aliased across calls

// Next returns the next frame's payload. The returned slice aliases the// reader's internal buffer and is invalidated by the next call.func (f *frameReader) Next() ([]byte, error) { ... }
Then in maybeIntercept, the body is passed to kmsg's ReadFrom which may retain references (most kmsg types do not copy []byte fields like UserData or MemberAssignment). If we then call Next() again before the rewritten frame is fully serialized and sent, those slices point at the new frame's content.

Currently it works only because we serialize before reading the next frame. But this is a footgun waiting to bite anyone who refactors.

Fix: either copy on decode, or use a clearly documented one-frame-at-a-time invariant enforced by the type system (e.g. a Frame value type that owns its bytes).

2.7 No backpressure on the engine
Engine.handle calls e.parts(ctx, req.Topics) — a synchronous network call to the broker for metadata — inline on the engine goroutine. While it's running, no other plan request and no telemetry refresh is processed. With multiple groups rebalancing at once, this serialises all of them behind one another.

Fix: the engine should hand each request to a worker pool with a snapshot copy of the telemetry. The engine goroutine itself only owns snapshot mutation.

2.8 No metadata cache
Every plan request fetches metadata from the broker. Metadata is high-volume, slow to fetch under load, and changes rarely. Hammering the broker on every rebalance is unnecessary and adds 50–500 ms of latency to a critical path with a 500 ms budget.

Fix: dedicated metadata cache (TTL 30–60 s, refreshed in background, invalidated on UNKNOWN_TOPIC_OR_PARTITION errors).

2.9 No back-channel observability of our own decisions
We log "rewrote SyncGroup" and that's it. We don't know:

Whether the broker accepted the rewritten assignment (look at SyncGroupResponse.ErrorCode).
How long the rebalance took end-to-end.
The actual lag delta after our rewrite vs. what the leader proposed.
Fix: parse responses, emit metrics (Prometheus), and emit a structured "rebalance decision" event including before/after partition distribution and predicted vs. actual lag.

2.10 The plan algorithm has a real edge case
pickNeediest ties break to the lowest index. With identical capacities and partitions in sorted order, member 0 always gets the first partition, member 1 the second, etc. — which is fine. But when capacities are zero (the total == 0 path) and there are 7 partitions across 3 members, the first member gets ceil(7/3)=3, others get 2 each — fine. However when partitions < members, some members get zero partitions and we have no protection against that. The README acknowledges this but the algorithm has no fix.

Fix: two-pass — minimum 1 partition per subscribed member when possible, then capacity-weighted distribution of the remainder.

3. Operational flaws
3.1 No graceful shutdown
Proxy.Serve waits for wg, but in-flight pumpUpstream goroutines block on Read from the client socket forever (no read deadline, ctx cancellation does not propagate to syscall blocking reads). The process will hang on SIGTERM until clients disconnect.

Fix: set per-iteration read deadlines, check ctx, close the conn on shutdown.

3.2 No connection limits / no rate limits
A misbehaving client can open unbounded TCP connections; each allocates a 16 MB max frame buffer on demand. kproxy has no DoS protection.

Fix: golang.org/x/net/netutil.LimitListener, plus per-IP connection limits and a configurable max-frame-size cap below 16 MiB for non-produce/fetch.

3.3 No authentication awareness
SASL handshake frames (SaslHandshake, SaslAuthenticate) flow through unchanged, which is correct — but kproxy never learns who the authenticated principal is, and so cannot enforce ACLs or attribute decisions in audit logs.

Fix: parse SaslAuthenticate to extract the principal; attach it to per-connection context; emit it in audit/metrics.

3.4 No TLS termination support
The README mentions "no TLS" as a limitation. In real environments, Kafka broker connections use SASL_SSL or mTLS. kproxy must either:

Terminate TLS itself and re-originate TLS to the broker (most flexible, breaks SASL_SCRAM channel binding), or
Be deployed in TLS-passthrough mode behind another LB — but then it cannot read the protocol.
Fix: TLS termination is mandatory for production. mTLS to broker, configurable cert sources (file, SPIFFE, ACM).

3.5 Single point of failure
One kproxy process per broker is fine, but kproxy itself has no HA story. If kproxy dies mid-rebalance, that group hangs until the broker times the SyncGroup out (~30s) and re-elects.

Fix: stateless kproxy + N replicas behind a TCP LB per broker. With no per-connection persistent state outside the JoinGroup→SyncGroup pair (a few seconds), a connection-level LB hash gives sticky enough routing.

3.6 No circuit breaker on Redis
If Redis hangs (not down — hangs), the feeder's Get calls block. The feeder is on its own goroutine so the engine still works with stale data, but read can pile up if scans are slow and Redis is slow. There's no per-call timeout.

Fix: per-Redis-call context timeout, circuit breaker, and a "telemetry health" gauge.

3.7 Telemetry trust model is broken
Any process that can write to Redis can lie about its lag and capture more partitions. There's no signing, no source verification.

Fix: for production, telemetry must come from a trusted source (the broker's __consumer_offsets topic plus a partition-leader-side latency hint), not from the consumers themselves.

4. Dependency / portability flaws
4.1 kmsg dependency leaks into the proxy
proxy imports github.com/twmb/franz-go/pkg/kmsg for codec. franz-go is excellent but it's:

A heavy dependency.
Tied to one author's release cadence.
Not the canonical Kafka schema source.
4.2 kgo dependency for metadata RPCs
main.go uses kgo.NewClient purely to issue MetadataRequest. This pulls in the entire franz-go consumer/producer machinery for one RPC.

Fix: issue Metadata directly using our own codec — it's a 50-line function.

4.3 Redis dependency is gratuitous
go-redis/v9 is fine, but Redis itself is the wrong substrate. See §6.

5. The redesigned architecture

┌─────────────────────────────────────────────────────────────────┐│ kproxy (stateless, N replicas per broker)                       ││                                                                  ││  ┌──────────┐   ┌────────────┐   ┌─────────────┐                ││  │ listener │──►│ connection │──►│   codec     │                ││  │ (TLS)    │   │  manager   │   │ (gen'd from │                ││  └──────────┘   └─────┬──────┘   │   schema)   │                ││                       │          └──────┬──────┘                ││                       ▼                 │                       ││             ┌──────────────────┐        │                       ││             │ frame router     │◄───────┘                       ││             │ - JoinGroup      │                                ││             │ - SyncGroup      │                                ││             │ - Metadata       │                                ││             │ - Heartbeat KIP848                                ││             │ - everything else: passthrough                    ││             └─────┬───────┬────┘                                ││         intercept │       │ rewrite                             ││                   ▼       ▼                                     ││          ┌──────────────────┐    ┌──────────────────┐           ││          │ subscription     │    │ topology         │           ││          │ store (per-conn) │    │ rewriter         │           ││          └────────┬─────────┘    │ (broker map)     │           ││                   │              └──────────────────┘           ││                   ▼                                             ││          ┌────────────────────────────────────────────┐         ││          │ planner (worker pool)                      │         ││          │ ┌──────────┐ ┌──────────────┐ ┌──────────┐│         ││          │ │ sticky   │ │ capacity-    │ │ feasibility│         ││          │ │ baseline │ │ weighted     │ │ check     ││         ││          │ └──────────┘ └──────────────┘ └──────────┘│         ││          └─────┬─────────────────┬────────────────────┘         ││                │ reads           │ reads                        ││                ▼                 ▼                              ││         ┌─────────────┐   ┌──────────────┐                      ││         │ telemetry   │   │ metadata     │                      ││         │ snapshot    │   │ cache        │                      ││         │ (immutable) │   │ (TTL 30s)    │                      ││         └──────┬──────┘   └──────┬───────┘                      ││                │                 │                              ││                │                 ▼                              ││                │          ┌─────────────┐                       ││                │          │ broker pool │ ──► Kafka brokers     ││                │          └─────────────┘                       ││                ▼                                                ││        ┌────────────────────────────────────────┐               ││        │ telemetry source (pluggable)           │               ││        │ ─ kafka-builtin (default, dep-free)    │ ──► Kafka     ││        │ ─ redis-source                         │ ──► Redis     ││        │ ─ prometheus-source                    │ ──► Prom      ││        │ ─ otel-source                          │ ──► OTLP      ││        └────────────────────────────────────────┘               ││                                                                  ││  ┌──────────────────────────────────────────────────────────┐   ││  │ observability: prom metrics, structured logs, audit trail│   ││  └──────────────────────────────────────────────────────────┘   │└─────────────────────────────────────────────────────────────────┘
5.1 Component breakdown
Component	Responsibility	Boundary
listener	TLS termination, accept, conn limits	net.Listener wrap
connection manager	per-conn state (auth principal, subscription cache)	one struct per TCP conn
codec	parse/serialize Kafka frames	generated from Apache Kafka's clients/src/main/resources/common/message/*.json
frame router	dispatch by API key	switch with metrics per key
subscription store	per-conn cache of (group, memberId) → subscription	bounded map, evicted on conn close
topology rewriter	rewrite MetadataResponse and FindCoordinatorResponse to advertise kproxy addresses	pure function
planner	worker pool, sticky+capacity+feasibility passes	bounded goroutines
telemetry snapshot	immutable, atomically swapped via atomic.Pointer[Snapshot]	one writer, many readers
metadata cache	TTL'd topic→partitions map, background refresh	separate goroutine
telemetry source	interface; built-in implementations include Kafka-native (no Redis)	strategy pattern
broker pool	persistent connections to upstream brokers for metadata + admin	connection pool
observability	Prometheus metrics, OTel traces around plan decisions	side-effect only
5.2 Data ownership rules (formalised)
Every snapshot is immutable after construction. Updates create a new snapshot and atomically replace the pointer. Readers always see a consistent view — no copy needed, no lock needed.
Per-connection state never escapes the connection's goroutines.
The planner takes inputs by value and returns outputs by value. No shared mutable state.
This is stricter than the current actor model and easier to reason about: it removes the actor goroutine as a serialisation bottleneck.

6. The right substrate: not Redis
Redis was chosen because it's familiar. It is the wrong choice for this workload:

Concern	Redis	Better choice
Source of lag truth	Anything writes anything	__consumer_offsets topic IS the lag truth
Source of latency truth	Self-reported, untrusted	Broker-side request latency histogram (KIP-714 client telemetry)
Operational burden	Another stateful service	Already have Kafka
Trust model	None	Consumers can't lie about offsets they don't own
Recommended default: Kafka-native telemetry

                        ┌──────────────────────────────────┐                        │ kproxy: telemetry source         │                        │                                  │__consumer_offsets ────►│  · derives lag = HWM - committed │                        │  · per (group, partition)        │__kproxy_metrics    ───►│  · derives latency from KIP-714  │(compacted topic)       │    client-reported metrics       │                        └──────────────────────────────────┘
Lag comes from joining __consumer_offsets (committed offsets per group, partition) with the partition high-water mark from MetadataResponse. This is the only correct lag number; everything else is an estimate.
Latency comes from KIP-714 client telemetry (which Kafka 3.7+ supports natively). Consumers push their own latency histograms via the PushTelemetry RPC; kproxy can subscribe.
This makes kproxy:

Dependency-free at the consumer side — no agent to install, no Redis writes.
Broker-version-portable — only requires Kafka 3.7+ for KIP-714, and falls back to lag-only weighting on older brokers.
Trustworthy — consumers can't fabricate their telemetry.
Keep Redis as one of several pluggable sources

type Source interface {    // Subscribe returns a stream of immutable snapshots.    Subscribe(ctx context.Context) <-chan *Snapshot}
Implementations: KafkaNativeSource (default), RedisSource (for environments where consumers already export to Redis), PromSource, NoopSource (for testing).

7. Broker-version & client-library portability
Kafka feature	kproxy behaviour
Classic eager rebalance (≤2.4)	Full intercept
Cooperative-sticky (KIP-429, ≥2.4)	Intercept + sticky pass that decodes UserData
KIP-848 (≥3.7, broker-driven assignment)	Different intercept point: ConsumerGroupHeartbeatResponse carries the assignment; kproxy must rewrite the response from the broker. This is why §2.2 (parse the downstream side) is non-negotiable.
Static membership (KIP-345)	Track by instanceId instead of ephemeral memberId
Client libraries: because kproxy operates at the wire protocol, every client works without modification: Java, librdkafka (C/C++/Python/Node/.NET), Sarama (Go), franz-go (Go), kafka-python, aiokafka, segmentio/kafka-go, ruby-kafka, snuba, etc.

The one gap: clients using the new KIP-848 protocol require kproxy to support that codepath in addition to legacy. Both must be implemented for forward compatibility.

8. Coding maturity gaps to fix
Area	Current state	Target
Tests	One package (assign) covered with table-driven tests; rest untested	≥80% line coverage, integration tests against ephemeral Kafka via testcontainers, fuzz tests on the codec
Linting	None	golangci-lint with revive, staticcheck, errcheck, gosec, bodyclose
Vulnerability scanning	None	govulncheck in CI
Race detector	Not run	go test -race mandatory in CI
Benchmarks	None	go test -bench for codec hot paths and planner
Profiling endpoints	None	pprof over admin port (separate listener, not exposed publicly)
Metrics	None	Prometheus: kproxy_frames_total{api}, kproxy_intercept_decision_total{outcome}, kproxy_plan_duration_seconds, kproxy_telemetry_age_seconds, kproxy_rebalance_outcome_total{result}
Tracing	None	OTel spans around plan decisions, with rebalance correlation IDs
Logging	slog ✓	Add request IDs, structured fields, sampling
Config	Flags only	Flags + env + file, with validation; document every knob
Errors	Mix of wrapping and ad-hoc	Sentinel errors for known categories; errors.Is/As everywhere
Resource cleanup	Some _ = x.Close() ignored	Explicit error logging on close failures
Graceful shutdown	Broken (§3.1)	Drain in-flight, connection deadline on SIGTERM
CI/CD	None visible	GitHub Actions: build, test, race, vet, lint, govulncheck, container build, SBOM
Containerisation	None	Distroless container, ~15 MB
Deployment	None	Helm chart, sidecar mode (DaemonSet) and gateway mode (Deployment)
9. Trade-offs to make explicit
Decision	Pros	Cons
Wire-level proxy vs. custom assignor library	Works for every client, no consumer changes, broker-agnostic	Must track Kafka protocol evolution forever; one more network hop
Stateless replicas vs. single instance	HA, horizontally scalable	Lose subscription cache on restart; first rebalance after a consumer reconnect to a different replica is suboptimal
Kafka-native telemetry vs. Redis	No new dependency, trustworthy, accurate	Harder to extend with custom signals (e.g., GC pause time)
Sticky-first vs. capacity-first planner	Stable assignments, low partition movement	Slower to converge to optimal load balance
TLS termination vs. passthrough	Can read protocol, can rewrite	Channel binding for SASL_SCRAM breaks; cert management overhead
Per-broker deployment vs. cluster-wide	Simple routing, no metadata rewriting needed for inter-broker	One kproxy per broker means N replicas to manage
Custom codec vs. kmsg dependency	Smaller binary, no upstream cadence dependence	We own the schema-tracking burden
10. The shortest path to production
If I had to ship this in one quarter, in order:

Replace io.Copy with framed pumps in both directions. Without this nothing else matters. (~1 week)
Add subscription tracking via JoinGroup parsing. Fix §2.4. (~3 days)
Add metadata rewriting and per-broker deployment model. Fix §2.1. (~1 week)
Add metadata cache and detach planner from engine. Fix §2.7, §2.8. (~3 days)
Replace ad-hoc plan with sticky+capacity+feasibility planner. Fix §2.5, §2.10. (~1 week)
Add Kafka-native telemetry source as default; keep Redis as plugin. §6. (~2 weeks)
Observability: Prometheus + OTel + structured rebalance events. §3.7, §2.9. (~1 week)
TLS termination + SASL principal extraction. §3.3, §3.4. (~1 week)
Graceful shutdown, conn limits, rate limits. §3.1, §3.2. (~3 days)
CI: race, vet, lint, govulncheck, integration tests against testcontainers Kafka. §8. (~1 week)
KIP-848 ConsumerGroupHeartbeat rewrite path. §7. (~2 weeks)
Total: ~3 months for a production-grade v1 by one engineer; faster with two.

11. The single most important decision
Everything above is execution. The one architectural choice that makes or breaks this product:

Telemetry must come from Kafka itself, not from a side channel.

Side-channel telemetry (Redis, Prometheus pull, custom agents) creates an integration tax on every consumer team and a trust problem on every assignment. Kafka 3.7+ already exposes everything we need:

Lag → __consumer_offsets + MetadataResponse HWM
Latency → KIP-714 PushTelemetry
Membership → JoinGroup / ConsumerGroupHeartbeat (which we already see)
A kproxy that reads only from Kafka and writes nothing outside Kafka is:

Zero-dependency for adopters.
Universal across every client library and broker version (with graceful degradation for older brokers).
Trustworthy — the broker is the source of truth, not the workload.
That's the version worth shipping.
