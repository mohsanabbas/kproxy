# Security Policy

`kproxy` is a private project. This document records the project's security
posture, the threat model the implementation assumes, and how to report a
vulnerability.

## Reporting a vulnerability

**Do not** open a public issue or push a commit that demonstrates the
vulnerability. Instead, contact the repository owner directly via private
channel (email or DM) with:

- a description of the issue,
- reproduction steps,
- the commit hash you reproduced against,
- the impact you believe it has.

You will receive an acknowledgement and a remediation plan.

## Supported versions

| Branch | Supported |
| ------ | --------- |
| `main` | yes       |
| any other | no     |

## Threat model

`kproxy` is intended to run **inside a trusted network**, in front of an
Apache Kafka cluster. The threat model assumes:

| Asset / threat                                  | Mitigation                                                                                                  |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Untrusted client connections on the listener    | Bounded `-conn-limit`, bounded frame size (`frame.MaxFrameSize`), per-iteration read deadline (`-idle`).    |
| Malformed Kafka frames                          | `internal/frame` enforces `length` bounds; `internal/kwire` decoder returns `truncated input` and the conn is closed. |
| Memory exhaustion via never-replied requests    | `Tracker.PendingMaxAge` evicts stale entries (default 5 min); `MaxInflight` caps tracker size.              |
| Unbounded planner queue                         | `-planner-queue` caps work in flight; full queue triggers passthrough fallback, never blocks the hot path.  |
| Pprof / metrics exposed publicly                | Admin server defaults to `127.0.0.1:9099`. Bind to public addresses **only** behind a trusted LB.           |
| Side-channel `kclient` to broker                | One outbound TCP conn; `dial-timeout` bounds connect; failure degrades to passthrough, never crashes proxy. |
| Sensitive data in logs                          | The proxy logs only metadata (api key, version, conn counts). It never logs payload bytes. (Hex-dumping is gated behind decode-error paths only.) |

### Out of scope

- TLS termination — kproxy currently speaks **PLAINTEXT** Kafka only. TLS
  must be terminated above the proxy (service mesh, NLB, etc.).
- SASL authentication — kproxy does not authenticate the client to the
  broker; it forwards SASL frames as opaque bytes if attempted.
- Multi-tenant authorisation — broker ACLs still apply; kproxy does not
  perform AuthZ.
- Denial-of-service from the broker side — a malicious broker is out of
  scope.

## Secure defaults

| Surface                 | Default                            |
| ----------------------- | ---------------------------------- |
| `-listen`               | `:9092` (operator must override)   |
| `-admin`                | `127.0.0.1:9099` (loopback only)   |
| `-conn-limit`           | `4096`                             |
| `-idle`                 | `5m` per-iteration read deadline   |
| `-plan-timeout`         | `2s`                               |
| `-drain-timeout`        | `30s`                              |
| `-subscription-cap`     | `100000` (across all groups)       |
| Pending max-age         | `5m`                               |

## Deploying behind a public address

If you must expose `kproxy` to traffic from outside the trusted network:

1. Terminate TLS in front of the proxy. Pin client certificates if possible.
2. Keep `-admin` bound to loopback (`127.0.0.1:...`) and reach it via SSH
   port-forward or a sidecar sidecar exporter — never expose `pprof` to the
   public internet.
3. Set `-conn-limit` to a value matching your fleet's expected fan-in.
4. Run kproxy as a non-root user in a hardened container image
   (e.g. `gcr.io/distroless/static`).

## Dependencies

The proxy binary (`cmd/kproxy`) has **zero non-stdlib runtime dependencies**.
Test dependencies are limited to:

- `github.com/twmb/franz-go` (used only by `example/` test rigs, not by
  the proxy).

`govulncheck ./...` runs in CI on every PR and must pass before merge.

## Build provenance

- Reproducible builds: pin Go version in `go.mod` and `.github/workflows/`.
- Container images (when published) should carry SBOM via `syft` and be
  signed via `cosign keyless` against the workflow OIDC identity.

## Cryptographic boundaries

`kproxy` performs **no cryptography** today. It does not generate keys,
sign messages, or encrypt traffic. This is by design — TLS belongs above
the proxy.

If a future change introduces TLS termination or SASL pass-through, this
document and the threat model must be updated **before** the change is
merged.
