// Package obs exposes runtime observability for kproxy: metrics (expvar +
// Prometheus text format) and an admin HTTP server (metrics, pprof, healthz).
//
// Why expvar over a metrics SDK: stdlib only, zero deps, every counter is
// monotonic int64 with atomic semantics. The Prometheus encoder is a few
// dozen lines and emits the same view.
package obs

import (
	"expvar"
	"strconv"
	"sync/atomic"
)

// Metrics holds every kproxy counter/gauge. All fields use atomic ops; the
// struct is allocated once at startup and shared across all goroutines.
//
// Naming follows Prometheus conventions: snake_case, base unit suffix.
type Metrics struct {
	// Counters
	FramesClientToBroker atomic.Int64
	FramesBrokerToClient atomic.Int64
	InterceptsTotal      atomic.Int64
	InterceptsRewrote    atomic.Int64
	InterceptsPassthru   atomic.Int64
	InterceptsTimeout    atomic.Int64
	InterceptsError      atomic.Int64
	UnmappedBrokers      atomic.Int64
	PlanDurationNanosSum atomic.Int64
	PlanCount            atomic.Int64

	// Gauges
	ConnActive       atomic.Int64
	TelemetryAgeNS   atomic.Int64
	SubscriptionLen  atomic.Int64
}

// IncClientToBroker satisfies proxy.FrameCounter.
func (m *Metrics) IncClientToBroker() { m.FramesClientToBroker.Add(1) }

// IncBrokerToClient satisfies proxy.FrameCounter.
func (m *Metrics) IncBrokerToClient() { m.FramesBrokerToClient.Add(1) }

// New returns a fresh Metrics registered under the given expvar root name. If
// publish is false (tests), expvar isn't touched.
func New(name string, publish bool) *Metrics {
	m := &Metrics{}
	if publish {
		expvar.Publish(name, expvar.Func(func() any {
			return map[string]int64{
				"frames_client_to_broker":   m.FramesClientToBroker.Load(),
				"frames_broker_to_client":   m.FramesBrokerToClient.Load(),
				"intercepts_total":          m.InterceptsTotal.Load(),
				"intercepts_rewrote":        m.InterceptsRewrote.Load(),
				"intercepts_passthrough":    m.InterceptsPassthru.Load(),
				"intercepts_timeout":        m.InterceptsTimeout.Load(),
				"intercepts_error":          m.InterceptsError.Load(),
				"unmapped_brokers":          m.UnmappedBrokers.Load(),
				"plan_duration_nanos_total": m.PlanDurationNanosSum.Load(),
				"plan_count":                m.PlanCount.Load(),
				"conn_active":               m.ConnActive.Load(),
				"telemetry_age_nanos":       m.TelemetryAgeNS.Load(),
				"subscription_len":          m.SubscriptionLen.Load(),
			}
		}))
	}
	return m
}

// PromText writes the Prometheus exposition format for m. Counters get the
// `_total` suffix; durations are rendered in seconds.
func (m *Metrics) PromText() []byte {
	var b []byte
	b = appendCounter(b, "kproxy_frames_total", "direction", "client_to_broker", m.FramesClientToBroker.Load())
	b = appendCounter(b, "kproxy_frames_total", "direction", "broker_to_client", m.FramesBrokerToClient.Load())
	b = appendCounter(b, "kproxy_intercepts_total", "outcome", "any", m.InterceptsTotal.Load())
	b = appendCounter(b, "kproxy_intercepts_total", "outcome", "rewrote", m.InterceptsRewrote.Load())
	b = appendCounter(b, "kproxy_intercepts_total", "outcome", "passthrough", m.InterceptsPassthru.Load())
	b = appendCounter(b, "kproxy_intercepts_total", "outcome", "timeout", m.InterceptsTimeout.Load())
	b = appendCounter(b, "kproxy_intercepts_total", "outcome", "error", m.InterceptsError.Load())
	b = appendSimple(b, "kproxy_unmapped_brokers_total", m.UnmappedBrokers.Load())
	b = appendSimple(b, "kproxy_plan_count_total", m.PlanCount.Load())
	planNS := m.PlanDurationNanosSum.Load()
	b = appendFloat(b, "kproxy_plan_duration_seconds_sum", float64(planNS)/1e9)
	b = appendSimple(b, "kproxy_conn_active", m.ConnActive.Load())
	b = appendFloat(b, "kproxy_telemetry_age_seconds", float64(m.TelemetryAgeNS.Load())/1e9)
	b = appendSimple(b, "kproxy_subscription_len", m.SubscriptionLen.Load())
	return b
}

func appendSimple(dst []byte, name string, v int64) []byte {
	dst = append(dst, name...)
	dst = append(dst, ' ')
	dst = strconv.AppendInt(dst, v, 10)
	dst = append(dst, '\n')
	return dst
}

func appendCounter(dst []byte, name, label, value string, v int64) []byte {
	dst = append(dst, name...)
	dst = append(dst, '{')
	dst = append(dst, label...)
	dst = append(dst, '=', '"')
	dst = append(dst, value...)
	dst = append(dst, '"', '}', ' ')
	dst = strconv.AppendInt(dst, v, 10)
	dst = append(dst, '\n')
	return dst
}

func appendFloat(dst []byte, name string, v float64) []byte {
	dst = append(dst, name...)
	dst = append(dst, ' ')
	dst = strconv.AppendFloat(dst, v, 'f', 6, 64)
	dst = append(dst, '\n')
	return dst
}
