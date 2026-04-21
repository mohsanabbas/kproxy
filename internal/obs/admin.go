package obs

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"
)

// HealthChecker is a snapshot of liveness state. /healthz returns 503 when
// any field is non-OK so k8s/LB will pull the pod out. Implementations are
// expected to be cheap (atomic loads, single map lookup).
type HealthChecker interface {
	// MetadataAgeOK returns true when the most recent metadata snapshot is
	// fresh enough to serve traffic.
	MetadataAgeOK() bool
}

// Admin runs an HTTP server exposing /metrics, /healthz, and (optionally)
// /debug/pprof. Bind to localhost by default; bind to 0.0.0.0 only behind a
// trusted LB. pprof MUST stay disabled on any port reachable from untrusted
// networks - heap profiles leak in-flight Kafka frame plaintext (group ids,
// topic names, possibly tokens) and 30s pprof.Profile is a CPU DoS.
type Admin struct {
	Addr    string
	Metrics *Metrics
	// Health, if non-nil, is consulted on every /healthz request. Nil means
	// /healthz returns 200 unconditionally (legacy behavior; do not use in
	// production).
	Health HealthChecker
	// EnablePprof exposes /debug/pprof. Default false because the data
	// surfaced is sensitive (see type doc).
	EnablePprof bool
}

// Run blocks serving until ctx is canceled, then shuts down with a 5s grace.
func (a *Admin) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	// Root index - lists the admin endpoints. Without this, navigating to the
	// admin host returns 404 which is a confusing first impression.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Strict 200 only on the actual root; everything else is 404.
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		body := "kproxy admin\n  GET /healthz\n  GET /metrics\n"
		if a.EnablePprof {
			body += "  GET /debug/pprof/\n"
		}
		_, _ = w.Write([]byte(body))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if a.Health != nil && !a.Health.MetadataAgeOK() {
			// 503 is the standard "pull me out of the LB" signal.
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("metadata stale\n"))
			return
		}
		panics := int64(0)
		if a.Metrics != nil {
			panics = a.Metrics.Panics.Load()
		}
		_, _ = w.Write([]byte("ok panics=" + strconv.FormatInt(panics, 10) + "\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write(a.Metrics.PromText())
	})
	if a.EnablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	srv := &http.Server{
		Addr:              a.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", a.Addr)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
