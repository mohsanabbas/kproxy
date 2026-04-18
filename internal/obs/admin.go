package obs

import (
	"context"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

// Admin runs an HTTP server exposing /metrics, /debug/pprof, and /healthz.
// It binds to localhost by default; bind to 0.0.0.0 only behind a trusted LB.
type Admin struct {
	Addr    string
	Metrics *Metrics
}

// Run blocks serving until ctx is cancelled, then shuts down with a 5s grace.
func (a *Admin) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	// Root index — lists the admin endpoints. Without this, navigating to the
	// admin host returns 404 which is a confusing first impression.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Strict 200 only on the actual root; everything else is 404.
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("kproxy admin\n" +
			"  GET /healthz\n" +
			"  GET /metrics\n" +
			"  GET /debug/pprof/\n"))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write(a.Metrics.PromText())
	})
	// pprof
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{
		Addr:              a.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := net.Listen("tcp", a.Addr)
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
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}
