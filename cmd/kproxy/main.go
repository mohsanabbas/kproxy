// Command kproxy runs the Kafka rebalance proxy. It accepts Kafka client
// connections, forwards each to a single broker, and intercepts JoinGroup /
// SyncGroup / Metadata / FindCoordinator frames to apply a globally fair
// partition assignment plan.
//
// Topology rewriting is mandatory in production: every client must see broker
// addresses that route through the proxy, otherwise consumers will bypass it
// after the initial Metadata exchange.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/mohsanabbas/kproxy/internal/interceptor"
	"github.com/mohsanabbas/kproxy/internal/kclient"
	"github.com/mohsanabbas/kproxy/internal/metadata"
	"github.com/mohsanabbas/kproxy/internal/obs"
	"github.com/mohsanabbas/kproxy/internal/planner"
	"github.com/mohsanabbas/kproxy/internal/proxy"
	"github.com/mohsanabbas/kproxy/internal/subscription"
	"github.com/mohsanabbas/kproxy/internal/telemetry"
	"github.com/mohsanabbas/kproxy/internal/topology"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "kproxy:", err)
		os.Exit(1)
	}
}

type config struct {
	listen        string
	broker        string
	bootstrap     string
	admin         string
	topologyFlag  string
	topologyFile  string
	connLimit     int
	frameMax      int
	idleTimeout   time.Duration
	planTimeout   time.Duration
	refresh       time.Duration
	telemetryEv   time.Duration
	dialTimeout   time.Duration
	drainTimeout  time.Duration
	plannerWorker int
	plannerQueue  int
	subMax        int
	clientID      string
}

func run() error {
	var cfg config
	flag.StringVar(&cfg.listen, "listen", ":9092", "address to accept client connections on")
	flag.StringVar(&cfg.broker, "broker", "", "single upstream broker to dial per accepted conn (host:port). Required.")
	flag.StringVar(&cfg.bootstrap, "bootstrap", "", "broker host:port used for metadata + telemetry side-channel. Defaults to -broker.")
	flag.StringVar(&cfg.admin, "admin", "127.0.0.1:9099", "admin HTTP listen address (metrics/pprof/healthz). Empty disables.")
	flag.StringVar(&cfg.topologyFlag, "topology", "", "comma-separated nodeID=real:port=advertised:port mapping (overrides -topology-file)")
	flag.StringVar(&cfg.topologyFile, "topology-file", "", "JSON topology file path")
	flag.IntVar(&cfg.connLimit, "conn-limit", 4096, "max concurrent client connections (0 disables)")
	flag.IntVar(&cfg.frameMax, "frame-max", 0, "max Kafka frame size in bytes (0 = frame package default)")
	flag.DurationVar(&cfg.idleTimeout, "idle", 5*time.Minute, "per-iteration read deadline on client/broker sockets")
	flag.DurationVar(&cfg.planTimeout, "plan-timeout", 2*time.Second, "max time to wait for planner result before falling back to passthrough")
	flag.DurationVar(&cfg.refresh, "refresh", 30*time.Second, "metadata cache refresh interval")
	flag.DurationVar(&cfg.telemetryEv, "telemetry", 15*time.Second, "telemetry poll interval")
	flag.DurationVar(&cfg.dialTimeout, "dial-timeout", 5*time.Second, "broker dial timeout")
	flag.DurationVar(&cfg.drainTimeout, "drain-timeout", 30*time.Second, "shutdown grace before force-closing live conns")
	flag.IntVar(&cfg.plannerWorker, "planner-workers", 0, "planner worker count (0 = GOMAXPROCS)")
	flag.IntVar(&cfg.plannerQueue, "planner-queue", 0, "planner queue depth (0 = workers*4)")
	flag.IntVar(&cfg.subMax, "subscription-cap", 100_000, "max tracked subscriptions across all groups")
	flag.StringVar(&cfg.clientID, "client-id", "kproxy", "client.id presented to the broker by side-channel kclient calls")
	flag.Parse()

	if cfg.broker == "" {
		return errors.New("-broker is required")
	}
	if cfg.bootstrap == "" {
		cfg.bootstrap = cfg.broker
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("starting kproxy",
		"listen", cfg.listen, "broker", cfg.broker, "bootstrap", cfg.bootstrap,
		"admin", cfg.admin, "go", runtime.Version())

	// Topology
	topo := topology.New()
	switch {
	case cfg.topologyFlag != "":
		t, err := topology.ParseFlag(cfg.topologyFlag)
		if err != nil {
			return fmt.Errorf("parse -topology: %w", err)
		}
		topo = t
	case cfg.topologyFile != "":
		t, err := topology.LoadFile(cfg.topologyFile)
		if err != nil {
			return fmt.Errorf("load -topology-file: %w", err)
		}
		topo = t
	}
	logger.Info("topology loaded", "entries", topo.Len())

	// Observability
	metrics := obs.New("kproxy", true)

	// Side-channel kclient (shared by metadata + telemetry) -
	side, err := kclient.Dial(cfg.bootstrap, cfg.clientID, cfg.dialTimeout)
	if err != nil {
		return fmt.Errorf("dial bootstrap: %w", err)
	}
	defer side.Close()

	// Metadata cache
	metaCache := metadata.NewCache(metadata.KClientSource{Conn: side}, cfg.refresh)

	// Subscription store + telemetry registry
	subStore := subscription.NewStore(cfg.subMax)
	groupReg := telemetry.NewSyncRegistry()
	subStore.SetOnChange(func(group string) { groupReg.Add(group) })

	// Telemetry holder + poller
	holder := &telemetry.Holder{}
	poller := &telemetry.Poller{
		Coord:    side,
		Registry: groupReg,
		Holder:   holder,
		Interval: cfg.telemetryEv,
		OnError: func(group string, err error) {
			logger.Warn("telemetry error", "group", group, "err", err)
		},
	}

	// Planner pool
	pp := planner.New(cfg.plannerWorker, cfg.plannerQueue)

	// Interceptor wiring
	ic := interceptor.New(interceptor.Deps{
		Topology:     topo,
		Metadata:     metaCache,
		Subscription: subStore,
		Telemetry:    holder,
		Planner:      pp,
		PlanTimeout:  cfg.planTimeout,
		Metrics:      metrics,
	})

	// Listener
	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer ln.Close()

	connCfg := proxy.Config{
		MaxFrameSize: cfg.frameMax,
		IdleTimeout:  cfg.idleTimeout,
		Frames:       metrics,
	}
	listener := &proxy.Listener{
		Listen:        ln,
		MaxConcurrent: cfg.connLimit,
		AcceptTimeout: cfg.dialTimeout,
		DialBroker: func(ctx context.Context) (net.Conn, error) {
			d := net.Dialer{Timeout: cfg.dialTimeout}
			return d.DialContext(ctx, "tcp", cfg.broker)
		},
		MakeConn: func(client, broker net.Conn) *proxy.Conn {
			return proxy.New(connCfg, client, broker, ic)
		},
		OnAcceptError: func(err error) { logger.Warn("accept error", "err", err) },
		OnConnStart: func() {
			metrics.ConnActive.Add(1)
			logger.Info("conn start", "active", metrics.ConnActive.Load())
		},
		OnConnEnd: func() {
			metrics.ConnActive.Add(-1)
			logger.Info("conn end", "active", metrics.ConnActive.Load())
		},
	}

	// Run everything
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	eg, egCtx := errgroup.WithContext(rootCtx)

	eg.Go(func() error { metaCache.Run(egCtx); return nil })
	eg.Go(func() error { poller.Run(egCtx); return nil })
	eg.Go(func() error {
		if cfg.admin == "" {
			return nil
		}
		admin := &obs.Admin{Addr: cfg.admin, Metrics: metrics}
		return admin.Run(egCtx)
	})

	serveDone := make(chan struct{})
	eg.Go(func() error {
		err := listener.Serve(egCtx)
		close(serveDone)
		return err
	})

	// telemetry-age + subscription-len gauges, refreshed at the same cadence
	// as telemetry polling.
	eg.Go(func() error { gaugeRefresher(egCtx, metrics, holder, subStore); return nil })

	logger.Info("kproxy ready", "addr", ln.Addr().String())

	select {
	case sig := <-sigCh:
		logger.Info("signal received", "sig", sig.String())
	case <-egCtx.Done():
		logger.Error("subsystem failed")
	}

	// Orderly shutdown: close listener so accept loop unblocks; wait up to
	// drainTimeout for in-flight connections to finish; then cancel root ctx
	// to force any stragglers to exit (their pumps will see net.ErrClosed).
	_ = ln.Close()
	logger.Info("draining", "live", metrics.ConnActive.Load(), "deadline", cfg.drainTimeout)
	dctx, dcancel := context.WithTimeout(context.Background(), cfg.drainTimeout)
	select {
	case <-serveDone:
	case <-dctx.Done():
		logger.Warn("drain timed out, forcing shutdown")
	}
	dcancel()
	rootCancel()
	pp.Close()
	// Wait for all goroutines to finish after cancellation.
	_ = eg.Wait()
	logger.Info("kproxy stopped")
	return nil
}

// gaugeRefresher updates time-derived gauges periodically. We do this in a
// dedicated goroutine because the snapshot pointers are atomic and the cost
// is trivial.
func gaugeRefresher(ctx context.Context, m *obs.Metrics, h *telemetry.Holder, s *subscription.Store) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if snap := h.Get(); snap != nil {
				age := time.Since(snap.BuiltAt)
				m.TelemetryAgeNS.Store(age.Nanoseconds())
			}
			m.SubscriptionLen.Store(int64(s.Len()))
		}
	}
}
