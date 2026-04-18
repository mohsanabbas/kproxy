// Command scenario orchestrates a full production-style demo against a local
// docker-compose Kafka. It:
//
//  1. Spawns cmd/kproxy as a subprocess pointing at the broker.
//  2. Starts N franz-go consumers connecting through the proxy.
//  3. Optionally kills one consumer mid-flight to trigger a rebalance.
//  4. Streams summary stats and surfaces the proxy's /metrics endpoint.
//
// kproxy's deployment model is "sidecar process", so spawning the real
// binary (rather than embedding the library) is the realistic demo.
//
// Usage (after `docker compose up -d kafka kafka-init` and `make build`):
//
//	go run ./scenario \
//	  -kproxy-bin ../bin/kproxy \
//	  -broker localhost:9094 \
//	  -listen 127.0.0.1:19094 \
//	  -consumers 4 -duration 90s -kill-after 30s
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var (
		kproxyBin  = flag.String("kproxy-bin", "../bin/kproxy", "path to compiled kproxy binary (run `make build` first)")
		broker     = flag.String("broker", "localhost:9094", "real broker host:port")
		listen     = flag.String("listen", "127.0.0.1:19094", "kproxy listen + advertised host:port")
		admin      = flag.String("admin", "127.0.0.1:9099", "kproxy admin endpoint")
		nodeID     = flag.Int("node-id", 1, "broker node id")
		topic      = flag.String("topic", "event-tracking_track-events-approved", "topic to consume")
		group      = flag.String("group", "scenario-group", "consumer group id")
		consumers  = flag.Int("consumers", 4, "number of consumer goroutines")
		duration   = flag.Duration("duration", 60*time.Second, "total run time")
		killAfter  = flag.Duration("kill-after", 25*time.Second, "kill one consumer after this delay (0 disables)")
		slow       = flag.Duration("slow", 0, "artificial per-record latency")
		skipKproxy = flag.Bool("skip-kproxy", false, "do not spawn kproxy (assume already running)")
	)
	flag.Parse()

	logger := log.New(os.Stderr, "[scenario] ", log.LstdFlags|log.Lmicroseconds)

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; logger.Print("signal received"); rootCancel() }()

	// --- Spawn kproxy subprocess -----------------------------------------
	if !*skipKproxy {
		topo := fmt.Sprintf("%d=%s=%s", *nodeID, *broker, *listen)
		cmd := exec.CommandContext(rootCtx, *kproxyBin,
			"-broker", *broker,
			"-listen", *listen,
			"-admin", *admin,
			"-topology", topo,
			"-telemetry", "5s",
			"-refresh", "10s",
		)
		cmd.Stdout = prefixWriter{prefix: "[kproxy] ", w: os.Stderr}
		cmd.Stderr = prefixWriter{prefix: "[kproxy] ", w: os.Stderr}
		if err := cmd.Start(); err != nil {
			logger.Fatalf("spawn kproxy: %v", err)
		}
		logger.Printf("kproxy pid=%d", cmd.Process.Pid)
		defer func() {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			_, _ = cmd.Process.Wait()
		}()
		if err := waitReady(rootCtx, "http://"+*admin+"/healthz", 10*time.Second); err != nil {
			logger.Fatalf("kproxy not ready: %v", err)
		}
		logger.Printf("kproxy ready on %s (admin %s)", *listen, *admin)
	}

	// --- Consumers --------------------------------------------------------
	var wg sync.WaitGroup
	cancels := make([]context.CancelFunc, *consumers)
	totals := make([]*atomic.Int64, *consumers)
	for i := 0; i < *consumers; i++ {
		i := i
		cctx, ccancel := context.WithCancel(rootCtx)
		cancels[i] = ccancel
		count := &atomic.Int64{}
		totals[i] = count
		wg.Add(1)
		go func() {
			defer wg.Done()
			runConsumer(cctx, logger, *listen, *topic, *group, fmt.Sprintf("c%d", i), *slow, count)
		}()
	}

	if *killAfter > 0 && *consumers > 1 {
		go func() {
			select {
			case <-time.After(*killAfter):
			case <-rootCtx.Done():
				return
			}
			logger.Printf("KILL c0 → expect rebalance")
			cancels[0]()
		}()
	}

	go func() {
		select {
		case <-time.After(*duration):
			logger.Printf("duration elapsed; shutting down")
			rootCancel()
		case <-rootCtx.Done():
		}
	}()

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-rootCtx.Done():
				return
			case <-t.C:
				summary(logger, totals, *admin)
			}
		}
	}()

	wg.Wait()
	summary(logger, totals, *admin)
	logger.Print("scenario complete")
}

func summary(logger *log.Logger, totals []*atomic.Int64, admin string) {
	parts := []string{}
	var total int64
	for i, c := range totals {
		n := c.Load()
		total += n
		parts = append(parts, fmt.Sprintf("c%d=%d", i, n))
	}
	sort.Strings(parts)
	logger.Printf("totals: %v sum=%d %s", parts, total, scrapeMetrics(admin))
}

func scrapeMetrics(admin string) string {
	if admin == "" {
		return ""
	}
	cli := http.Client{Timeout: 500 * time.Millisecond}
	resp, err := cli.Get("http://" + admin + "/metrics")
	if err != nil || resp.StatusCode != 200 {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	keys := []string{
		`kproxy_intercepts_total{outcome="any"}`,
		"kproxy_plan_count_total",
		"kproxy_unmapped_brokers_total",
		"kproxy_conn_active",
	}
	out := ""
	for _, k := range keys {
		if v := pluck(string(b), k); v != "" {
			out += k + "=" + v + " "
		}
	}
	return out
}

func pluck(text, key string) string {
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, key) {
			continue
		}
		return strings.TrimSpace(line[len(key):])
	}
	return ""
}

func runConsumer(ctx context.Context, logger *log.Logger, brokers, topic, group, id string, slow time.Duration, count *atomic.Int64) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ClientID("scenario-"+id),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.AutoCommitInterval(3*time.Second),
		kgo.SessionTimeout(15*time.Second),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			logger.Printf("[%s] +%v", id, m)
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			logger.Printf("[%s] -%v", id, m)
		}),
	)
	if err != nil {
		logger.Printf("[%s] new client: %v", id, err)
		return
	}
	defer cl.Close()

	for {
		if ctx.Err() != nil {
			cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = cl.CommitUncommittedOffsets(cctx)
			cancel()
			logger.Printf("[%s] exit (final n=%d)", id, count.Load())
			return
		}
		fetches := cl.PollFetches(ctx)
		fetches.EachRecord(func(_ *kgo.Record) {
			count.Add(1)
			if slow > 0 {
				time.Sleep(slow)
			}
		})
	}
}

func waitReady(ctx context.Context, url string, deadline time.Duration) error {
	dctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	cli := http.Client{Timeout: 200 * time.Millisecond}
	for {
		req, _ := http.NewRequestWithContext(dctx, http.MethodGet, url, nil)
		resp, err := cli.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		select {
		case <-dctx.Done():
			return dctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

type prefixWriter struct {
	prefix string
	w      io.Writer
}

func (p prefixWriter) Write(b []byte) (int, error) {
	for _, line := range strings.Split(string(b), "\n") {
		if line == "" {
			continue
		}
		fmt.Fprintln(p.w, p.prefix+line)
	}
	return len(b), nil
}
