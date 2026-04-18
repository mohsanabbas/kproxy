// Command consumer joins a consumer group through kproxy and prints assigned
// partitions and per-poll lag. Run multiple instances to observe rebalances
// and the proxy's planning take effect.
//
// Production-level edge cases this exercises:
//
//   - Topology rewrite: -brokers points at kproxy, but the consumer also
//     issues FindCoordinator + Metadata which the proxy must rewrite, else
//     the second hop bypasses kproxy and the rebalance plan never lands.
//   - Cooperative-sticky: cooperative rebalance protocol so the proxy's
//     SyncGroup intercept must preserve owned partitions across rebalances.
//   - Slow consumer: -slow flag introduces processing latency so lag grows
//     and the planner has signal to re-balance towards healthier members.
//   - Graceful shutdown: SIGTERM commits offsets and leaves the group so the
//     remaining members rebalance promptly.
//   - Crash simulation: -crash-after duration aborts without committing, so
//     you can observe session-timeout-driven rebalances.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var (
		brokers     = flag.String("brokers", "localhost:19094", "comma-separated bootstrap servers (point at kproxy)")
		topic       = flag.String("topic", "event-tracking_track-events-approved", "topic to consume")
		group       = flag.String("group", "demo-group", "consumer group id")
		clientID    = flag.String("client-id", "", "Kafka client.id (default: demo-consumer-<pid>)")
		instance    = flag.String("instance", "", "static membership group.instance.id (empty disables)")
		slow        = flag.Duration("slow", 0, "artificial per-record processing latency")
		crashAfter  = flag.Duration("crash-after", 0, "if >0 os.Exit(1) without commit after this duration")
		commitEvery = flag.Duration("commit", 5*time.Second, "auto-commit interval")
	)
	flag.Parse()

	if *clientID == "" {
		*clientID = fmt.Sprintf("demo-consumer-%d", os.Getpid())
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(splitCSV(*brokers)...),
		kgo.ClientID(*clientID),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.AutoCommitInterval(*commitEvery),
		kgo.SessionTimeout(15 * time.Second),
		kgo.RebalanceTimeout(30 * time.Second),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			log.Printf("[%s] assigned: %s", *clientID, fmtParts(assigned))
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			log.Printf("[%s] revoked:  %s", *clientID, fmtParts(revoked))
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			log.Printf("[%s] LOST:     %s", *clientID, fmtParts(lost))
		}),
	}
	if *instance != "" {
		opts = append(opts, kgo.InstanceID(*instance))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("[%s] signal — leaving group cleanly", *clientID)
		cancel()
	}()

	if *crashAfter > 0 {
		go func() {
			time.Sleep(*crashAfter)
			log.Printf("[%s] CRASH — exiting without commit", *clientID)
			os.Exit(1)
		}()
	}

	var processed atomic.Int64
	go reportLoop(ctx, *clientID, &processed)

	for {
		if ctx.Err() != nil {
			log.Printf("[%s] shutdown — committing", *clientID)
			cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := cl.CommitUncommittedOffsets(cctx); err != nil {
				log.Printf("[%s] final commit err: %v", *clientID, err)
			}
			ccancel()
			return
		}
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if ctx.Err() == nil {
					log.Printf("[%s] poll err t=%s p=%d: %v", *clientID, e.Topic, e.Partition, e.Err)
				}
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			processed.Add(1)
			if *slow > 0 {
				time.Sleep(*slow)
			}
		})
	}
}

func reportLoop(ctx context.Context, id string, processed *atomic.Int64) {
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()
	var prev int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cur := processed.Load()
			log.Printf("[%s] processed=%d Δ=%d", id, cur, cur-prev)
			prev = cur
		}
	}
}

func fmtParts(m map[string][]int32) string {
	if len(m) == 0 {
		return "<none>"
	}
	out := ""
	for t, ps := range m {
		sorted := append([]int32(nil), ps...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		out += fmt.Sprintf("%s=%v ", t, sorted)
	}
	return out
}

func splitCSV(s string) []string {
	out := []string{}
	cur := ""
	for _, c := range s {
		if c == ',' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		cur += string(c)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
