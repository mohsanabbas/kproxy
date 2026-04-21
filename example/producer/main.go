// Command producer publishes a steady stream of events to a topic. Used by
// the kproxy demo to feed the consumer group with traffic so rebalances and
// lag are observable.
//
// Usage:
//
//	go run ./producer -brokers localhost:19094 -topic event-tracking_track-events-approved -rate 200
//
// Point -brokers at kproxy (not the broker directly) so the producer's
// Metadata exchange is rewritten and you can verify the topology mapping
// works end-to-end.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var (
		brokers  = flag.String("brokers", "localhost:19094", "comma-separated bootstrap servers (point at kproxy)")
		topic    = flag.String("topic", "event-tracking_track-events-approved", "topic to produce to")
		rate     = flag.Int("rate", 100, "approx messages/sec")
		clientID = flag.String("client-id", "demo-producer", "Kafka client.id")
		keySpace = flag.Int("keys", 1000, "number of distinct keys (controls partition spread)")
	)
	flag.Parse()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(splitCSV(*brokers)...),
		kgo.ClientID(*clientID),
		kgo.DefaultProduceTopic(*topic),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; cancel() }()

	interval := time.Second / time.Duration(max(1, *rate))
	t := time.NewTicker(interval)
	defer t.Stop()

	var sent atomic.Int64
	go func() {
		report := time.NewTicker(2 * time.Second)
		defer report.Stop()
		var prev int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-report.C:
				cur := sent.Load()
				log.Printf("produced=%d Δ=%d", cur, cur-prev)
				prev = cur
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Printf("flushing... produced=%d", sent.Load())
			_ = cl.Flush(context.Background())
			return
		case <-t.C:
			k := strconv.Itoa(rand.IntN(*keySpace))
			v := fmt.Sprintf("evt@%d k=%s rnd=%d", time.Now().UnixNano(), k, rand.Int64())
			cl.Produce(ctx, &kgo.Record{Key: []byte(k), Value: []byte(v)}, func(r *kgo.Record, err error) {
				if err != nil {
					if ctx.Err() == nil {
						log.Printf("produce err: %v", err)
					}
					return
				}
				sent.Add(1)
			})
		}
	}
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
