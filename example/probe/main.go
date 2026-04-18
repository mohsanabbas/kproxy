package main
import (
  "context"
  "fmt"
  "os"
  "time"
  "github.com/twmb/franz-go/pkg/kgo"
)
func main(){
  bs := os.Args[1]
  cl, err := kgo.NewClient(
    kgo.SeedBrokers(bs),
    kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)),
    kgo.DefaultProduceTopic("event-tracking_track-events-approved"),
  )
  if err != nil { panic(err) }
  defer cl.Close()
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
  defer cancel()
  res := cl.ProduceSync(ctx, &kgo.Record{Value: []byte("hello")})
  for _, r := range res {
    if r.Err != nil { fmt.Println("ERR", r.Err) } else { fmt.Println("OK", r.Record.Partition, r.Record.Offset) }
  }
}
