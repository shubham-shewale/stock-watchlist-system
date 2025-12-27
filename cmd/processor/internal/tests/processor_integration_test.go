package tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/shubham-shewale/stock-watchlist/cmd/processor/internal/processor"
	"github.com/shubham-shewale/stock-watchlist/cmd/processor/internal/testutils"
	"github.com/shubham-shewale/stock-watchlist/pkg/config"
	"github.com/shubham-shewale/stock-watchlist/pkg/models"
)

func TestProcessor_EndToEnd_Flow(t *testing.T) {
	mr := miniredis.RunT(t)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	update := models.StockUpdate{Symbol: "GOOG", Price: 1500.50, SeqID: 100}
	val, _ := json.Marshal(update)

	msgs := []kafka.Message{
		{Key: []byte("GOOG"), Value: val},
	}
	// Use Mock Reader because spinning up real Kafka is heavy/complex for unit tests
	mockReader := &testutils.MockKafkaReader{Messages: msgs}

	cfg := &config.Config{}
	cfg.Processor.NumWorkers = 1
	logger := zap.NewNop()

	proc := processor.NewProcessor(cfg, logger, rdb, mockReader)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		proc.Run(ctx)
		close(done)
	}()

	// Poll until the key appears (since processor is async)
	success := false
	for i := 0; i < 10; i++ {
		if mr.Exists("stock:GOOG") {
			success = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !success {
		t.Fatal("Processor did not write stock:GOOG to Redis")
	}

	savedVal, _ := mr.Get("stock:GOOG")
	if savedVal != string(val) {
		t.Errorf("Redis value mismatch.\nGot:  %s\nWant: %s", savedVal, string(val))
	}

	cancel()
	<-done
}
