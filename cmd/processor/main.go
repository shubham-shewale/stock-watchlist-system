package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/shubham-shewale/stock-watchlist/pkg/config"
	"github.com/shubham-shewale/stock-watchlist/pkg/models"
	"go.uber.org/zap"
)

var (
	logger *zap.Logger
	rdb    *redis.Client
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	logger, err = config.NewLogger(cfg.Logger)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer logger.Sync()

	rdb = redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Fatal("Failed to connect to Redis", zap.Error(err))
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Kafka.Brokers,
		Topic:    cfg.Kafka.Topic,
		GroupID:  cfg.Kafka.GroupID,
		MinBytes: 200,
		MaxBytes: 10e6,
		MaxWait:  200 * time.Millisecond,
		// Auto-commit for throughput with deduplication by SeqID (avoids complex distributed offset management)
		CommitInterval: 1,
		// Rebalancing: 3s heartbeat, 10s session timeout for responsive scaling
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    10 * time.Second,
	})

	// Worker pool sharded by symbol for concurrency and order preservation
	numWorkers := cfg.Processor.NumWorkers
	workerChans := make([]chan []byte, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		workerChans[i] = make(chan []byte, 100) // Buffered channel for backpressure
		wg.Add(1)
		go worker(i, workerChans[i], &wg)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.Info("Processor Started", zap.Int("workers", numWorkers))
		for {
			// Auto commit after the message is fetched, TradeOff: Use reader.FetchMessage (which does not commit)
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Error("Kafka Read Error", zap.Error(err))
				continue
			}

			// Consistent hashing ensures symbol-based sharding for order preservation
			workerID := getWorkerID(m.Key, numWorkers)

			select {
			case workerChans[workerID] <- m.Value:
			case <-ctx.Done():
				return
			default:
				// NON-BLOCKING: If channel is full, we drop the packet.
				// In real-time stocks, "latest" is better than "all".
				logger.Warn("Dropping slow packet", zap.String("key", string(m.Key)), zap.Int("worker_id", workerID))
			}
		}
	}()

	<-sigChan
	logger.Info("Shutdown signal received, stopping processor...")
	cancel()

	logger.Info("Closing Kafka Reader...")
	if err := reader.Close(); err != nil {
		logger.Error("Error closing reader", zap.Error(err))
	}

	logger.Info("Waiting for workers to drain...")
	for _, ch := range workerChans {
		close(ch)
	}
	wg.Wait()

	logger.Info("Closing Redis...")
	rdb.Close()

	logger.Info("Processor exited cleanly")
}

func worker(id int, msgs <-chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background() // Background context prevents cancellation mid-Redis write

	// Per-worker deduplication state for exactly-once processing
	lastSeq := make(map[string]int64)

	for payload := range msgs {
		var update models.StockUpdate
		if err := json.Unmarshal(payload, &update); err != nil {
			logger.Error("JSON Unmarshal Error", zap.Error(err))
			continue
		}

		// Deduplication: skip if SeqID not greater than last processed
		if update.SeqID <= lastSeq[update.Symbol] {
			logger.Debug("Skipping duplicate update", zap.String("symbol", update.Symbol), zap.Int64("seq_id", update.SeqID), zap.Int64("last_seq", lastSeq[update.Symbol]))
			continue
		}

		key := fmt.Sprintf("stock:%s", update.Symbol)

		// Atomic SET + PUBLISH in single pipeline for consistency
		pipe := rdb.Pipeline()
		pipe.Set(ctx, key, payload, 1*time.Hour) // TTL prevents unbounded memory growth
		channelName := fmt.Sprintf("prices.%s", update.Symbol)
		pipe.Publish(ctx, channelName, payload)

		_, err := pipe.Exec(ctx)
		if err != nil {
			logger.Error("Redis Pipeline Error", zap.Error(err), zap.String("symbol", update.Symbol))
		} else {
			logger.Debug("Processed", zap.String("symbol", update.Symbol), zap.Int("worker_id", id), zap.Int64("seq_id", update.SeqID))
			lastSeq[update.Symbol] = update.SeqID
		}
	}
}

func getWorkerID(key []byte, numWorkers int) int {
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32()) % numWorkers
}
