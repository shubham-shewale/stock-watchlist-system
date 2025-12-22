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

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
	"github.com/shubham-shewale/stock-watchlist/pkg/config"
	"github.com/shubham-shewale/stock-watchlist/pkg/models"
	"go.uber.org/zap"
)

var (
	logger *zap.Logger
	rdb    *redis.Client
)

// NumWorkers defines concurrency level.
// Should correspond roughly to number of CPU cores or Redis capability.
const NumWorkers = 10

func main() {
	// 1. Initialize Zap
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	// 2. Load Config
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}

	// 3. Redis Setup
	rdb = redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Fatal("Failed to connect to Redis", zap.Error(err))
	}

	// 4. Kafka Reader Setup
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Kafka.Brokers,
		Topic:    cfg.Kafka.Topic,
		GroupID:  cfg.Kafka.GroupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
		// Explicit commit gives us control, but for high-throughput stock ticks
		// with a worker pool, managing distributed offsets is complex.
		// We rely on auto-commit here for throughput, accepting slight risk of
		// duplicate processing (idempotency is handled by SeqID in a real app).
		CommitInterval: 1,
	})

	// 5. Setup Worker Pool (Sharded by Symbol)
	// We create N channels for N workers
	workerChans := make([]chan []byte, NumWorkers)
	var wg sync.WaitGroup

	for i := 0; i < NumWorkers; i++ {
		workerChans[i] = make(chan []byte, 100) // Buffer 100
		wg.Add(1)
		// Start Worker
		go worker(i, workerChans[i], &wg)
	}

	// 6. Graceful Shutdown Setup
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 7. Main Consumer Loop
	go func() {
		logger.Info("Processor Started", zap.Int("workers", NumWorkers))
		for {
			// Read Message (Blocking)
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // Shutdown triggered
				}
				logger.Error("Kafka Read Error", zap.Error(err))
				continue
			}

			// Determine which worker gets this message
			// Hashing logic ensures "AAPL" always goes to same worker -> preserving order
			// Key must be the Symbol
			workerID := getWorkerID(m.Key)

			// Dispatch to worker
			select {
			case workerChans[workerID] <- m.Value:
				// Success
			case <-ctx.Done():
				return
			}
		}
	}()

	// 8. Wait for Shutdown
	<-sigChan
	logger.Info("Shutdown signal received, stopping processor...")
	cancel() // Stops the kafka read loop

	// 9. Cleanup Sequence
	logger.Info("Closing Kafka Reader...")
	if err := reader.Close(); err != nil {
		logger.Error("Error closing reader", zap.Error(err))
	}

	logger.Info("Waiting for workers to drain...")
	// Close all channels to signal workers to stop
	for _, ch := range workerChans {
		close(ch)
	}
	// Wait for workers to finish processing their buffer
	wg.Wait()

	logger.Info("Closing Redis...")
	rdb.Close()

	logger.Info("Processor exited cleanly")
}

// worker handles the actual processing pipeline
func worker(id int, msgs <-chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background() // Use background context for Redis writes (don't cancel mid-write)

	for payload := range msgs {
		var update models.StockUpdate
		if err := json.Unmarshal(payload, &update); err != nil {
			logger.Error("JSON Unmarshal Error", zap.Error(err))
			continue
		}

		key := fmt.Sprintf("stock:%s", update.Symbol)

		// Step A: Snapshot (KV)
		// In production, we might use a Pipeline here to do both Set and Publish in 1 RTT
		pipe := rdb.Pipeline()
		pipe.Set(ctx, key, payload, 0)
		channelName := fmt.Sprintf("prices.%s", update.Symbol)
		pipe.Publish(ctx, channelName, payload)

		_, err := pipe.Exec(ctx)
		if err != nil {
			logger.Error("Redis Pipeline Error", zap.Error(err), zap.String("symbol", update.Symbol))
			// Retry logic could go here
		} else {
			logger.Debug("Processed", zap.String("symbol", update.Symbol), zap.Int("worker_id", id))
		}
	}
}

// getWorkerID hashes the key (Symbol) to find a consistent worker ID
func getWorkerID(key []byte) int {
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32()) % NumWorkers
}
