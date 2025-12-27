package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/shubham-shewale/stock-watchlist/pkg/config"
	"github.com/shubham-shewale/stock-watchlist/pkg/models"
)

type Processor struct {
	cfg        *config.Config
	logger     Logger
	rdb        RedisClient
	reader     KafkaReader
	numWorkers int
}

func NewProcessor(cfg *config.Config, logger Logger, rdb RedisClient, reader KafkaReader) *Processor {
	return &Processor{
		cfg:        cfg,
		logger:     logger,
		rdb:        rdb,
		reader:     reader,
		numWorkers: cfg.Processor.NumWorkers,
	}
}

func (p *Processor) Run(ctx context.Context) error {
	workerChans := make([]chan []byte, p.numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < p.numWorkers; i++ {
		workerChans[i] = make(chan []byte, 100)
		wg.Add(1)
		go p.worker(i, workerChans[i], &wg)
	}

	go func() {
		p.logger.Info("Processor Started", zap.Int("workers", p.numWorkers))
		for {
			m, err := p.reader.ReadMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				p.logger.Error("Kafka Read Error", zap.Error(err))
				continue
			}

			// Deterministic Sharding: Same symbol always goes to same worker
			workerID := getWorkerID(m.Key, p.numWorkers)

			select {
			case workerChans[workerID] <- m.Value:
			case <-ctx.Done():
				return
			default:
				p.logger.Warn("Dropping slow packet", zap.String("key", string(m.Key)), zap.Int("worker_id", workerID))
			}
		}
	}()

	<-ctx.Done()
	p.logger.Info("Shutdown signal received, stopping processor...")

	for _, ch := range workerChans {
		close(ch)
	}
	p.logger.Info("Waiting for workers to drain...")
	wg.Wait()

	return nil
}

func (p *Processor) worker(id int, msgs <-chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()

	// Local state for deduplication (only works because of deterministic sharding)
	lastSeq := make(map[string]int64)

	for payload := range msgs {
		var update models.StockUpdate
		if err := json.Unmarshal(payload, &update); err != nil {
			p.logger.Error("JSON Unmarshal Error", zap.Error(err))
			continue
		}

		if update.SeqID <= lastSeq[update.Symbol] {
			p.logger.Debug("Skipping duplicate update", zap.String("symbol", update.Symbol), zap.Int64("seq_id", update.SeqID))
			continue
		}

		key := fmt.Sprintf("stock:%s", update.Symbol)

		// Atomic Update via Pipeline
		pipe := p.rdb.Pipeline()
		pipe.Set(ctx, key, payload, 1*time.Hour)
		channelName := fmt.Sprintf("prices.%s", update.Symbol)
		pipe.Publish(ctx, channelName, payload)

		_, err := pipe.Exec(ctx)
		if err != nil {
			p.logger.Error("Redis Pipeline Error", zap.Error(err), zap.String("symbol", update.Symbol))
		} else {
			p.logger.Debug("Processed", zap.String("symbol", update.Symbol), zap.Int("worker_id", id))
			lastSeq[update.Symbol] = update.SeqID
		}
	}
}

func getWorkerID(key []byte, numWorkers int) int {
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32()) % numWorkers
}
