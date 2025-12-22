package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/shubham-shewale/stock-watchlist/pkg/config"
	"github.com/shubham-shewale/stock-watchlist/pkg/models"
	"go.uber.org/zap"
)

var (
	tickers = []string{"AAPL", "GOOG", "TSLA", "AMZN"}
	logger  *zap.Logger
)

func main() {
	// 1. Initialize Zap Logger
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

	// 3. Create Topic (Ensure it exists)
	createTopic(cfg.Kafka.Brokers[0], cfg.Kafka.Topic)

	// 4. Setup Kafka Writer (Production Tuning)
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Kafka.Brokers...),
		Topic:    cfg.Kafka.Topic,
		Balancer: &kafka.LeastBytes{},
		// Optimization: Send batches to reduce network IO
		BatchSize:    100,                   // Send after 100 messages
		BatchTimeout: 10 * time.Millisecond, // OR send after 10ms
		Async:        true,                  // Write non-blocking (fire and forget handled by buffer)
	}

	// 5. Setup Shutdown Hook
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Local State
	seqCounters := make(map[string]int64)
	basePrices := map[string]float64{
		"AAPL": 150.0, "GOOG": 2800.0, "TSLA": 700.0, "AMZN": 3400.0,
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// 6. Main Generator Loop
	go func() {
		logger.Info("Generator Started", zap.Strings("brokers", cfg.Kafka.Brokers))

		for {
			select {
			case <-ctx.Done():
				return
			default:
				symbol := tickers[r.Intn(len(tickers))]
				fluctuation := (r.Float64() * 10) - 5
				price := basePrices[symbol] + fluctuation
				seqCounters[symbol]++

				update := models.StockUpdate{
					Symbol:    symbol,
					Price:     price,
					Timestamp: time.Now().UnixMicro(),
					SeqID:     seqCounters[symbol],
				}

				payload, err := json.Marshal(update)
				if err != nil {
					logger.Error("JSON Marshal Error", zap.Error(err))
					continue
				}

				// Write to Kafka (Async due to writer config)
				err = writer.WriteMessages(ctx, kafka.Message{
					Key:   []byte(symbol), // Key ensures partition ordering
					Value: payload,
				})

				if err != nil {
					logger.Error("Kafka Write Error", zap.Error(err))
				} else {
					// Debug level helps reduce log spam in production
					logger.Debug("Sent update", zap.String("symbol", symbol), zap.Float64("price", price))
				}

				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// 7. Wait for Shutdown Signal
	<-sigChan
	logger.Info("Shutdown signal received")
	cancel() // Stop the generation loop

	// 8. Flush Kafka Buffer (CRITICAL)
	if err := writer.Close(); err != nil {
		logger.Error("Error closing Kafka writer", zap.Error(err))
	} else {
		logger.Info("Kafka writer closed cleanly")
	}
}

func createTopic(brokerAddress, topicName string) {
	conn, err := kafka.Dial("tcp", brokerAddress)
	if err != nil {
		logger.Warn("Failed to dial leader for topic creation", zap.Error(err))
		return
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		logger.Warn("Failed to connect to controller", zap.Error(err))
		return
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		logger.Warn("Failed to dial controller", zap.Error(err))
		return
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{{
		Topic:             topicName,
		NumPartitions:     4, // Increased partitions for better concurrency
		ReplicationFactor: 1,
	}}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		logger.Debug("Topic creation info", zap.Error(err))
	}
}
