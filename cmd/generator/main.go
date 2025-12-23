package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	logger *zap.Logger
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

	createTopic(cfg.Kafka.Brokers, cfg.Kafka.Topic)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Kafka.Brokers...),
		Topic:    cfg.Kafka.Topic,
		Balancer: &kafka.LeastBytes{},
		// Optimization: Send batches to reduce network IO
		BatchSize:    100,
		BatchTimeout: 200 * time.Millisecond,
		Compression:  kafka.Snappy,
		Async:        true, // Non-blocking writes for high throughput
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				// Failed to deliver some messages, Broker down !!
				logger.Error("Kafka Write Error", zap.Error(err))
			}
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	seqCounters := make(map[string]int64)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	go func() {
		logger.Info("Generator Started", zap.Strings("brokers", cfg.Kafka.Brokers))

		for {
			select {
			case <-ctx.Done():
				return
			default:
				symbol := cfg.Generator.Tickers[r.Intn(len(cfg.Generator.Tickers))]
				fluctuation := (r.Float64() * 10) - 5
				price := cfg.Generator.BasePrices[symbol] + fluctuation
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

				err = writer.WriteMessages(ctx, kafka.Message{
					Key:   []byte(symbol), // Key ensures partition ordering for scalability
					Value: payload,
				})

				if err != nil {
					logger.Error("Kafka Write Error", zap.Error(err), zap.String("symbol", symbol), zap.Float64("price", price))
				} else {
					logger.Debug("Sent update", zap.String("symbol", symbol), zap.Float64("price", price), zap.Int64("seq_id", seqCounters[symbol]))
				}

				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	<-sigChan
	logger.Info("Shutdown signal received")
	cancel()

	if err := writer.Close(); err != nil {
		logger.Error("Error closing Kafka writer", zap.Error(err))
	} else {
		logger.Info("Kafka writer closed cleanly")
	}
}

func createTopic(brokers []string, topicName string) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}
	var conn *kafka.Conn
	var err error
	for _, addr := range brokers {
		conn, err = dialer.DialContext(context.Background(), "tcp", addr)
		if err == nil {
			break
		}
	}

	if err != nil {
		logger.Warn("Failed to dial any broker for topic creation", zap.Error(err))
		return
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		logger.Warn("Failed to get controller", zap.Error(err))
		return
	}

	controllerConn, err := dialer.DialContext(context.Background(), "tcp",
		net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		logger.Warn("Failed to dial controller", zap.Error(err))
		return
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{{
		Topic:             topicName,
		NumPartitions:     4,
		ReplicationFactor: 3,
	}}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		if checkErr, ok := err.(kafka.Error); ok && checkErr == kafka.TopicAlreadyExists {
			logger.Info("Topic already exists", zap.String("topic", topicName))
		} else {
			logger.Warn("Failed to create topic", zap.Error(err))
			return // If creation failed significantly, we should probably stop
		}
	} else {
		logger.Info("Topic creation request sent", zap.String("topic", topicName))
	}

	logger.Info("Waiting for topic metadata to propagate...", zap.String("topic", topicName))

	logger.Info("Waiting for topic to initialize...", zap.String("topic", topicName))

	maxRetries := 15
	for i := 0; i < maxRetries; i++ {
		// We allow the loop to sleep first to give Kafka a moment
		time.Sleep(1 * time.Second)

		// We check partitions. If this returns success, the topic is known.
		partitions, err := conn.ReadPartitions(topicName)

		if err == nil && len(partitions) > 0 {
			logger.Info("Topic is ready!", zap.String("topic", topicName), zap.Int("partitions", len(partitions)))
			return
		}

		logger.Debug("Topic not ready yet, retrying...", zap.String("topic", topicName))
	}

	logger.Warn("Timed out waiting for topic creation. Writes might fail initially.", zap.String("topic", topicName))
}
