package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/shubham-shewale/stock-watchlist/cmd/processor/internal/processor"
	"github.com/shubham-shewale/stock-watchlist/pkg/config"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	logger, err := config.NewLogger(cfg.Logger)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Fatal("Failed to connect to Redis", zap.Error(err))
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           cfg.Kafka.Brokers,
		Topic:             cfg.Kafka.Topic,
		GroupID:           cfg.Kafka.GroupID,
		MinBytes:          200,
		MaxBytes:          10e6,
		MaxWait:           200 * time.Millisecond,
		CommitInterval:    1,
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    10 * time.Second,
	})

	// We pass the concrete infrastructure implementations to the domain logic
	proc := processor.NewProcessor(cfg, logger, rdb, reader)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := proc.Run(ctx); err != nil {
		logger.Error("Processor Run Error", zap.Error(err))
	}

	logger.Info("Closing Kafka Reader...")
	if err := reader.Close(); err != nil {
		logger.Error("Error closing reader", zap.Error(err))
	}

	logger.Info("Closing Redis...")
	if err := rdb.Close(); err != nil {
		logger.Error("Error closing Redis", zap.Error(err))
	}

	logger.Info("Processor exited cleanly")
	logger.Sync()
}
