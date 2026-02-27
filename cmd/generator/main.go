package main

import (
	"context"
	"math/rand"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.uber.org/zap"

	"github.com/shubham-shewale/stock-watchlist/cmd/generator/internal/generator"
	"github.com/shubham-shewale/stock-watchlist/pkg/config"
)

func main() {
	cfg, _ := config.LoadConfig()
	logger, _ := config.NewLogger(cfg.Logger)
	defer logger.Sync()

	clock := generator.RealClock{}
	dialer := &generator.RealKafkaDialer{Dialer: &kafka.Dialer{Timeout: 10 * time.Second}}

	tc := generator.NewTopicCreator(logger, dialer, clock)
	tc.Create(cfg.Kafka.Brokers, cfg.Kafka.Topic)

	writer := &kafka.Writer{
		Addr:        kafka.TCP(cfg.Kafka.Brokers...),
		Topic:       cfg.Kafka.Topic,
		Balancer:    &kafka.LeastBytes{},
		Compression: compress.Snappy,
		Async:       true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				// Failed to deliver some messages, Broker down !!
				logger.Error("Kafka Write Error", zap.Error(err))
			}
		},
	}

	source := rand.NewSource(time.Now().UnixNano())
	rnd := generator.RealRand{Rand: rand.New(source)}

	gen := generator.NewStockGenerator(logger, writer, cfg.Generator.Tickers, cfg.Generator.BasePrices, rnd, clock)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go gen.Run(ctx)

	<-ctx.Done()
	logger.Info("Shutting down...")
	writer.Close()
}
