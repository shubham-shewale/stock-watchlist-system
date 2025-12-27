package processor

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Logger abstracts the logging library
type Logger interface {
	Info(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
	Debug(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Fatal(msg string, fields ...zap.Field)
	Sync() error
}

// KafkaReader abstracts the input stream
type KafkaReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

// RedisClient abstracts the output storage connection
type RedisClient interface {
	Ping(ctx context.Context) *redis.StatusCmd
	Pipeline() redis.Pipeliner
	Close() error
}

// Pipeliner abstracts the Redis pipeline operations
type Pipeliner interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd
	Exec(ctx context.Context) ([]redis.Cmder, error)
}
