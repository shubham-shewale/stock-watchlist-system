package repository

import (
	"context"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

const (
	keyPrefix     = "stock:"
	channelPrefix = "prices."
)

// Compile-time check to ensure RedisStore implements PriceStore
var _ PriceStore = (*RedisStore)(nil)

type RedisStore struct {
	client *redis.Client
	pubsub *redis.PubSub
	mu     sync.Mutex // Protects access to pubsub if needed
}

func NewRedisStore(client *redis.Client) *RedisStore {
	ps := client.Subscribe(context.Background())
	return &RedisStore{
		client: client,
		pubsub: ps,
	}
}

// GetSnapshots fetches the latest static price for a list of symbols (MGET)
func (r *RedisStore) GetSnapshots(ctx context.Context, symbols []string) ([]string, error) {
	if len(symbols) == 0 {
		return nil, nil
	}

	keys := make([]string, len(symbols))
	for i, sym := range symbols {
		keys[i] = keyPrefix + sym
	}

	results, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	var snapshots []string
	for _, val := range results {
		if payload, ok := val.(string); ok && payload != "" {
			snapshots = append(snapshots, payload)
		}
	}
	return snapshots, nil
}

// SubscribeToFeed tells Redis we want to listen to this channel
func (r *RedisStore) SubscribeToFeed(ctx context.Context, symbol string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	channel := channelPrefix + symbol
	return r.pubsub.Subscribe(ctx, channel)
}

// UnsubscribeFromFeed tells Redis to stop sending messages for this channel
func (r *RedisStore) UnsubscribeFromFeed(ctx context.Context, symbol string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	channel := channelPrefix + symbol
	return r.pubsub.Unsubscribe(ctx, channel)
}

// RunPubSub is a blocking loop that reads messages from Redis and triggers the callback
func (r *RedisStore) RunPubSub(ctx context.Context, onMessage func(channel string, payload string)) {
	ch := r.pubsub.Channel()

	for msg := range ch {
		parts := strings.Split(msg.Channel, ".")
		if len(parts) < 2 {
			continue
		}

		// We pass the raw symbol (from channel name) and the payload to the callback
		// Ideally, we pass just the symbol.
		// The Hub expects: Broadcast(symbol string, payload string)
		onMessage(parts[1], msg.Payload)
	}
}

func (r *RedisStore) Close() error {
	if err := r.pubsub.Close(); err != nil {
		return err
	}
	return r.client.Close()
}
