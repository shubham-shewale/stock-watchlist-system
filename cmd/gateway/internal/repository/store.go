package repository

import (
	"context"
)

type PriceStore interface {
	GetSnapshots(ctx context.Context, symbols []string) ([]string, error)
	SubscribeToFeed(ctx context.Context, symbol string) error
	UnsubscribeFromFeed(ctx context.Context, symbol string) error
	RunPubSub(ctx context.Context, onMessage func(channel string, payload string))
	Close() error
}

type RateLimiter interface {
	Allow(ip string) (bool, error)
}
