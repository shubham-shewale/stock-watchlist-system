package testutils

import (
	"context"
	"sync"
	"testing"

	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/protocol"
)

// MockClient simulates a connected websocket client
type MockClient struct {
	IDVal    string
	Messages []protocol.WSResponse // Stores decoded JSON messages
	RawBytes []string              // Stores raw bytes
	Closed   bool
	Mu       sync.Mutex
}

func NewMockClient(id string) *MockClient {
	return &MockClient{IDVal: id, Messages: make([]protocol.WSResponse, 0)}
}

func (m *MockClient) ID() string { return m.IDVal }

func (m *MockClient) Close() {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.Closed = true
}

func (m *MockClient) SendJSON(v interface{}) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	// If it's a response, store it
	if resp, ok := v.(protocol.WSResponse); ok {
		m.Messages = append(m.Messages, resp)
	}
}

func (m *MockClient) SendBytes(b []byte) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.RawBytes = append(m.RawBytes, string(b))
}

func (m *MockClient) LastMsgType() string {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if len(m.Messages) == 0 {
		return ""
	}
	return m.Messages[len(m.Messages)-1].Type
}

// MockPriceStore simulates Redis
type MockPriceStore struct {
	SubscribedChannels map[string]int // symbol -> count
	Mu                 sync.Mutex
}

func NewMockStore() *MockPriceStore {
	return &MockPriceStore{SubscribedChannels: make(map[string]int)}
}

func (m *MockPriceStore) GetSnapshots(ctx context.Context, symbols []string) ([]string, error) {
	return []string{`{"symbol":"AAPL","price":150}`}, nil
}

func (m *MockPriceStore) SubscribeToFeed(ctx context.Context, symbol string) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.SubscribedChannels[symbol]++
	return nil
}

func (m *MockPriceStore) UnsubscribeFromFeed(ctx context.Context, symbol string) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.SubscribedChannels[symbol]--
	if m.SubscribedChannels[symbol] <= 0 {
		delete(m.SubscribedChannels, symbol)
	}
	return nil
}

func (m *MockPriceStore) RunPubSub(ctx context.Context, onMessage func(channel string, payload string)) {
	// No-op for unit tests
}

func (m *MockPriceStore) Close() error { return nil }

func AssertTrue(t *testing.T, condition bool, msg string) {
	if !condition {
		t.Errorf("Assertion failed: %s", msg)
	}
}
