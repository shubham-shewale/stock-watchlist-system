package testutils

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)


type MockKafkaReader struct {
	Messages []kafka.Message
	Index    int
	Mu       sync.Mutex
	// Closed simulates a closed connection or end of stream
	Closed bool
}

func (m *MockKafkaReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	if m.Closed {
		return kafka.Message{}, io.EOF
	}

	if m.Index >= len(m.Messages) {
		// Block or return error to simulate "waiting" or "end of test"
		// Returning DeadlineExceeded is a clean way to stop the processor loop in tests
		return kafka.Message{}, context.DeadlineExceeded
	}

	msg := m.Messages[m.Index]
	m.Index++
	return msg, nil
}

func (m *MockKafkaReader) Close() error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.Closed = true
	return nil
}


type MockPipeline struct {
	redis.Pipeliner // Embed interface to satisfy missing methods like ACLCat, etc.

	ExecCount    int
	RecordedCmds []string // <--- RENAMED (was Cmds)
	Mu           sync.Mutex
}

func (m *MockPipeline) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.RecordedCmds = append(m.RecordedCmds, "SET "+key) // <--- UPDATED
	return redis.NewStatusCmd(ctx)
}

func (m *MockPipeline) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.RecordedCmds = append(m.RecordedCmds, "PUBLISH "+channel) // <--- UPDATED
	return redis.NewIntCmd(ctx)
}

func (m *MockPipeline) Exec(ctx context.Context) ([]redis.Cmder, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.ExecCount++
	return nil, nil
}


type MockRedisClient struct {
	PipelineSpy *MockPipeline
}

func NewMockRedisClient() *MockRedisClient {
	return &MockRedisClient{PipelineSpy: &MockPipeline{}}
}

func (m *MockRedisClient) Pipeline() redis.Pipeliner {
	return m.PipelineSpy
}

func (m *MockRedisClient) Ping(ctx context.Context) *redis.StatusCmd {
	return redis.NewStatusCmd(ctx)
}

func (m *MockRedisClient) Close() error { return nil }
