package testutils

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/shubham-shewale/stock-watchlist/cmd/generator/internal/generator"
)


type MockKafkaWriter struct {
	Messages   []kafka.Message
	Mu         sync.Mutex
	ShouldFail bool
}

func (m *MockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if m.ShouldFail {
		return errors.New("kafka error")
	}
	m.Messages = append(m.Messages, msgs...)
	return nil
}

func (m *MockKafkaWriter) Close() error { return nil }


type MockClock struct {
	CurrentTime time.Time
}

func (m *MockClock) Now() time.Time        { return m.CurrentTime }
func (m *MockClock) Sleep(d time.Duration) { m.CurrentTime = m.CurrentTime.Add(d) }

type MockRand struct {
	ValInt   int
	ValFloat float64
}

func (m *MockRand) Intn(n int) int   { return m.ValInt }
func (m *MockRand) Float64() float64 { return m.ValFloat }


type MockKafkaConn struct {
	CreatedTopics []string
}

func (m *MockKafkaConn) Controller() (kafka.Broker, error) {
	return kafka.Broker{Host: "localhost", Port: 9092}, nil
}
func (m *MockKafkaConn) Close() error { return nil }
func (m *MockKafkaConn) CreateTopics(topics ...kafka.TopicConfig) error {
	for _, t := range topics {
		m.CreatedTopics = append(m.CreatedTopics, t.Topic)
	}
	return nil
}
func (m *MockKafkaConn) ReadPartitions(topics ...string) ([]kafka.Partition, error) {
	// Simulate "Ready" state immediately
	return []kafka.Partition{{ID: 0}}, nil
}

type MockKafkaDialer struct {
	ConnSpy *MockKafkaConn
}

func (m *MockKafkaDialer) DialContext(ctx context.Context, network, address string) (generator.KafkaConn, error) {
	if m.ConnSpy == nil {
		m.ConnSpy = &MockKafkaConn{}
	}
	return m.ConnSpy, nil
}
