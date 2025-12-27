package generator

import (
	"context"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

// for deterministic testing
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
}

// for deterministic values
type Rand interface {
	Intn(n int) int
	Float64() float64
}

type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type KafkaDialer interface {
	DialContext(ctx context.Context, network, address string) (KafkaConn, error)
}

type KafkaConn interface {
	Controller() (kafka.Broker, error)
	Close() error
	CreateTopics(topics ...kafka.TopicConfig) error
	ReadPartitions(topics ...string) ([]kafka.Partition, error)
}


type RealClock struct{}

func (RealClock) Now() time.Time        { return time.Now() }
func (RealClock) Sleep(d time.Duration) { time.Sleep(d) }

type RealRand struct{ *rand.Rand }

func (r RealRand) Intn(n int) int   { return r.Rand.Intn(n) }
func (r RealRand) Float64() float64 { return r.Rand.Float64() }

// RealKafkaConn adapts a *kafka.Conn to our interface
type RealKafkaConn struct{ *kafka.Conn }

func (c *RealKafkaConn) Controller() (kafka.Broker, error) { return c.Conn.Controller() }
func (c *RealKafkaConn) Close() error                      { return c.Conn.Close() }
func (c *RealKafkaConn) CreateTopics(topics ...kafka.TopicConfig) error {
	return c.Conn.CreateTopics(topics...)
}
func (c *RealKafkaConn) ReadPartitions(topics ...string) ([]kafka.Partition, error) {
	return c.Conn.ReadPartitions(topics...)
}

// RealKafkaDialer adapts *kafka.Dialer
type RealKafkaDialer struct{ *kafka.Dialer }

func (d *RealKafkaDialer) DialContext(ctx context.Context, network, address string) (KafkaConn, error) {
	conn, err := d.Dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &RealKafkaConn{Conn: conn}, nil
}
