package generator

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type TopicCreator struct {
	logger *zap.Logger
	dialer KafkaDialer
	clock  Clock
}

func NewTopicCreator(logger *zap.Logger, dialer KafkaDialer, clock Clock) *TopicCreator {
	return &TopicCreator{
		logger: logger,
		dialer: dialer,
		clock:  clock,
	}
}

func (tc *TopicCreator) Create(brokers []string, topicName string) {
	ctx := context.Background()
	var conn KafkaConn
	var err error

	for _, addr := range brokers {
		conn, err = tc.dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			break
		}
	}
	if err != nil {
		tc.logger.Warn("Failed to dial brokers", zap.Error(err))
		return
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		tc.logger.Warn("Failed to get controller", zap.Error(err))
		return
	}

	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := tc.dialer.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		tc.logger.Warn("Failed to dial controller", zap.Error(err))
		return
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     4,
		ReplicationFactor: 1,
	})

	if err != nil {
		tc.logger.Info("Topic creation finished (might already exist)", zap.Error(err))
	} else {
		tc.logger.Info("Topic creation request sent", zap.String("topic", topicName))
	}

	tc.waitForTopic(conn, topicName)
}

func (tc *TopicCreator) waitForTopic(conn KafkaConn, topicName string) {
	tc.logger.Info("Waiting for topic initialization...", zap.String("topic", topicName))
	for i := 0; i < 5; i++ {
		tc.clock.Sleep(200 * time.Millisecond) // Fast retry for tests
		partitions, err := conn.ReadPartitions(topicName)
		if err == nil && len(partitions) > 0 {
			tc.logger.Info("Topic is ready!", zap.Int("partitions", len(partitions)))
			return
		}
	}
	tc.logger.Warn("Timed out waiting for topic")
}
