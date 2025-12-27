package processor_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/shubham-shewale/stock-watchlist/cmd/processor/internal/processor"
	"github.com/shubham-shewale/stock-watchlist/cmd/processor/internal/testutils"
	"github.com/shubham-shewale/stock-watchlist/pkg/config"
	"github.com/shubham-shewale/stock-watchlist/pkg/models"
)

func TestProcessor_WorkerLogic(t *testing.T) {
	updates := []models.StockUpdate{
		{Symbol: "AAPL", Price: 100.0, SeqID: 1},
		{Symbol: "AAPL", Price: 100.0, SeqID: 1},
		{Symbol: "AAPL", Price: 101.0, SeqID: 2},
		{Symbol: "TSLA", Price: 900.0, SeqID: 1},
	}

	var msgs []kafka.Message
	for _, u := range updates {
		val, _ := json.Marshal(u)
		msgs = append(msgs, kafka.Message{
			Key:   []byte(u.Symbol),
			Value: val,
		})
	}

	mockReader := &testutils.MockKafkaReader{Messages: msgs}
	mockRedis := testutils.NewMockRedisClient()
	logger := zap.NewNop()

	cfg := &config.Config{}
	cfg.Processor.NumWorkers = 2

	proc := processor.NewProcessor(cfg, logger, mockRedis, mockReader)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := proc.Run(ctx)
	if err != nil {
		t.Logf("Processor stopped: %v", err)
	}

	pipeline := mockRedis.PipelineSpy
	pipeline.Mu.Lock()
	defer pipeline.Mu.Unlock()

	if pipeline.ExecCount != 3 {
		t.Errorf("Expected 3 pipeline executions, got %d", pipeline.ExecCount)
	}

	hasAAPL := false
	hasTSLA := false

	// Check RecordedCmds instead of Cmds
	for _, cmd := range pipeline.RecordedCmds {
		if cmd == "SET stock:AAPL" {
			hasAAPL = true
		}
		if cmd == "SET stock:TSLA" {
			hasTSLA = true
		}
	}

	if !hasAAPL {
		t.Error("Missing Redis command for AAPL")
	}
	if !hasTSLA {
		t.Error("Missing Redis command for TSLA")
	}
}

func TestProcessor_InvalidJSON(t *testing.T) {
	msgs := []kafka.Message{
		{Key: []byte("AAPL"), Value: []byte("{broken-json")},
	}

	mockReader := &testutils.MockKafkaReader{Messages: msgs}
	mockRedis := testutils.NewMockRedisClient()

	proc := processor.NewProcessor(&config.Config{Processor: config.ProcessorConfig{NumWorkers: 1}}, zap.NewNop(), mockRedis, mockReader)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	proc.Run(ctx)

	if mockRedis.PipelineSpy.ExecCount > 0 {
		t.Error("Should not execute Redis commands for invalid JSON")
	}
}
