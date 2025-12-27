package generator_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/shubham-shewale/stock-watchlist/cmd/generator/internal/generator"
	"github.com/shubham-shewale/stock-watchlist/cmd/generator/internal/testutils"
	"github.com/shubham-shewale/stock-watchlist/pkg/models"
)

func TestGenerator_Logic(t *testing.T) {
	logger := zap.NewNop()
	mockWriter := &testutils.MockKafkaWriter{}

	// Fix Randomness: Always pick Index 0 (AAPL), Always return 0.5 fluctuation
	mockRand := &testutils.MockRand{ValInt: 0, ValFloat: 0.5}

	// Fix Time: Start at Epoch
	mockClock := &testutils.MockClock{CurrentTime: time.Unix(0, 0)}

	tickers := []string{"AAPL"}
	basePrices := map[string]float64{"AAPL": 100.0}

	gen := generator.NewStockGenerator(logger, mockWriter, tickers, basePrices, mockRand, mockClock)

	// Since MockClock.Sleep advances time instantly, we run in a goroutine and cancel quickly
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	gen.Run(ctx)

	mockWriter.Mu.Lock()
	defer mockWriter.Mu.Unlock()

	if len(mockWriter.Messages) == 0 {
		t.Fatal("Expected messages to be generated")
	}

	var update models.StockUpdate
	err := json.Unmarshal(mockWriter.Messages[0].Value, &update)
	if err != nil {
		t.Fatalf("Generated invalid JSON: %v", err)
	}

	if update.Symbol != "AAPL" {
		t.Errorf("Expected AAPL, got %s", update.Symbol)
	}
	if update.SeqID != 1 {
		t.Errorf("Expected SeqID 1, got %d", update.SeqID)
	}

	// logic: (0.5 * 10) - 5 = 0 fluctuation. Price should equal base price 100.0
	// Wait, code is: (rand.Float64() * 10) - 5
	// If MockFloat is 0.5 -> (0.5 * 10) - 5 = 0.
	// So Price should be exactly 100.0
	if update.Price != 100.0 {
		t.Errorf("Expected Price 100.0, got %f", update.Price)
	}
}

func TestTopicCreator_Flow(t *testing.T) {
	logger := zap.NewNop()
	mockDialer := &testutils.MockKafkaDialer{} // Will auto-create ConnSpy
	mockClock := &testutils.MockClock{}

	tc := generator.NewTopicCreator(logger, mockDialer, mockClock)

	tc.Create([]string{"broker:9092"}, "my-topic")

	if mockDialer.ConnSpy == nil {
		t.Fatal("Dialer was never called")
	}

	if len(mockDialer.ConnSpy.CreatedTopics) == 0 {
		t.Error("No topics created")
	}

	if mockDialer.ConnSpy.CreatedTopics[0] != "my-topic" {
		t.Errorf("Expected topic 'my-topic', got %s", mockDialer.ConnSpy.CreatedTopics[0])
	}
}
