package tests

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/shubham-shewale/stock-watchlist/cmd/generator/internal/generator"
	"github.com/shubham-shewale/stock-watchlist/cmd/generator/internal/testutils"
)

func TestGenerator_ComponentWiring(t *testing.T) {
	// This test simulates the "Main" loop but with a fake output

	logger := zap.NewNop()
	mockWriter := &testutils.MockKafkaWriter{}

	// Use Real Randomness/Clock to ensure "Production" implementation doesn't panic
	// (We override interfaces with Real structs wrapped in mocks if needed,
	// but here we can just use the MockClock to make the test fast)
	mockClock := &testutils.MockClock{CurrentTime: time.Now()}
	mockRand := &testutils.MockRand{ValInt: 0, ValFloat: 0.9}

	tickers := []string{"MSFT", "GOOG"}
	basePrices := map[string]float64{"MSFT": 300.0, "GOOG": 2000.0}

	gen := generator.NewStockGenerator(logger, mockWriter, tickers, basePrices, mockRand, mockClock)

	// We use a context that cancels after "3 virtual ticks"
	// Since MockClock.Sleep just advances time, the loop runs as fast as CPU allows
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(10 * time.Millisecond) // Let it generate a few
		cancel()
	}()

	gen.Run(ctx)

	mockWriter.Mu.Lock()
	defer mockWriter.Mu.Unlock()

	if len(mockWriter.Messages) == 0 {
		t.Error("Generator failed to produce any messages in component test")
	}

	// Verify we got both tickers eventually (statistically probable with real rand,
	// but with MockRand we control it. MockRand returns 0 -> Index 0 -> MSFT)
	// So we expect only MSFT.
	for _, msg := range mockWriter.Messages {
		if string(msg.Key) != "MSFT" {
			t.Errorf("Expected MSFT based on MockRand, got %s", string(msg.Key))
		}
	}
}
