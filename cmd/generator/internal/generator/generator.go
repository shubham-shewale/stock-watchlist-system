package generator

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/shubham-shewale/stock-watchlist/pkg/models"
)

type StockGenerator struct {
	logger      *zap.Logger
	writer      KafkaWriter
	tickers     []string
	basePrices  map[string]float64
	rand        Rand
	clock       Clock
	seqCounters map[string]int64
}

func NewStockGenerator(
	logger *zap.Logger,
	writer KafkaWriter,
	tickers []string,
	basePrices map[string]float64,
	rnd Rand,
	clock Clock,
) *StockGenerator {
	return &StockGenerator{
		logger:      logger,
		writer:      writer,
		tickers:     tickers,
		basePrices:  basePrices,
		rand:        rnd,
		clock:       clock,
		seqCounters: make(map[string]int64),
	}
}

func (sg *StockGenerator) Run(ctx context.Context) {
	sg.logger.Info("Generator Started", zap.Strings("tickers", sg.tickers))

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if len(sg.tickers) == 0 {
				sg.clock.Sleep(1 * time.Second)
				continue
			}

			symbol := sg.tickers[sg.rand.Intn(len(sg.tickers))]
			fluctuation := (sg.rand.Float64() * 10) - 5
			price := sg.basePrices[symbol] + fluctuation
			sg.seqCounters[symbol]++

			update := models.StockUpdate{
				Symbol:    symbol,
				Price:     price,
				Timestamp: sg.clock.Now().UnixMicro(),
				SeqID:     sg.seqCounters[symbol],
			}

			payload, _ := json.Marshal(update) // Error ignored for simplicity in loop

			err := sg.writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(symbol),
				Value: payload,
			})

			if err != nil {
				sg.logger.Error("Kafka Write Error", zap.Error(err))
			}

			sg.clock.Sleep(100 * time.Millisecond)
		}
	}
}
