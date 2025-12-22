package models

// StockUpdate represents a single market tick for a stock symbol
type StockUpdate struct {
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	Timestamp int64   `json:"timestamp"` // unix micro
	SeqID     int64   `json:"seq_id"`    // monotonic counter per symbol
}
