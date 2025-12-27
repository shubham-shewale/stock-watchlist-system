package hub_test

import (
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/hub"
	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/protocol"
	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/testutils"
)

func setup() (*hub.Hub, *testutils.MockPriceStore) {
	store := testutils.NewMockStore()
	logger := zap.NewNop()
	return hub.NewHub(store, logger), store
}

var validTickers = map[string]bool{"AAPL": true, "TSLA": true, "GOOG": true}

func TestHub_Subscribe_Success(t *testing.T) {
	h, store := setup()
	client := testutils.NewMockClient("c1")

	req := protocol.WSRequest{
		Action:  "subscribe",
		Payload: protocol.RequestPayload{Symbols: []string{"AAPL"}},
		ID:      "req-1",
	}

	h.HandleCommand(client, req, validTickers)

	if client.LastMsgType() != "ack" {
		t.Errorf("Expected ack, got %s", client.LastMsgType())
	}

	if store.SubscribedChannels["AAPL"] != 1 {
		t.Errorf("Expected Redis subscription to AAPL")
	}
}

func TestHub_Subscribe_MixedValidity(t *testing.T) {
	h, _ := setup()
	client := testutils.NewMockClient("c1")

	req := protocol.WSRequest{
		Action:  "subscribe",
		Payload: protocol.RequestPayload{Symbols: []string{"AAPL", "INVALID_STOCK"}},
		ID:      "req-2",
	}

	h.HandleCommand(client, req, validTickers)

	lastMsg := client.Messages[len(client.Messages)-1]
	if lastMsg.Status != "success" {
		t.Errorf("Expected success for partial valid subscription")
	}
	if !strings.Contains(lastMsg.Message, "AAPL") {
		t.Errorf("Response should contain accepted symbol AAPL")
	}
	if strings.Contains(lastMsg.Message, "INVALID_STOCK") {
		t.Errorf("Response should NOT contain invalid symbol")
	}
}

func TestHub_Subscribe_Idempotency(t *testing.T) {
	h, store := setup()
	client := testutils.NewMockClient("c1")
	req := protocol.WSRequest{
		Action: "subscribe", Payload: protocol.RequestPayload{Symbols: []string{"AAPL"}},
	}

	h.HandleCommand(client, req, validTickers)

	h.HandleCommand(client, req, validTickers)

	// Redis should still have count 1, not 2
	if store.SubscribedChannels["AAPL"] != 1 {
		t.Errorf("Redis should only subscribe once per unique symbol")
	}
}

func TestHub_Unsubscribe_Logic(t *testing.T) {
	h, store := setup()
	client := testutils.NewMockClient("c1")

	h.HandleCommand(client, protocol.WSRequest{
		Action: "subscribe", Payload: protocol.RequestPayload{Symbols: []string{"AAPL", "TSLA"}},
	}, validTickers)

	h.HandleCommand(client, protocol.WSRequest{
		Action: "unsubscribe", Payload: protocol.RequestPayload{Symbols: []string{"AAPL"}},
	}, validTickers)

	if store.SubscribedChannels["AAPL"] != 0 {
		t.Errorf("Redis should be unsubscribed from AAPL")
	}
	if store.SubscribedChannels["TSLA"] != 1 {
		t.Errorf("Redis should still be subscribed to TSLA")
	}
}

func TestHub_Unsubscribe_NotSubscribed(t *testing.T) {
	h, _ := setup()
	client := testutils.NewMockClient("c1")

	h.HandleCommand(client, protocol.WSRequest{
		Action: "unsubscribe", Payload: protocol.RequestPayload{Symbols: []string{"GOOG"}},
		ID: "err-check",
	}, validTickers)

	lastMsg := client.Messages[len(client.Messages)-1]
	if lastMsg.Type != "error" {
		t.Errorf("Expected error response for unsubscribing non-watched symbol")
	}
}

func TestHub_UnsubscribeAll(t *testing.T) {
	h, store := setup()
	client := testutils.NewMockClient("c1")

	h.HandleCommand(client, protocol.WSRequest{
		Action: "subscribe", Payload: protocol.RequestPayload{Symbols: []string{"AAPL", "TSLA"}},
	}, validTickers)

	h.HandleCommand(client, protocol.WSRequest{Action: "unsubscribe_all"}, validTickers)

	if len(store.SubscribedChannels) != 0 {
		t.Errorf("Store should be empty after unsubscribe_all")
	}
}

func TestHub_RaceCondition(t *testing.T) {
	// Run with `go test -race ./...`
	h, _ := setup()
	client := testutils.NewMockClient("c1")

	go func() {
		h.HandleCommand(client, protocol.WSRequest{Action: "subscribe", Payload: protocol.RequestPayload{Symbols: []string{"AAPL"}}}, validTickers)
	}()
	go func() {
		h.HandleCommand(client, protocol.WSRequest{Action: "unsubscribe", Payload: protocol.RequestPayload{Symbols: []string{"AAPL"}}}, validTickers)
	}()
	go func() {
		h.Unregister(client)
	}()
}
