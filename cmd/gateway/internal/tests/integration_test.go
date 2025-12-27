package tests

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gobwas/ws"
	"github.com/gorilla/websocket" // Using Gorilla for the test CLIENT
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/gateway"
	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/hub"
	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/repository"
)

func startServer(t *testing.T) (*httptest.Server, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	repo := repository.NewRedisStore(rdb)
	wsHub := hub.NewHub(repo, zap.NewNop())
	validTickers := map[string]bool{"AAPL": true, "MSFT": true}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		client := gateway.NewClient(conn, wsHub, zap.NewNop(), validTickers)
		client.Start()
	}))

	return server, mr
}

func connectWS(t *testing.T, serverURL string) *websocket.Conn {
	url := "ws" + strings.TrimPrefix(serverURL, "http")
	wsConn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Failed to connect to websocket: %v", err)
	}
	return wsConn
}

func TestEndToEnd_FullFlow(t *testing.T) {
	server, mr := startServer(t)
	defer server.Close()
	defer mr.Close()

	wsConn := connectWS(t, server.URL)
	defer wsConn.Close()

	subMsg := `{"action": "subscribe", "payload": {"symbols": ["AAPL"]}, "id": "t1"}`
	wsConn.WriteMessage(websocket.TextMessage, []byte(subMsg))

	_, msg, _ := wsConn.ReadMessage()
	if !strings.Contains(string(msg), "success") {
		t.Errorf("Expected subscription success, got: %s", msg)
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		mr.Publish("prices.AAPL", `{"symbol":"AAPL","price":150.5}`)
	}()

	wsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := wsConn.ReadMessage()
	if err != nil {
		t.Fatalf("Failed to receive broadcast: %v", err)
	}
	if !strings.Contains(string(msg), "150.5") {
		t.Errorf("Expected price 150.5, got: %s", msg)
	}

	unsubMsg := `{"action": "unsubscribe", "payload": {"symbols": ["AAPL"]}, "id": "t2"}`
	wsConn.WriteMessage(websocket.TextMessage, []byte(unsubMsg))

	_, msg, _ = wsConn.ReadMessage()
	if !strings.Contains(string(msg), "Unsubscribed") {
		t.Errorf("Expected unsubscribe ack, got: %s", msg)
	}
}

func TestEndToEnd_InvalidJSON(t *testing.T) {
	server, _ := startServer(t)
	defer server.Close()
	wsConn := connectWS(t, server.URL)
	defer wsConn.Close()

	wsConn.WriteMessage(websocket.TextMessage, []byte(`{ "action": "subsc`))

	_, msg, _ := wsConn.ReadMessage()
	if !strings.Contains(string(msg), "Invalid JSON") && !strings.Contains(string(msg), "error") {
		t.Errorf("Expected error message for bad JSON, got: %s", msg)
	}
}

func TestEndToEnd_MaxMessageSize(t *testing.T) {
	server, _ := startServer(t)
	defer server.Close()
	wsConn := connectWS(t, server.URL)
	defer wsConn.Close()

	hugePayload := strings.Repeat("a", 513*1024)
	hugeMsg := fmt.Sprintf(`{"action":"subscribe", "payload": {"symbols": ["%s"]}}`, hugePayload)

	err := wsConn.WriteMessage(websocket.TextMessage, []byte(hugeMsg))
	// Depending on timing, write might succeed, but Read should fail (Disconnect)
	if err == nil {
		// Try to read response, expect connection closed error
		wsConn.SetReadDeadline(time.Now().Add(1 * time.Second))
		_, _, err := wsConn.ReadMessage()
		if err == nil {
			t.Error("Server should have closed connection for huge message, but it stayed open")
		}
	}
}
