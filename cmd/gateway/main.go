package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/shubham-shewale/stock-watchlist/pkg/config"
)

// --- Constants ---
const (
	keyPrefix      = "stock:"
	channelPrefix  = "prices."
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	sendBufferSize = 256
)

var (
	logger *zap.Logger

	// Metrics
	activeConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_active_connections",
		Help: "Current number of active WebSocket connections",
	})
	activeRedisSubscriptions = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_redis_subscriptions_active",
		Help: "Number of unique stock symbols the gateway is subscribed to in Redis",
	})
)

func init() {
	prometheus.MustRegister(activeConnections)
	prometheus.MustRegister(activeRedisSubscriptions)
}

// --- Rate Limiter ---
var ipLimiters = make(map[string]*rate.Limiter)
var muLimiter sync.Mutex

func getLimiter(ip string) *rate.Limiter {
	muLimiter.Lock()
	defer muLimiter.Unlock()
	limiter, exists := ipLimiters[ip]
	if !exists {
		limiter = rate.NewLimiter(1, 5)
		ipLimiters[ip] = limiter
	}
	return limiter
}

var validTickers = map[string]bool{
	"AAPL": true, "GOOG": true, "TSLA": true, "AMZN": true,
}

// --- Client Struct ---
type Client struct {
	conn net.Conn
	hub  *Hub
	send chan []byte
}

func (c *Client) readPump() {
	defer func() {
		c.hub.Unsubscribe(c)
		c.conn.Close()
	}()
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	for {
		header, err := ws.ReadHeader(c.conn)
		if err != nil {
			break
		}
		payload := make([]byte, header.Length)
		_, err = c.conn.Read(payload)
		if err != nil {
			break
		}
		if header.OpCode == ws.OpPong {
			c.conn.SetReadDeadline(time.Now().Add(pongWait))
			continue
		}
		if header.OpCode == ws.OpClose {
			break
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.Write(ws.CompiledClose)
				return
			}
			if err := wsutil.WriteServerText(c.conn, message); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := wsutil.WriteServerMessage(c.conn, ws.OpPing, nil); err != nil {
				return
			}
		}
	}
}

// --- HUB (Smart Router) ---
type Hub struct {
	subscribers map[string]map[*Client]bool
	clientSubs  map[*Client][]string
	redisClient *redis.Client
	redisPubSub *redis.PubSub
	refCount    map[string]int
	mu          sync.RWMutex
}

func NewHub(rdb *redis.Client) *Hub {
	pubsub := rdb.Subscribe(context.Background()) // Init empty
	return &Hub{
		subscribers: make(map[string]map[*Client]bool),
		clientSubs:  make(map[*Client][]string),
		redisClient: rdb,
		redisPubSub: pubsub,
		refCount:    make(map[string]int),
	}
}

func (h *Hub) RunRedisLoop() {
	ch := h.redisPubSub.Channel()
	for msg := range ch {
		// Optimization: Split string instead of Unmarshal JSON
		parts := strings.Split(msg.Channel, ".")
		if len(parts) < 2 {
			continue
		}
		symbol := parts[1]
		h.Broadcast(symbol, []byte(msg.Payload))
	}
}

func (h *Hub) Subscribe(client *Client, symbols []string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx := context.Background()
	h.clientSubs[client] = symbols

	for _, sym := range symbols {
		if _, ok := h.subscribers[sym]; !ok {
			h.subscribers[sym] = make(map[*Client]bool)
		}
		h.subscribers[sym][client] = true
		h.refCount[sym]++

		// Granular Subscription Logic
		if h.refCount[sym] == 1 {
			channelName := channelPrefix + sym
			logger.Info("Subscribing to Redis", zap.String("channel", channelName))
			if err := h.redisPubSub.Subscribe(ctx, channelName); err == nil {
				activeRedisSubscriptions.Inc()
			}
		}
	}
	activeConnections.Inc()
	logger.Info("Client registered", zap.Strings("symbols", symbols))
}

func (h *Hub) Unsubscribe(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx := context.Background()
	if symbols, ok := h.clientSubs[client]; ok {
		for _, sym := range symbols {
			delete(h.subscribers[sym], client)
			h.refCount[sym]--

			if h.refCount[sym] <= 0 {
				channelName := channelPrefix + sym
				logger.Info("Unsubscribing from Redis", zap.String("channel", channelName))
				if err := h.redisPubSub.Unsubscribe(ctx, channelName); err == nil {
					activeRedisSubscriptions.Dec()
				}
				delete(h.subscribers, sym)
				delete(h.refCount, sym)
			}
		}
		delete(h.clientSubs, client)
		close(client.send)
		activeConnections.Dec()
	}
}

func (h *Hub) Broadcast(symbol string, payload []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if clients, ok := h.subscribers[symbol]; ok {
		for client := range clients {
			select {
			case client.send <- payload:
			default:
				logger.Warn("Dropping slow client", zap.String("symbol", symbol))
				go h.Unsubscribe(client)
			}
		}
	}
}

func (h *Hub) Shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.redisPubSub.Close()
	for client := range h.clientSubs {
		payload := ws.NewCloseFrameBody(ws.StatusGoingAway, "Server Shutdown")
		_ = wsutil.WriteServerMessage(client.conn, ws.OpClose, payload)
		client.conn.Close()
	}
}

// --- Main ---
func main() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatal("Config error", zap.Error(err))
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Fatal("Redis connect error", zap.Error(err))
	}

	hub := NewHub(rdb)
	go hub.RunRedisLoop()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		wsHandler(hub, w, r)
	})

	srv := &http.Server{Addr: cfg.App.Port, Handler: mux}

	go func() {
		logger.Info("Gateway Started (Granular)", zap.String("port", cfg.App.Port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("HTTP Error", zap.Error(err))
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	hub.Shutdown()
	logger.Info("Exited cleanly")
}

func wsHandler(hub *Hub, w http.ResponseWriter, r *http.Request) {
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	if !getLimiter(ip).Allow() {
		http.Error(w, "Too Many Requests", 429)
		return
	}
	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		return
	}

	client := &Client{conn: conn, hub: hub, send: make(chan []byte, sendBufferSize)}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	payload, err := wsutil.ReadClientText(conn)
	if err != nil {
		conn.Close()
		return
	}

	var symbols []string
	if err := json.Unmarshal(payload, &symbols); err != nil {
		sendJsonError(conn, "Invalid JSON")
		conn.Close()
		return
	}

	var validSymbols []string
	for _, sym := range symbols {
		if validTickers[sym] {
			validSymbols = append(validSymbols, sym)
		}
	}
	if len(validSymbols) == 0 {
		sendJsonError(conn, "No valid symbols")
		conn.Close()
		return
	}

	hub.Subscribe(client, validSymbols)
	go sendSnapshot(hub.redisClient, client, validSymbols)
	go client.writePump()
	go client.readPump()
}

func sendJsonError(conn net.Conn, msg string) {
	errBytes, _ := json.Marshal(map[string]string{"error": msg})
	wsutil.WriteServerText(conn, errBytes)
}

func sendSnapshot(rdb *redis.Client, client *Client, symbols []string) {
	ctx := context.Background()
	keys := make([]string, len(symbols))
	for i, sym := range symbols {
		keys[i] = keyPrefix + sym
	}
	results, _ := rdb.MGet(ctx, keys...).Result()
	for _, val := range results {
		if payload, ok := val.(string); ok {
			select {
			case client.send <- []byte(payload):
			default:
				return
			}
		}
	}
}
