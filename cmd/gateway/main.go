package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/shubham-shewale/stock-watchlist/pkg/config"
)

const (
	keyPrefix     = "stock:"
	channelPrefix = "prices."
)

var (
	logger           *zap.Logger
	totalConnections int64
	globalRdb        *redis.Client
)

const rateLimitScript = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local rate = 1
local burst = 5
local fields = redis.call('HGETALL', key)
local tokens = 0
local last_refill = 0
if #fields > 0 then
    for i=1, #fields, 2 do
        if fields[i] == 'tokens' then tokens = tonumber(fields[i+1]) end
        if fields[i] == 'last_refill' then last_refill = tonumber(fields[i+1]) end
    end
end
local elapsed = now - last_refill
tokens = math.min(burst, tokens + elapsed * rate)
if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)
    return 1
else
    return 0
end
`

func allowRequest(ip string) bool {
	key := "ratelimit:" + ip
	now := time.Now().Unix()
	result, err := globalRdb.Eval(context.Background(), rateLimitScript, []string{key}, now).Result()
	if err != nil {
		logger.Warn("Rate limit check failed, allowing request", zap.Error(err))
		return true
	}
	return result.(int64) == 1
}

var validTickers map[string]bool

var (
	writeWait      time.Duration
	pongWait       time.Duration
	pingPeriod     time.Duration
	sendBufferSize int
)

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

type Hub struct {
	subscribers map[string]map[*Client]bool
	clientSubs  map[*Client][]string
	redisClient *redis.Client
	redisPubSub *redis.PubSub
	refCount    map[string]int
	mu          sync.RWMutex
}

func NewHub(rdb *redis.Client) *Hub {
	pubsub := rdb.Subscribe(context.Background())
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
		// Optimization: Split channel string instead of JSON parsing
		parts := strings.Split(msg.Channel, ".")
		if len(parts) < 2 {
			logger.Warn("Invalid Redis channel format", zap.String("channel", msg.Channel))
			continue
		}
		symbol := parts[1]
		if symbol == "" {
			logger.Warn("Empty symbol in Redis channel", zap.String("channel", msg.Channel))
			continue
		}
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

		// Subscribe to Redis channel only when first client requests symbol
		if h.refCount[sym] == 1 {
			channelName := channelPrefix + sym
			logger.Info("Subscribing to Redis", zap.String("channel", channelName))
			if err := h.redisPubSub.Subscribe(ctx, channelName); err != nil {
				logger.Error("Failed to subscribe to Redis channel", zap.String("channel", channelName), zap.Error(err))
			}
		}
	}
	atomic.AddInt64(&totalConnections, 1)
	logger.Info("Client registered", zap.Strings("symbols", symbols), zap.Int("total_clients", len(h.clientSubs)))
}

func (h *Hub) Unsubscribe(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	symbols, ok := h.clientSubs[client]
	if !ok {
		return // Already unsubscribed, do nothing.
	}
	for _, sym := range symbols {
		delete(h.subscribers[sym], client)
		h.refCount[sym]--

		if h.refCount[sym] <= 0 {
			channelName := channelPrefix + sym
			logger.Info("Unsubscribing from Redis", zap.String("channel", channelName))
			// Run this in background to avoid holding the Hub lock during network IO
			go func(c string) {
				if err := h.redisPubSub.Unsubscribe(context.Background(), c); err != nil {
					logger.Error("Redis Unsubscribe Error", zap.Error(err))
				}
			}(channelName)
			delete(h.subscribers, sym)
			delete(h.refCount, sym)
		}
	}

	delete(h.clientSubs, client)
	close(client.send)
	atomic.AddInt64(&totalConnections, -1)
	logger.Info("Client unsubscribed cleanly", zap.Int("remaining_clients", len(h.clientSubs)))
}

func (h *Hub) Broadcast(symbol string, payload []byte) {
	h.mu.RLock()
	var slowClients []*Client
	if clients, ok := h.subscribers[symbol]; ok {
		for client := range clients {
			select {
			case client.send <- payload:
			default:
				slowClients = append(slowClients, client)
			}
		}
	}
	h.mu.RUnlock()

	// Unsubscribe slow clients after releasing lock to avoid deadlock
	for _, client := range slowClients {
		go h.Unsubscribe(client)
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

var globalCfg *config.Config

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	globalCfg = cfg

	logger, err = config.NewLogger(cfg.Logger)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer logger.Sync()

	validTickers = make(map[string]bool)
	for _, t := range cfg.Gateway.ValidTickers {
		validTickers[t] = true
	}
	writeWait = cfg.Gateway.Timeouts.WriteWait
	pongWait = cfg.Gateway.Timeouts.PongWait
	pingPeriod = cfg.Gateway.Timeouts.PingPeriod
	sendBufferSize = cfg.Gateway.Timeouts.SendBufferSize

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Fatal("Redis connect error", zap.Error(err))
	}

	globalRdb = rdb
	hub := NewHub(rdb)
	go hub.RunRedisLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		wsHandler(hub, w, r)
	})

	srv := &http.Server{Addr: cfg.App.Port, Handler: mux}

	go func() {
		logger.Info("Gateway Started", zap.String("port", cfg.App.Port))
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
	if !allowRequest(ip) {
		http.Error(w, "Too Many Requests", 429)
		return
	}

	if globalCfg != nil && globalCfg.Gateway.MaxConnections > 0 {
		if atomic.LoadInt64(&totalConnections) >= int64(globalCfg.Gateway.MaxConnections) {
			http.Error(w, "Connection limit exceeded", 429)
			return
		}
	}

	if globalCfg != nil && len(globalCfg.Gateway.AllowedOrigins) > 0 {
		origin := r.Header.Get("Origin")
		if origin != "" {
			allowed := false
			for _, allowedOrigin := range globalCfg.Gateway.AllowedOrigins {
				if allowedOrigin == "*" || origin == allowedOrigin {
					allowed = true
					break
				}
			}
			if !allowed {
				http.Error(w, "Origin not allowed", 403)
				return
			}
		}
	}

	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		return
	}

	client := &Client{conn: conn, hub: hub, send: make(chan []byte, sendBufferSize)}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	payload, err := wsutil.ReadClientText(conn)
	if err != nil {
		logger.Warn("Failed to read client message", zap.Error(err))
		conn.Close()
		return
	}

	var symbols []string
	if err := json.Unmarshal(payload, &symbols); err != nil {
		logger.Warn("Invalid JSON from client", zap.Error(err), zap.String("payload", string(payload)))
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
	defer func() {
		if r := recover(); r != nil {
			logger.Warn("Snapshot send aborted (Client disconnected)", zap.Any("reason", r))
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	keys := make([]string, len(symbols))
	for i, sym := range symbols {
		keys[i] = keyPrefix + sym
	}

	results, err := rdb.MGet(ctx, keys...).Result()
	if err != nil {
		logger.Error("Snapshot MGet failed", zap.Error(err))
		return
	}

	for i, val := range results {
		payload, ok := val.(string)
		if !ok || payload == "" {
			continue // No data for this symbol yet
		}

		select {
		case client.send <- []byte(payload):
		default:
			logger.Warn("Snapshot dropped (Buffer full)",
				zap.String("symbol", symbols[i]),
				zap.String("remote_addr", client.conn.RemoteAddr().String()))
		}
	}
}
