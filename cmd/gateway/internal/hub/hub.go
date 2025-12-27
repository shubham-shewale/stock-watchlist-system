package hub

import (
	"context"
	"fmt"
	"sync"

	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/protocol"
	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/repository"
	"go.uber.org/zap"
)

type ClientInterface interface {
	ID() string
	SendJSON(v interface{})
	SendBytes(b []byte)
	Close()
}

type Hub struct {
	subscribers map[string]map[ClientInterface]bool
	clientSubs  map[ClientInterface]map[string]bool

	store    repository.PriceStore
	logger   *zap.Logger
	mu       sync.RWMutex
	refCount map[string]int
}

func NewHub(store repository.PriceStore, logger *zap.Logger) *Hub {
	h := &Hub{
		subscribers: make(map[string]map[ClientInterface]bool),
		clientSubs:  make(map[ClientInterface]map[string]bool),
		store:       store,
		logger:      logger,
		refCount:    make(map[string]int),
	}

	go h.store.RunPubSub(context.Background(), h.Broadcast)

	return h
}

func (h *Hub) HandleCommand(client ClientInterface, req protocol.WSRequest, validTickers map[string]bool) {
	switch req.Action {
	case protocol.ActionSubscribe:
		h.handleSubscribe(client, req, validTickers)
	case protocol.ActionUnsubscribe:
		h.handleUnsubscribe(client, req)
	case protocol.ActionUnsubscribeAll:
		h.handleUnsubscribeAll(client, req)
	default:
		h.sendError(client, req.ID, "Unknown action: "+req.Action)
	}
}

func (h *Hub) handleSubscribe(client ClientInterface, req protocol.WSRequest, validTickers map[string]bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	var valid []string
	for _, s := range req.Payload.Symbols {
		if validTickers[s] {
			// Idempotency: Ignore if already subscribed
			if h.clientSubs[client] != nil && h.clientSubs[client][s] {
				continue
			}
			valid = append(valid, s)
		}
	}

	if len(valid) == 0 {
		h.sendError(client, req.ID, "No valid/new symbols provided")
		return
	}

	if h.clientSubs[client] == nil {
		h.clientSubs[client] = make(map[string]bool)
	}

	for _, sym := range valid {
		h.clientSubs[client][sym] = true
		if h.subscribers[sym] == nil {
			h.subscribers[sym] = make(map[ClientInterface]bool)
		}
		h.subscribers[sym][client] = true

		// Manage upstream subscription (Ref counting)
		h.refCount[sym]++
		if h.refCount[sym] == 1 {
			if err := h.store.SubscribeToFeed(context.Background(), sym); err != nil {
				h.logger.Error("Failed to subscribe upstream", zap.String("symbol", sym), zap.Error(err))
			}
		}
	}

	h.sendAck(client, req.ID, "success", fmt.Sprintf("Subscribed to %v", valid))

	// Send Snapshots (Async to avoid blocking lock)
	go func(targets []string) {
		snapshots, err := h.store.GetSnapshots(context.Background(), targets)
		if err == nil {
			for _, snap := range snapshots {
				client.SendBytes([]byte(snap))
			}
		}
	}(valid)
}

func (h *Hub) handleUnsubscribe(client ClientInterface, req protocol.WSRequest) {
	h.mu.Lock()
	defer h.mu.Unlock()

	var removed []string
	if subs, ok := h.clientSubs[client]; ok {
		for _, sym := range req.Payload.Symbols {
			if subs[sym] {
				delete(subs, sym)
				delete(h.subscribers[sym], client)
				removed = append(removed, sym)
				h.decreaseRefCount(sym)
			}
		}
	}

	if len(removed) > 0 {
		h.sendAck(client, req.ID, "success", fmt.Sprintf("Unsubscribed from %v", removed))
	} else {
		h.sendError(client, req.ID, fmt.Sprintf("Not subscribed to: %v", req.Payload.Symbols))
	}
}

func (h *Hub) handleUnsubscribeAll(client ClientInterface, req protocol.WSRequest) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if subs, ok := h.clientSubs[client]; ok {
		for sym := range subs {
			delete(h.subscribers[sym], client)
			h.decreaseRefCount(sym)
		}
		// Clear the map but keep the client registered
		h.clientSubs[client] = make(map[string]bool)
	}
	h.sendAck(client, req.ID, "success", "Unsubscribed from all symbols")
}

func (h *Hub) Unregister(client ClientInterface) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if subs, ok := h.clientSubs[client]; ok {
		for sym := range subs {
			delete(h.subscribers[sym], client)
			h.decreaseRefCount(sym)
		}
		delete(h.clientSubs, client)
	}
	client.Close()
}

func (h *Hub) Broadcast(symbol string, payload string) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if clients, ok := h.subscribers[symbol]; ok {
		msgBytes := []byte(payload)
		for client := range clients {
			client.SendBytes(msgBytes)
		}
	}
}

func (h *Hub) decreaseRefCount(symbol string) {
	h.refCount[symbol]--
	if h.refCount[symbol] <= 0 {
		if err := h.store.UnsubscribeFromFeed(context.Background(), symbol); err != nil {
			h.logger.Error("Failed to unsubscribe upstream", zap.String("symbol", symbol), zap.Error(err))
		}
		delete(h.refCount, symbol)
		delete(h.subscribers, symbol)
	}
}

func (h *Hub) sendAck(c ClientInterface, id, status, msg string) {
	c.SendJSON(protocol.WSResponse{Type: "ack", ID: id, Status: status, Message: msg})
}

func (h *Hub) sendError(c ClientInterface, id, msg string) {
	c.SendJSON(protocol.WSResponse{Type: "error", ID: id, Message: msg})
}
