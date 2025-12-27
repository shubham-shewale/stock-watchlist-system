package gateway

import (
	"encoding/json"
	"io"
	"net"
	"strings"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"go.uber.org/zap"

	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/hub"
	"github.com/shubham-shewale/stock-watchlist/cmd/gateway/internal/protocol"
)

const (
	maxMessageSize = 512 * 1024
)

type ClientAdapter struct {
	conn         net.Conn
	hub          *hub.Hub
	send         chan []byte
	logger       *zap.Logger
	validTickers map[string]bool

	writeWait  time.Duration
	pongWait   time.Duration
	pingPeriod time.Duration
}

func NewClient(conn net.Conn, h *hub.Hub, logger *zap.Logger, validTickers map[string]bool) *ClientAdapter {
	return &ClientAdapter{
		conn:         conn,
		hub:          h,
		send:         make(chan []byte, 256),
		logger:       logger,
		validTickers: validTickers,
		writeWait:    5 * time.Second,
		pongWait:     60 * time.Second,
		pingPeriod:   50 * time.Second,
	}
}

func (c *ClientAdapter) Start() {
	go c.writePump()
	go c.readPump()
}

func (c *ClientAdapter) ID() string { return c.conn.RemoteAddr().String() }
func (c *ClientAdapter) Close()     { close(c.send) } // Only close channel, let writePump close conn

func (c *ClientAdapter) SendJSON(v interface{}) {
	b, err := json.Marshal(v)
	if err == nil {
		c.send <- b
	}
}

func (c *ClientAdapter) SendBytes(b []byte) {
	select {
	case c.send <- b:
	default:
		// Drop message if buffer full (Backpressure)
	}
}

func (c *ClientAdapter) readPump() {
	defer func() {
		c.hub.Unregister(c)
		c.conn.Close()
	}()

	c.conn.SetReadDeadline(time.Now().Add(c.pongWait))

	for {
		header, err := ws.ReadHeader(c.conn)
		if err != nil {
			break
		}

		if header.Length > int64(maxMessageSize) {
			c.logger.Warn("Msg too big", zap.Int64("size", header.Length))
			break
		}

		if !header.Fin {
			c.logger.Warn("Client sent fragmented message (not supported)")
			break
		}

		payload := make([]byte, header.Length)
		if _, err := io.ReadFull(c.conn, payload); err != nil {
			break
		}

		if header.Masked {
			ws.Cipher(payload, header.Mask, 0)
		}

		if header.OpCode == ws.OpClose {
			break
		}
		if header.OpCode == ws.OpPong {
			c.conn.SetReadDeadline(time.Now().Add(c.pongWait))
			continue
		}

		if header.OpCode == ws.OpText {
			var req protocol.WSRequest
			if err := json.Unmarshal(payload, &req); err != nil {
				c.SendJSON(protocol.WSResponse{Type: "error", Message: "Invalid JSON"})
				continue
			}

			for i, s := range req.Payload.Symbols {
				req.Payload.Symbols[i] = strings.ToUpper(strings.TrimSpace(s))
			}

			c.hub.HandleCommand(c, req, c.validTickers)
		}
	}
}

func (c *ClientAdapter) writePump() {
	ticker := time.NewTicker(c.pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(c.writeWait))
			if !ok {
				c.conn.Write(ws.CompiledClose)
				return
			}
			if err := wsutil.WriteServerText(c.conn, msg); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(c.writeWait))
			if err := wsutil.WriteServerMessage(c.conn, ws.OpPing, nil); err != nil {
				return
			}
		}
	}
}
