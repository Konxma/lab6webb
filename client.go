package main

import (
	"log"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

const (
	writeWait     = 5 * time.Second
	maxFrameBytes = 4096
)

var roomPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,32}$`)

// The lab requires cross-origin WebSocket clients. The default listener is local.
var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

type Client struct {
	hub    *Hub
	conn   *websocket.Conn
	send   chan []byte
	room   string
	sender string
}

func (h *Hub) serveWS(w http.ResponseWriter, r *http.Request) {
	room := strings.TrimSpace(r.URL.Query().Get("room"))
	if room == "" {
		room = "general"
	}
	if !roomPattern.MatchString(room) {
		http.Error(w, "Invalid room name", http.StatusBadRequest)
		return
	}
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		name = r.RemoteAddr
	}
	if !utf8.ValidString(name) || utf8.RuneCountInString(name) > 48 || strings.ContainsFunc(name, unicode.IsControl) {
		http.Error(w, "Invalid sender name", http.StatusBadRequest)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	client := &Client{hub: h, conn: conn, send: make(chan []byte, 256), room: room, sender: name}
	select {
	case h.register <- client:
	case <-h.ctx.Done():
		conn.Close()
		return
	}
	go client.writePump()
	client.readPump()
}

func (c *Client) readPump() {
	defer c.leave()
	c.conn.SetReadLimit(maxFrameBytes)
	if err := c.conn.SetReadDeadline(time.Now().Add(c.hub.pongWait)); err != nil {
		return
	}
	c.conn.SetPongHandler(func(string) error {
		log.Printf("[HEARTBEAT] Pong from %s (%s)", c.sender, c.room)
		return c.conn.SetReadDeadline(time.Now().Add(c.hub.pongWait))
	})
	for {
		kind, data, err := c.conn.ReadMessage()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Printf("[HEARTBEAT] Read deadline expired for %s (%s)", c.sender, c.room)
			}
			return
		}
		if kind != websocket.TextMessage || !utf8.Valid(data) {
			continue
		}
		message := strings.TrimSpace(string(data))
		if message == "" {
			continue
		}
		select {
		case c.hub.broadcast <- Message{c.room, c.sender, message, time.Now().Format(time.RFC3339)}:
		case <-c.hub.ctx.Done():
			return
		}
	}
}

func (c *Client) writePump() {
	defer c.leave()
	ticker := time.NewTicker(c.hub.pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case data, ok := <-c.send:
			if !ok || c.write(websocket.TextMessage, data) != nil {
				return
			}
		case <-ticker.C:
			if c.write(websocket.PingMessage, nil) != nil {
				return
			}
			log.Printf("[HEARTBEAT] Ping to %s (%s)", c.sender, c.room)
		}
	}
}

func (c *Client) write(kind int, data []byte) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return err
	}
	return c.conn.WriteMessage(kind, data)
}

func (c *Client) leave() {
	select {
	case c.hub.unregister <- c:
	case <-c.hub.ctx.Done():
	}
}
