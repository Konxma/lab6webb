package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func testServer(t *testing.T, ping, pong time.Duration) *httptest.Server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	hub := newHub(ctx, ping, pong)
	go hub.Run()
	server := httptest.NewServer(routes(hub))
	t.Cleanup(func() {
		cancel()
		<-hub.done
		server.Close()
	})
	return server
}

func TestMetricsStream(t *testing.T) {
	server := testServer(t, time.Minute, 2*time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/metrics/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 || res.Header.Get("Content-Type") != "text/event-stream" ||
		res.Header.Get("Cache-Control") != "no-cache" || res.Header.Get("Connection") != "keep-alive" {
		t.Fatalf("Unexpected SSE response: %d %v", res.StatusCode, res.Header)
	}
	scanner := bufio.NewScanner(res.Body)
	var previous time.Time
	for i := 0; i < 2; i++ {
		if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "data: ") {
			t.Fatalf("Missing SSE frame: %q (%v)", scanner.Text(), scanner.Err())
		}
		var current metrics
		if err := json.Unmarshal([]byte(strings.TrimPrefix(scanner.Text(), "data: ")), &current); err != nil {
			t.Fatal(err)
		}
		stamp, err := time.Parse(time.RFC3339, current.Time)
		if err != nil || !stamp.After(previous) || current.AllocMB <= 0 || current.Goroutines <= 0 {
			t.Fatalf("Invalid metrics: %+v", current)
		}
		previous = stamp
		if !scanner.Scan() || scanner.Text() != "" {
			t.Fatal("SSE frame lacks blank separator")
		}
	}
	cancel()
	if scanner.Scan() || scanner.Err() != context.Canceled {
		t.Fatalf("Expected canceled stream, got %v", scanner.Err())
	}
}

func wsClient(t *testing.T, server *httptest.Server, query string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/v1/ws" + query
	// Cross-origin connections are explicitly required by the assignment.
	conn, res, err := websocket.DefaultDialer.Dial(url, http.Header{"Origin": {"http://other.example"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("Expected upgrade status 101, got %d", res.StatusCode)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func sendText(t *testing.T, conn *websocket.Conn, text string) {
	t.Helper()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(text)); err != nil {
		t.Fatal(err)
	}
}

func readText(t *testing.T, conn *websocket.Conn, room, sender, payload string) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var msg Message
	if err := conn.ReadJSON(&msg); err != nil {
		t.Fatal(err)
	}
	if msg.Room != room || msg.Sender != sender || msg.Text != payload {
		t.Fatalf("Unexpected message: %+v", msg)
	}
}

func TestWebSocketRooms(t *testing.T) {
	server := testServer(t, time.Minute, 2*time.Minute)
	b := wsClient(t, server, "?room=alerts&name=Dispatcher")
	d := wsClient(t, server, "?room=general&name=Monitor")
	// Synchronize registrations using each client's own echo.
	for _, peer := range []struct {
		conn       *websocket.Conn
		room, name string
	}{{b, "alerts", "Dispatcher"}, {d, "general", "Monitor"}} {
		sendText(t, peer.conn, "ready")
		readText(t, peer.conn, peer.room, peer.name, "ready")
	}
	a := wsClient(t, server, "?room=alerts&name=Operator")
	c := wsClient(t, server, "?name=General")
	sendText(t, a, "Перегрів вузла")
	readText(t, a, "alerts", "Operator", "Перегрів вузла")
	readText(t, b, "alerts", "Operator", "Перегрів вузла")
	sendText(t, c, "Зв’язок перевірено")
	readText(t, c, "general", "General", "Зв’язок перевірено")
	readText(t, d, "general", "General", "Зв’язок перевірено")
	if err := b.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.ReadMessage(); err == nil {
		t.Fatal("General message crossed into alerts")
	}
}

func TestWebSocketHeartbeat(t *testing.T) {
	const pongWait = 800 * time.Millisecond
	server := testServer(t, 150*time.Millisecond, pongWait)
	responsive := wsClient(t, server, "?room=online")
	silent := wsClient(t, server, "?room=offline")
	gotPing := make(chan struct{}, 1)
	defaultPingHandler := responsive.PingHandler()
	responsive.SetPingHandler(func(data string) error {
		select {
		case gotPing <- struct{}{}:
		default:
		}
		return defaultPingHandler(data)
	})
	result := make(chan Message, 1)
	go func() {
		defer close(result)
		var msg Message
		if responsive.ReadJSON(&msg) == nil {
			result <- msg
		}
	}()
	select {
	case <-gotPing:
	case <-time.After(2 * time.Second):
		t.Fatal("Didn't receive Ping")
	}
	silent.SetPingHandler(func(string) error { return nil })
	if err := silent.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := silent.ReadMessage(); err == nil || !websocket.IsCloseError(err, websocket.CloseAbnormalClosure) {
		t.Fatalf("Expected server to drop silent client, got %v", err)
	}
	sendText(t, responsive, "alive")
	select {
	case msg := <-result:
		if msg.Text != "alive" {
			t.Fatalf("Responsive connection failed: %+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("Responsive connection did not receive message")
	}
}

func TestStaticPage(t *testing.T) {
	server := testServer(t, time.Minute, 2*time.Minute)
	res, err := server.Client().Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != http.StatusOK || !strings.Contains(string(body), "Пульт моніторингу") {
		t.Fatalf("Unexpected static response: %d (%v)", res.StatusCode, err)
	}
}
