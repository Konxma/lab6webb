package main

import (
	"context"
	"encoding/json"
	"log"
	"time"
)

type Message struct {
	Room   string `json:"room"`
	Sender string `json:"sender"`
	Text   string `json:"text"`
	Time   string `json:"time"`
}

type Hub struct {
	ctx        context.Context
	done       chan struct{}
	register   chan *Client
	unregister chan *Client
	broadcast  chan Message
	pingPeriod time.Duration
	pongWait   time.Duration
}

func newHub(ctx context.Context, pingPeriod, pongWait time.Duration) *Hub {
	return &Hub{
		ctx: ctx, done: make(chan struct{}),
		register: make(chan *Client), unregister: make(chan *Client),
		broadcast: make(chan Message), pingPeriod: pingPeriod, pongWait: pongWait,
	}
}

func (h *Hub) Run() {
	defer close(h.done)
	rooms := make(map[string]map[*Client]bool)
	remove := func(client *Client) {
		members := rooms[client.room]
		if !members[client] {
			return
		}
		delete(members, client)
		if len(members) == 0 {
			delete(rooms, client.room)
		}
		close(client.send)
		client.conn.Close()
		log.Printf("[WS] Disconnected %s from %s", client.sender, client.room)
	}
	for {
		select {
		case <-h.ctx.Done():
			for _, members := range rooms {
				for client := range members {
					remove(client)
				}
			}
			return
		case client := <-h.register:
			members := rooms[client.room]
			if members == nil {
				members = make(map[*Client]bool)
				rooms[client.room] = members
			}
			members[client] = true
			log.Printf("[WS] Connected %s to %s", client.sender, client.room)
		case client := <-h.unregister:
			remove(client)
		case msg := <-h.broadcast:
			data, err := json.Marshal(msg)
			if err != nil {
				log.Printf("[HUB] Encode message: %v", err)
				continue
			}
			delivered := 0
			for client := range rooms[msg.Room] {
				select {
				case client.send <- data:
					delivered++
				default:
					remove(client)
				}
			}
			log.Printf("[HUB] Room %s: queued for %d client(s)", msg.Room, delivered)
		}
	}
}
