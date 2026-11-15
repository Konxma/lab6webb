package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"time"
)

type metrics struct {
	Time       string  `json:"time"`
	Goroutines int     `json:"goroutines"`
	AllocMB    float64 `json:"alloc_mb"`
}

func metricsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	log.Printf("[SSE] Connected %s", r.RemoteAddr)
	defer log.Printf("[SSE] Disconnected %s", r.RemoteAddr)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	controller := http.NewResponseController(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case now := <-ticker.C:
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			data, err := json.Marshal(metrics{now.Format(time.RFC3339), runtime.NumGoroutine(), float64(mem.Alloc) / (1024 * 1024)})
			if err != nil {
				log.Printf("[SSE] Encode metrics: %v", err)
				return
			}
			// Limit blocked writes without imposing a timeout on the whole stream.
			if err := controller.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil && err != http.ErrNotSupported {
				return
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
