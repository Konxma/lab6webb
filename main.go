package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
)

//go:embed web/*
var assets embed.FS

func routes(hub *Hub) http.Handler {
	web, err := fs.Sub(assets, "web")
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/metrics/stream", metricsStream)
	mux.HandleFunc("GET /api/v1/ws", hub.serveWS)
	mux.Handle("GET /", http.FileServer(http.FS(web)))
	return mux
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	hub := newHub(ctx, 40*time.Second, 60*time.Second)
	go hub.Run()

	server := &http.Server{
		Addr: *addr, Handler: routes(hub),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	ended := make(chan error, 1)
	go func() { ended <- server.ListenAndServe() }()
	log.Printf("[SERVER] WebSocket Hub started. Listening on %s", *addr)
	select {
	case err := <-ended:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-ctx.Done():
		// Close streaming requests when the service stops. Hub closes upgraded sockets.
		if err := server.Close(); err != nil {
			log.Print(err)
		}
		<-hub.done
	}
}
