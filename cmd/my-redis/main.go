// Command my-redis runs a toy in-memory key-value server that speaks RESP.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"my-redis/internal/server"
)

// shutdownGrace bounds how long a signalled server waits for connected clients
// to go away before exiting anyway.
const shutdownGrace = 5 * time.Second

func main() {
	var (
		host    = flag.String("host", "127.0.0.1", "address to bind to")
		port    = flag.Int("port", 6379, "port to listen on")
		verbose = flag.Bool("verbose", false, "log every connection and command")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	addr := net.JoinHostPort(*host, strconv.Itoa(*port))
	srv, err := server.Listen(addr, log)
	if err != nil {
		log.Error("failed to listen", "addr", addr, "error", err)
		os.Exit(1)
	}
	log.Info("ready to accept connections", "addr", srv.Addr())

	signalled, stopListening := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopListening()

	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()

	select {
	case err := <-served:
		log.Error("server stopped", "error", err)
		os.Exit(1)
	case <-signalled.Done():
		log.Info("signal received, shutting down", "grace", shutdownGrace)
	}

	ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Warn("shut down with clients still connected", "error", err)
	}
	<-served // Serve returns once the listener is closed.
	log.Info("stopped")
}
