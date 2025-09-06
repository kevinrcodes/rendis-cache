// Command my-redis runs a toy in-memory key-value server that speaks RESP.
package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"strconv"

	"my-redis/internal/server"
)

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

	if err := srv.Serve(); err != nil {
		log.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
