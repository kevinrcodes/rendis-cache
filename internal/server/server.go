// Package server implements a Redis-compatible TCP server: it accepts client
// connections and executes the commands they send.
package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"my-redis/internal/resp"
	"my-redis/internal/store"
)

// sweepInterval is how often expired keys are reclaimed in the background.
// Expiry itself is exact: a key stops being visible the moment it expires,
// whether or not a sweep has run.
const sweepInterval = time.Second

// Server accepts connections on a listener and serves commands on them. Each
// client is served by its own goroutine, so a slow client cannot block others.
type Server struct {
	ln    net.Listener
	log   *slog.Logger
	store *store.Store

	conns sync.WaitGroup // in-flight connections, awaited by Shutdown

	closeOnce sync.Once
	closed    chan struct{} // closed by Close, stopping background work
}

// Listen binds a TCP listener to addr and returns a Server ready to Serve.
// An addr with port 0 binds an arbitrary free port, which Addr then reports.
func Listen(addr string, log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.Default()
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{
		ln:     ln,
		log:    log,
		store:  store.New(),
		closed: make(chan struct{}),
	}
	go s.sweepExpiredKeys(sweepInterval)
	return s, nil
}

// sweepExpiredKeys reclaims the memory held by keys that expired and were never
// read again. It runs until the server is closed.
func (s *Server) sweepExpiredKeys(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.closed:
			return
		case <-ticker.C:
			if removed := s.store.RemoveExpired(); removed > 0 {
				s.log.Debug("reclaimed expired keys", "count", removed)
			}
		}
	}
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Close stops the server from accepting new connections and stops its
// background work, causing Serve to return. Connections already being served
// are left to finish; use Shutdown to wait for them. Close is safe to call more
// than once.
func (s *Server) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return s.ln.Close()
}

// Shutdown stops accepting new connections and waits for the clients already
// connected to disconnect, or for ctx to be done, whichever happens first. It
// returns ctx.Err() if clients were still connected when the wait was cut short.
func (s *Server) Shutdown(ctx context.Context) error {
	// net.ErrClosed just means the listener is already closed, which makes
	// Shutdown safe to call more than once.
	if err := s.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	drained := make(chan struct{})
	go func() {
		s.conns.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Serve accepts connections, handing each to its own goroutine. It runs until
// the listener is closed, and returns nil in that case.
func (s *Server) Serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		s.conns.Add(1)
		go func() {
			defer s.conns.Done()
			s.serveConn(conn)
		}()
	}
}

// serveConn runs the read-execute-reply loop for one client until the client
// disconnects or sends input the protocol cannot recover from.
func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()

	log := s.log.With("remote_addr", conn.RemoteAddr().String())
	log.Debug("client connected")

	reader := resp.NewReader(conn)
	for {
		args, err := reader.ReadCommand()
		switch {
		case errors.Is(err, io.EOF):
			log.Debug("client disconnected")
			return
		case errors.Is(err, resp.ErrProtocol):
			// The stream is out of sync and cannot be resynchronised, so
			// report the problem and hang up.
			log.Warn("protocol error", "error", err)
			writeReply(conn, resp.Errorf("ERR Protocol error: %s", err), log)
			return
		case err != nil:
			log.Warn("read failed", "error", err)
			return
		}
		if len(args) == 0 {
			continue // an empty command line; nothing to execute
		}
		if !writeReply(conn, s.execute(args), log) {
			return
		}
	}
}

// writeReply sends one reply and reports whether the connection is still usable.
func writeReply(conn net.Conn, reply resp.Value, log *slog.Logger) bool {
	if _, err := reply.WriteTo(conn); err != nil {
		log.Warn("write failed", "error", err)
		return false
	}
	return true
}
