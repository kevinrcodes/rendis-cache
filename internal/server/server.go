// Package server implements a Redis-compatible TCP server: it accepts client
// connections and executes the commands they send.
package server

import (
	"errors"
	"io"
	"log/slog"
	"net"

	"my-redis/internal/resp"
)

// Server accepts connections on a listener and serves commands on them.
type Server struct {
	ln  net.Listener
	log *slog.Logger
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
	return &Server{ln: ln, log: log}, nil
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Close stops the server from accepting new connections.
func (s *Server) Close() error { return s.ln.Close() }

// Serve accepts connections and serves each one in turn. It runs until the
// listener is closed, and returns nil in that case.
func (s *Server) Serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		s.serveConn(conn)
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
		if !writeReply(conn, execute(args), log) {
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
