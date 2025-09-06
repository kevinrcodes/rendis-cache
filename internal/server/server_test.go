package server_test

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"my-redis/internal/server"
)

// newTestServer starts a server on an arbitrary free port and returns its
// address. The server is shut down when the test ends.
func newTestServer(t *testing.T) string {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	srv, err := server.Listen("127.0.0.1:0", log)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve() }()
	t.Cleanup(func() {
		srv.Close()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	return srv.Addr()
}

// client is a raw connection to the server, used to assert on exact wire bytes.
type client struct {
	t    *testing.T
	conn net.Conn
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	return &client{t: t, conn: conn}
}

// send writes a command in the array-of-bulk-strings form clients use.
func (c *client) send(args ...string) {
	c.t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
	}
	c.sendRaw(b.String())
}

func (c *client) sendRaw(s string) {
	c.t.Helper()
	if _, err := io.WriteString(c.conn, s); err != nil {
		c.t.Fatalf("write %q: %v", s, err)
	}
}

// expect reads exactly as many bytes as want and asserts they match.
func (c *client) expect(want string) {
	c.t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(c.conn, got); err != nil {
		c.t.Fatalf("read reply: %v (read %q, want %q)", err, got, want)
	}
	if string(got) != want {
		c.t.Errorf("reply = %q, want %q", got, want)
	}
}

// expectClosed asserts the server hung up with nothing further to say.
func (c *client) expectClosed() {
	c.t.Helper()
	if _, err := io.ReadAll(c.conn); err != nil {
		c.t.Fatalf("draining connection: %v", err)
	}
}

func TestPing(t *testing.T) {
	c := dial(t, newTestServer(t))
	c.send("PING")
	c.expect("+PONG\r\n")
}

func TestPingIsCaseInsensitive(t *testing.T) {
	c := dial(t, newTestServer(t))
	c.send("ping")
	c.expect("+PONG\r\n")
}

func TestPingWithMessage(t *testing.T) {
	c := dial(t, newTestServer(t))
	c.send("PING", "hello")
	c.expect("$5\r\nhello\r\n")
}

func TestPingWithTooManyArguments(t *testing.T) {
	c := dial(t, newTestServer(t))
	c.send("PING", "a", "b")
	c.expect("-ERR wrong number of arguments for 'ping' command\r\n")
}

func TestMultipleCommandsOnOneConnection(t *testing.T) {
	c := dial(t, newTestServer(t))
	for range 3 {
		c.send("PING")
		c.expect("+PONG\r\n")
	}
}

func TestPipelinedCommands(t *testing.T) {
	c := dial(t, newTestServer(t))
	c.sendRaw(strings.Repeat("*1\r\n$4\r\nPING\r\n", 3))
	c.expect(strings.Repeat("+PONG\r\n", 3))
}

func TestInlineCommand(t *testing.T) {
	c := dial(t, newTestServer(t))
	c.sendRaw("PING\r\n")
	c.expect("+PONG\r\n")
}

func TestUnknownCommand(t *testing.T) {
	c := dial(t, newTestServer(t))
	c.send("NOPE", "a")
	c.expect("-ERR unknown command 'NOPE', with args beginning with: 'a', \r\n")
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	c := dial(t, newTestServer(t))
	c.sendRaw("*1\r\n+PING\r\n")
	c.expect("-ERR Protocol error: ")
	c.expectClosed()
}
