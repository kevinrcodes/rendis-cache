package server

import (
	"strings"

	"my-redis/internal/resp"
)

// handler executes one command against the server and returns the reply to send
// back. args holds the whole command, including its name at index 0.
type handler func(s *Server, args []string) resp.Value

// commands maps a lowercased command name to its implementation.
var commands = map[string]handler{
	"echo": echo,
	"get":  get,
	"ping": ping,
	"set":  set,
}

// execute dispatches a command, which must be non-empty.
func (s *Server) execute(args []string) resp.Value {
	name := strings.ToLower(args[0])
	handle, ok := commands[name]
	if !ok {
		return resp.Errorf("ERR unknown command '%s', with args beginning with: %s",
			args[0], quoteArgs(args[1:]))
	}
	return handle(s, args)
}

// ping replies PONG, or echoes its argument when given one.
// https://redis.io/docs/latest/commands/ping/
func ping(_ *Server, args []string) resp.Value {
	switch len(args) {
	case 1:
		return resp.SimpleString("PONG")
	case 2:
		return resp.BulkString(args[1])
	default:
		return wrongArgCount(args[0])
	}
}

// echo returns its single argument unchanged.
// https://redis.io/docs/latest/commands/echo/
func echo(_ *Server, args []string) resp.Value {
	if len(args) != 2 {
		return wrongArgCount(args[0])
	}
	return resp.BulkString(args[1])
}

// set stores a value under a key, replacing whatever was there.
// https://redis.io/docs/latest/commands/set/
func set(s *Server, args []string) resp.Value {
	if len(args) != 3 {
		return wrongArgCount(args[0])
	}
	key, value := args[1], args[2]
	s.store.Set(key, value)
	return resp.SimpleString("OK")
}

// get returns the value stored under a key, or the null bulk string if the key
// does not exist.
// https://redis.io/docs/latest/commands/get/
func get(s *Server, args []string) resp.Value {
	if len(args) != 2 {
		return wrongArgCount(args[0])
	}
	value, ok := s.store.Get(args[1])
	if !ok {
		return resp.Nil()
	}
	return resp.BulkString(value)
}

func wrongArgCount(name string) resp.Value {
	return resp.Errorf("ERR wrong number of arguments for '%s' command", strings.ToLower(name))
}

// quoteArgs renders arguments the way Redis does in its unknown-command error.
func quoteArgs(args []string) string {
	var b strings.Builder
	for _, arg := range args {
		b.WriteString("'")
		b.WriteString(arg)
		b.WriteString("', ")
	}
	return b.String()
}
