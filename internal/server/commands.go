package server

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"my-redis/internal/resp"
	"my-redis/internal/store"
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

// set stores a value under a key, replacing whatever was there, and applies any
// expiry options that follow.
// https://redis.io/docs/latest/commands/set/
func set(s *Server, args []string) resp.Value {
	if len(args) < 3 {
		return wrongArgCount(args[0])
	}
	key, value := args[1], args[2]

	opts, err := parseSetOptions(s.store.Now(), args[3:])
	if err != nil {
		return errorReply(err)
	}
	s.store.Set(key, value, opts)
	return resp.SimpleString("OK")
}

// Errors that map onto the replies real Redis sends for a malformed SET.
var (
	errSyntax       = errors.New("ERR syntax error")
	errNotAnInteger = errors.New("ERR value is not an integer or out of range")
	errInvalidTTL   = errors.New("ERR invalid expire time in 'set' command")
)

// parseSetOptions reads the optional arguments that follow "SET key value".
// Expiry may be given as a duration from now (EX seconds, PX milliseconds), as
// an absolute Unix timestamp (EXAT seconds, PXAT milliseconds), or KEEPTTL to
// leave the expiry already on the key alone. At most one may be given.
func parseSetOptions(now time.Time, args []string) (store.SetOptions, error) {
	var (
		opts store.SetOptions
		seen string // the expiry option already applied, if any
	)
	for i := 0; i < len(args); i++ {
		option := strings.ToUpper(args[i])
		switch option {
		case "EX", "PX", "EXAT", "PXAT":
			if seen != "" {
				return opts, errSyntax
			}
			if i+1 >= len(args) {
				return opts, errSyntax
			}
			i++
			expiresAt, err := expiryFrom(now, option, args[i])
			if err != nil {
				return opts, err
			}
			opts.ExpiresAt, seen = expiresAt, option
		case "KEEPTTL":
			if seen != "" {
				return opts, errSyntax
			}
			opts.KeepTTL, seen = true, option
		default:
			return opts, errSyntax
		}
	}
	return opts, nil
}

// unitOf maps an expiry option to the resolution of its argument.
var unitOf = map[string]time.Duration{
	"EX":   time.Second,
	"PX":   time.Millisecond,
	"EXAT": time.Second,
	"PXAT": time.Millisecond,
}

// expiryFrom turns an expiry option and its argument into an absolute time.
func expiryFrom(now time.Time, option, arg string) (time.Time, error) {
	n, err := strconv.ParseInt(arg, 10, 64)
	if err != nil {
		return time.Time{}, errNotAnInteger
	}
	unit := unitOf[option]

	// Reject values that would overflow when scaled to a Duration, so that an
	// absurd expiry is an error rather than a time in the past.
	if n <= 0 || n > int64(math.MaxInt64)/int64(unit) {
		return time.Time{}, errInvalidTTL
	}
	offset := time.Duration(n) * unit

	if strings.HasSuffix(option, "AT") {
		return time.Unix(0, 0).UTC().Add(offset), nil
	}
	return now.Add(offset), nil
}

// errorReply renders an error whose message is already in Redis's form.
func errorReply(err error) resp.Value {
	return resp.Error(err.Error())
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
