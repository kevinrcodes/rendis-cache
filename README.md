# my-redis

A toy in-memory key-value server that speaks [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/), the Redis wire protocol, over TCP.
Go standard library only, no dependencies.

Any Redis client can talk to it:

```console
$ go run ./cmd/my-redis
time=... level=INFO msg="ready to accept connections" addr=127.0.0.1:6379

$ redis-cli SET foo bar PX 100
OK
$ redis-cli GET foo
"bar"
$ sleep 0.2 && redis-cli GET foo
(nil)
```

## Commands

| Command | Reply |
| --- | --- |
| `PING [message]` | `+PONG`, or the message as a bulk string |
| `ECHO message` | The message as a bulk string |
| `SET key value [EX s \| PX ms \| EXAT ts \| PXAT ts-ms \| KEEPTTL]` | `+OK` |
| `GET key` | The value, or the null bulk string if the key is absent or expired |

Command and option names are case-insensitive.
A plain `SET` clears any expiry the key already had; `KEEPTTL` retains it.
At most one expiry option may be given.

## Running

```bash
go run ./cmd/my-redis                 # 127.0.0.1:6379
go run ./cmd/my-redis -port 6380      # a different port
go run ./cmd/my-redis -host 0.0.0.0   # every interface (see below)
go run ./cmd/my-redis -verbose        # log connections and key reclamation
```

The server binds loopback by default rather than every interface, the way real Redis does.
It has no authentication, so exposing it beyond this machine should be a deliberate choice.

`SIGINT` or `SIGTERM` stops it accepting new connections and gives the clients already connected five seconds to disconnect.

## Testing

```bash
go test -race ./...
```

There is no `redis-cli` dependency: the server tests dial a real listener and assert on exact wire bytes, which is the only thing a client actually sees.

## How it fits together

- `internal/resp` - the protocol.
  `Value` encodes replies; `Reader` turns a connection into a stream of commands, accepting both the array-of-bulk-strings form real clients send and the inline form a `netcat` session produces.
  Line, element and array sizes are capped so a hostile client cannot make the server allocate without bound.
  Malformed input is reported as an error wrapping `ErrProtocol`: the server answers it and hangs up, because a desynchronised stream cannot be resynchronised.
- `internal/store` - the keyspace.
  A mutex-guarded map, shared by every connection, where entries may carry an expiry.
- `internal/server` - the accept loop, one goroutine per connection, and the command table.

### Expiry

Expiry is exact, not sampled: a key stops being visible the instant its expiry time is reached.
Reads hide expired entries under the read lock instead of deleting them, so concurrent `GET`s still run in parallel, and a sweep every second reclaims the memory held by keys that expired and were never read again.

The store reads the clock through a field, so its tests advance a fake clock instead of sleeping.

## What this deliberately leaves out

It is a toy, and stops well short of Redis:

- Only strings.
  No lists, hashes, sets, sorted sets or streams.
- No `DEL`, `EXISTS`, `TTL`, `EXPIRE`, `INCR`, `KEYS`, or the `NX`/`XX`/`GET` options of `SET`.
- No persistence.
  Everything is lost when the process exits: no RDB snapshots, no append-only file.
- No replication, no transactions, no pub/sub, no Lua, no cluster.
- No authentication and no TLS.
- One lock over one map.
  That is ample here, but a server under real load would want the keyspace sharded so that writes to unrelated keys do not contend.
