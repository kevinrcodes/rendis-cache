# my-redis

A toy in-memory key-value server that speaks [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/)
over TCP.
Built with the Go standard library only - no dependencies.

Supported commands: `PING`, `ECHO`, `SET` (with expiry options), `GET`.

## Running

```bash
go run ./cmd/my-redis
```

## Testing

```bash
go test ./...
```
