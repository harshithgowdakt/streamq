# StreamQ

A Kafka-compatible message queue written in Go for educational purposes.

StreamQ implements the real Kafka wire protocol, allowing standard Kafka tools like `kcat`, `kafka-console-producer`, and `kafka-console-consumer` to connect directly.

> **Note**: This project was developed using AI tools including [Claude Code](https://claude.ai/claude-code) and [Codex](https://openai.com/index/openai-codex/). It is intended for educational purposes only and is not suitable for production use.

## Features

- **Real Kafka wire protocol** — supports ApiVersions, Metadata, Produce, Fetch, ListOffsets, FindCoordinator, JoinGroup, OffsetFetch, and CreateTopics APIs
- **Persistent append-only log** with CRC-32C integrity checks
- **Automatic segment rolling** and retention enforcement (size and age based)
- **Sparse memory-mapped indexes** for fast offset lookups
- **Batching producer** with configurable batch size and linger time
- **Poll-based consumer** with automatic offset tracking
- **Auto-topic creation** on first produce (configurable)

## Architecture

```
cmd/
  streamq-broker/       # Broker entry point
  streamq-producer/     # CLI producer
  streamq-consumer/     # CLI consumer
internal/
  broker/               # Topic management, request handlers
  server/               # TCP server with length-prefixed framing
  protocol/             # Kafka wire protocol encode/decode, RecordBatch translation
  log/                  # Commit log, segments, indexes
pkg/
  producer/             # Producer client library
  consumer/             # Consumer client library
```

## Quick Start

```bash
# Build
go build ./...

# Start the broker
./streamq-broker --addr :9092 --data-dir /tmp/streamq-data

# Produce (stdin, one message per line)
echo "hello streamq" | ./streamq-producer --broker localhost:9092 --topic test

# Consume
./streamq-consumer --broker localhost:9092 --topic test --offset 0
```

## Using with kcat

```bash
# Metadata discovery
kcat -b localhost:9092 -L

# Produce
echo "hello kafka" | kcat -b localhost:9092 -t test -P

# Consume
kcat -b localhost:9092 -t test -C -o beginning -e
```

## Supported Kafka APIs

| API             | Key | Versions | Purpose                          |
|-----------------|-----|----------|----------------------------------|
| Produce         | 0   | 0-3      | Write records                    |
| Fetch           | 1   | 0-4      | Read records                     |
| ListOffsets     | 2   | 0-1      | Get earliest/latest offset       |
| Metadata        | 3   | 0-1      | Broker/topic/partition discovery |
| OffsetFetch     | 9   | 0        | Returns -1 (no committed offsets)|
| FindCoordinator | 10  | 0        | Returns self as coordinator      |
| JoinGroup       | 11  | 0        | Returns error (no consumer groups)|
| ApiVersions     | 18  | 0-2      | Version negotiation              |
| CreateTopics    | 19  | 0        | Create topics                    |

## Running Tests

```bash
go test ./... -race
```

## License

Educational use only.
