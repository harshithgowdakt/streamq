# StreamQ

A Kafka-style message queue written in Go. Educational project — not for production.

Speaks the Kafka wire protocol (partially). Implements consumer groups, replicated partitions, a Raft-backed controller, and a replicated `__consumer_offsets` topic.

> Built collaboratively with [Claude Code](https://claude.ai/claude-code) and [Codex](https://openai.com/index/openai-codex/).

## What it does

- **Topics with partitions** — commit-log-per-partition, CRC-checked records, sparse mmap indexes.
- **Producers** — batching client, per-topic/partition routing, configurable acks.
- **Consumers** — two kinds:
  - `Consumer` — manual partition assignment, tracks offsets in memory.
  - `GroupConsumer` — full Kafka consumer group protocol (Join / Sync / Heartbeat / Leave / Commit / Fetch).
- **Consumer groups** — coordinator runs the state machine (Empty → PreparingRebalance → CompletingRebalance → Stable), session timeouts, partition assignment on the group leader (Range or RoundRobin).
- **Durable offsets** — commits are written synchronously to `__consumer_offsets` and survive a hard broker crash.
- **Cluster mode** — optional. A separate controller process runs Raft (via `hashicorp/raft`) and manages broker membership, partition placement, leader/follower roles, and ISR.
- **Replication** — in cluster mode, followers pull from leaders via `FetchRequest` with `ReplicaID = brokerID`. Produces go to leaders only. High-watermark advances once followers ack.

## Architecture

### The big picture — cluster mode

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              CONTROL PLANE                                        │
│                                                                                   │
│   ┌───────────────┐ Raft AppendEntries ┌───────────────┐ Raft  ┌───────────────┐ │
│   │ Controller A  │◄──────────────────►│ Controller B  │◄─────►│ Controller C  │ │
│   │  (leader)     │     :7000          │  (follower)   │ :7000 │  (follower)   │ │
│   │               │                    │               │       │               │ │
│   │  ┌─────────┐  │                    │  ┌─────────┐  │       │  ┌─────────┐  │ │
│   │  │  FSM    │  │                    │  │  FSM    │  │       │  │  FSM    │  │ │
│   │  │Metadata │  │                    │  │Metadata │  │       │  │Metadata │  │ │
│   │  └─────────┘  │                    │  └─────────┘  │       │  └─────────┘  │ │
│   │  ┌─────────┐  │                    │  ┌─────────┐  │       │  ┌─────────┐  │ │
│   │  │BoltDB   │  │                    │  │BoltDB   │  │       │  │BoltDB   │  │ │
│   │  │raft.db  │  │                    │  │raft.db  │  │       │  │raft.db  │  │ │
│   │  └─────────┘  │                    │  └─────────┘  │       │  └─────────┘  │ │
│   └───────┬───────┘                    └───────────────┘       └───────────────┘ │
│           │ :7001                                                                 │
│           │ (broker RPC — only leader has broker connections)                     │
└───────────┼───────────────────────────────────────────────────────────────────────┘
            │
            │  MsgRegisterBroker / MsgHeartbeat / MsgISRChange / MsgCreateTopicReq
            │  MsgMetadataUpdate ◄── (pushed on every FSM apply)
            │
┌───────────┼───────────────────────────────────────────────────────────────────────┐
│           │                       DATA PLANE                                      │
│           │                                                                       │
│  ┌────────▼───────┐      replicate log      ┌────────────────┐  ┌──────────────┐  │
│  │   Broker 1     │◄────────────────────────│   Broker 2     │  │   Broker 3   │  │
│  │   :9092        │    ReplicaFetcher       │   :9093        │  │   :9094      │  │
│  │                │                         │                │  │              │  │
│  │ ┌──────────┐   │                         │ ┌──────────┐   │  │ ┌──────────┐ │  │
│  │ │ Topic    │   │                         │ │ Topic    │   │  │ │ Topic    │ │  │
│  │ │ Manager  │   │                         │ │ Manager  │   │  │ │ Manager  │ │  │
│  │ └──────────┘   │                         │ └──────────┘   │  │ └──────────┘ │  │
│  │ ┌──────────┐   │                         │ ┌──────────┐   │  │ ┌──────────┐ │  │
│  │ │ Group    │   │                         │ │ Group    │   │  │ │ Group    │ │  │
│  │ │ Coord    │   │                         │ │ Coord    │   │  │ │ Coord    │ │  │
│  │ └──────────┘   │                         │ └──────────┘   │  │ └──────────┘ │  │
│  │ ┌──────────┐   │                         │ ┌──────────┐   │  │ ┌──────────┐ │  │
│  │ │ Commit   │   │                         │ │ Commit   │   │  │ │ Commit   │ │  │
│  │ │ Logs     │   │                         │ │ Logs     │   │  │ │ Logs     │ │  │
│  │ │ (disk)   │   │                         │ │ (disk)   │   │  │ │ (disk)   │ │  │
│  │ └──────────┘   │                         │ └──────────┘   │  │ └──────────┘ │  │
│  └────────▲───────┘                         └────────────────┘  └──────────────┘  │
└───────────┼───────────────────────────────────────────────────────────────────────┘
            │
            │ Kafka wire protocol (TCP)
            │ Produce / Fetch / Metadata / JoinGroup / Sync / Heartbeat / Commit
            │
      ┌─────┴──────┬──────────────┐
      │            │              │
 ┌────▼────┐ ┌─────▼─────┐ ┌──────▼──────┐
 │Producer │ │ Consumer  │ │   Group     │
 │(client) │ │ (client)  │ │  Consumer   │
 └─────────┘ └───────────┘ └─────────────┘
```

**Two-sentence summary:**
- **Control plane:** 3 or 5 controller processes form a Raft cluster that agrees on `ClusterMetadata` (who owns what). Each controller pushes the latest metadata to brokers whenever the FSM advances.
- **Data plane:** Brokers host partitions as append-only commit logs, replicate leader→follower via `ReplicaFetcher`, serve Kafka-protocol clients, and run consumer-group coordinators that durably commit offsets to an internal `__consumer_offsets` topic.

### Ports

| Port | Who listens | What speaks over it |
|---|---|---|
| `:9092`, `:9093`, … | Brokers | Kafka wire protocol (clients) |
| `:7000`, `:7010`, … | Controllers | Raft peer traffic (controller ↔ controller) |
| `:7001`, `:7011`, … | Controllers | Broker RPC (broker ↔ controller + metadata pushes) |

Only `:9092`-series ports are visible to clients. Everything else is internal cluster plumbing.

### Inside a single broker

```
                  Kafka wire protocol (TCP :9092)
                              │
                              ▼
                     ┌──────────────────┐
                     │     SERVER       │   internal/server/
                     │ length-prefixed  │   - one goroutine per connection
                     │ framing + router │   - read frame → decode → dispatch
                     └────────┬─────────┘
                              │
                              ▼
                     ┌──────────────────┐
                     │     BROKER       │   internal/broker/broker.go
                     │    Dispatch()    │   - fans out to handlers by request type
                     └────────┬─────────┘
                              │
          ┌───────────────────┼───────────────────────────────────┐
          ▼                   ▼                                   ▼
  ┌───────────────┐  ┌─────────────────┐              ┌──────────────────────┐
  │  TOPIC        │  │  GROUP          │              │  CONTROLLER CLIENT   │
  │  MANAGER      │  │  COORDINATOR    │              │  (cluster only)      │
  │               │  │                 │              │                      │
  │ ┌───────────┐ │  │ ┌─────────────┐ │              │ ┌──────────────────┐ │
  │ │  Topic    │ │  │ │ConsumerGroup│ │              │ │ readLoop         │ │
  │ │ Partition │ │  │ │  (per grp)  │ │              │ │ - MetadataUpdate │ │
  │ │ Partition │ │  │ │   state     │ │              │ │ - push handler   │ │
  │ └─────┬─────┘ │  │ │   members   │ │              │ └──────────────────┘ │
  │       │       │  │ │   offsets   │ │              │ ┌──────────────────┐ │
  │       ▼       │  │ └─────────────┘ │              │ │ heartbeatLoop    │ │
  │ ┌───────────┐ │  │                 │              │ └──────────────────┘ │
  │ │ COMMIT    │ │  │  writes to      │              │                      │
  │ │ LOG       │ │  │  __consumer_    │─── records ──┼──► TCP :7001 ───────►│
  │ │           │ │  │  offsets topic  │              │    to controller     │
  │ │ Segments  │ │  │  (via Topic     │              │                      │
  │ │  .log     │ │  │   Manager)      │              └──────────────────────┘
  │ │  .index   │ │  │                 │                        │
  │ └───────────┘ │  └─────────────────┘                        │
  └───────────────┘                                              │
          ▲                                                      │
          │ cluster mode only: followers pull from leader        │
          │                                                      │
  ┌───────┴─────────┐                                            │
  │ REPLICA FETCHER │  internal/broker/replicator.go             │
  │  (follower)     │  - FetchRequest with ReplicaID=my_broker   │
  │                 │  - AppendReplica preserves leader's offset │
  └───────┬─────────┘                                            │
          │                                                      │
          ▼                                                      │
  ┌─────────────────┐                                            │
  │  ISR MANAGER    │  internal/broker/isr.go                    │
  │                 │  - watches follower lag                    │
  │                 │  - reports shrink/expand to controller ────┘
  └─────────────────┘                    (MsgISRChange)
```

### The three control flows

#### 1. Produce with replication (acks=all)

```
Producer            Broker A (leader)           Broker B (follower)
   │                      │                             │
   │── ProduceRequest ───►│                             │
   │                  Check role=LEADER                 │
   │                  Append to commit log              │
   │                      │◄── FetchRequest             │
   │                      │    (ReplicaID=B)            │
   │                      │── records + HWM ──────────►│
   │                      │                        Append Replica
   │                  Update HWM = min(LEO across ISR)  │
   │◄── ProduceResponse ──│                             │
```

#### 2. Consumer group join

```
Consumer                            Broker (coordinator)
   │── FindCoordinator("my-grp") ──►│
   │◄─── {host, port} ──────────────│
   │                                │
   │── JoinGroup(memberID="") ─────►│  Empty → PreparingRebalance
   │                                │  500ms delay (absorb joiners)
   │                                │  → ++gen, pick leader, pick protocol
   │◄── JoinGroupResponse ──────────│  → CompletingRebalance
   │    {gen, leader, members}      │
   │                                │
   │  if I'm leader: run            │
   │  RangeAssignor client-side     │
   │                                │
   │── SyncGroup(assignments) ─────►│  Store each member's bytes
   │                                │  → Stable
   │◄── SyncGroupResponse ──────────│
   │    {my assignment}             │
   │                                │
   │── OffsetFetch ────────────────►│
   │◄── last committed offsets ─────│
   │                                │
   │── FetchRequest ───────────────►│
   │◄── records ────────────────────│
   │                                │
   │── Heartbeat (loop) ───────────►│  every session_timeout / 3
   │── OffsetCommit ───────────────►│  written to __consumer_offsets
```

#### 3. Cluster metadata change (create topic)

```
Client      Broker          Controller Leader       Peer Controllers     All Brokers
  │            │                   │                       │                  │
  │──Create ──►│──MsgCreateTopic──►│                       │                  │
  │            │     (:7001)       │──raft.Apply──►        │                  │
  │            │                   │──AppendEntries───────►│                  │
  │            │                   │    (:7000)        persist to raft.db     │
  │            │                   │◄──── ack ─────────────│                  │
  │            │               majority → commit           │                  │
  │            │               FSM.Apply (every node):     │                  │
  │            │                 placement                 │                  │
  │            │                 leader = replicas[0]      │                  │
  │            │                 epoch = 1                 │                  │
  │            │                                                              │
  │            │◄── MsgMetadataUpdate ─── BroadcastMetadata hook ────────────►│
  │            │                                                         onMetadataUpdate:
  │◄──ack──────│                                                           - set role
  │                                                                        - start fetcher
```

### Persistent state on disk

```
Controller                              Broker
─────────────────────                   ────────────────────────────
{dataDir}/                              {dataDir}/
├── raft.db     (BoltDB)                ├── orders-0/           (user topic)
│   └── Raft log entries                │   ├── 00...00.log
│       replicated across               │   └── 00...00.index
│       controller nodes                ├── orders-1/
├── raft.db-shm                         │   ├── 00...00.log
├── raft.db-wal                         │   └── 00...00.index
└── snapshots/                          ├── __consumer_offsets-0/  (internal)
    └── periodic FSM snapshots          │   ├── 00...00.log
                                        │   └── 00...00.index
                                        ├── __consumer_offsets-1/
                                        │   ...
                                        └── __consumer_offsets-7/
                                            ...
```

## Layout

```
cmd/
  streamq-broker/       # dataplane process (handles produce/fetch/group RPCs)
  streamq-controller/   # control plane process (Raft, cluster metadata)
  streamq-producer/     # CLI producer
  streamq-consumer/     # CLI consumer
internal/
  broker/               # topic manager, group coordinator, replicator, ISR manager
  controller/           # Raft + FSM managing cluster metadata
  cluster/              # shared broker-controller RPC types
  protocol/             # Kafka wire format encode/decode
  log/                  # commit log, segments, indexes
pkg/
  producer/             # public producer library
  consumer/             # public consumer library (Consumer + GroupConsumer)
```

`pkg/` is the public client API (safe to import). `internal/` is off-limits to external projects (Go enforces this).

## Quick start — single node

```bash
go build ./...

# start broker
./streamq-broker --addr :9092 --data-dir /tmp/sq-data

# produce
echo "hello streamq" | ./streamq-producer --broker localhost:9092 --topic demo

# consume (no group)
./streamq-consumer --broker localhost:9092 --topic demo --offset 0
```

## Quick start — consumer groups (Go API)

```go
import "github.com/harshithgowda/streamq/pkg/consumer"

cfg := consumer.DefaultGroupConfig()
cfg.BrokerAddr = "localhost:9092"
cfg.GroupID = "my-app"

c, _ := consumer.NewGroupConsumer(cfg)
c.Subscribe("orders")
defer c.Close()

for {
    msgs, _ := c.Poll(500 * time.Millisecond)
    for _, m := range msgs {
        handle(m)
    }
    c.CommitSync()
}
```

Start two of these in the same group and partitions split between them. Kill one and the survivor picks up its partitions on the next rebalance.

## Quick start — cluster mode (3 controllers + 3 brokers)

```bash
# controllers (dedicated Raft cluster)
./streamq-controller --raft-addr :7000 --rpc-addr :7001 --bootstrap --peers ":7010,:7020" &
./streamq-controller --raft-addr :7010 --rpc-addr :7011 &
./streamq-controller --raft-addr :7020 --rpc-addr :7021 &

# brokers — point any one at a controller's rpc-addr
./streamq-broker --addr :9092 --data-dir /tmp/sq1 --controller-addr localhost:7001 &
./streamq-broker --addr :9093 --data-dir /tmp/sq2 --controller-addr localhost:7001 &
./streamq-broker --addr :9094 --data-dir /tmp/sq3 --controller-addr localhost:7001 &
```

Ports:
- `7000-7020` — controller-to-controller Raft traffic.
- `7001-7021` — broker-to-controller RPC (registration, heartbeats, metadata push).
- `9092-9094` — client-facing Kafka protocol.

Create a topic with 3 partitions, replication factor 3:

```go
import "github.com/harshithgowda/streamq/pkg/producer"  // or use kafka-go / sarama etc.
```

The controller's FSM places replicas round-robin across brokers, assigns leaders as `replicas[0]`, and pushes the new assignment to every broker. Followers start pulling from their leader within one metadata-update cycle.

## Supported Kafka APIs

| API | Key | Versions | Notes |
|---|---|---|---|
| Produce | 0 | 0-3 | acks=0/1/-1; -1 waits for HWM |
| Fetch | 1 | 0-4 | `ReplicaID >= 0` treated as follower fetch |
| ListOffsets | 2 | 0-1 | earliest/latest |
| Metadata | 3 | 0-1 | brokers, leaders, ISR, replicas |
| OffsetCommit | 8 | 0-2 | synchronous write to `__consumer_offsets` |
| OffsetFetch | 9 | 0 | reads from in-memory cache (log-restored at startup) |
| FindCoordinator | 10 | 0 | returns self (no hash routing yet) |
| JoinGroup | 11 | 0-1 | full state-machine flow |
| Heartbeat | 12 | 0 | resets session timer, signals rebalance |
| LeaveGroup | 13 | 0 | explicit leave triggers rebalance |
| SyncGroup | 14 | 0 | leader distributes assignments |
| ApiVersions | 18 | 0-2 | version negotiation |
| CreateTopics | 19 | 0 | routed through controller in cluster mode |

## Testing

```bash
go test ./... -race
```

All packages test clean with the race detector. There's also an end-to-end demo I ran during development proving consumer offsets survive a `SIGKILL -9` of the broker — committed progress gets written to `__consumer_offsets` synchronously, so a fresh consumer in the same group resumes from the last commit, not from zero.

## What's **not** production-ready

Treat StreamQ as a reading exercise, not a replacement for Kafka or Redpanda.

- **No `fsync`.** Writes survive a process crash (as demonstrated) but not an OS/power loss.
- **No log compaction.** `__consumer_offsets` grows forever; startup replay scans everything.
- **Coordinator failover is broken.** Every broker currently acts as coordinator for every group. Two consumers on different brokers can diverge.
- **Controller failover isn't wired on the broker side.** Brokers hold one TCP connection to the controller they first registered with; they don't follow leader changes.
- **Kafka client compatibility is partial.** Our own Go client works end-to-end. `librdkafka` (kcat) crashes against our `ApiVersions` response — we don't implement flexible versions or tagged fields.
- **No SASL, no TLS, no ACLs, no quotas.**
- **No transactions, no idempotent producers.**
- **No compression** (gzip/snappy/zstd/lz4).
- **Assignment is trivial round-robin** — no rack awareness, no sticky partitions.

## Further reading

The code is deliberately small (~3-4k lines) and maps to the ideas directly:

- `internal/controller/fsm.go` — the replicated state machine. Commands (`register_broker`, `create_topic`, `update_isr`, `broker_timeout`) mutate `ClusterMetadata`; every controller applies identical entries so the state converges.
- `internal/broker/group_coordinator.go` — the consumer group state machine and blocking join/sync pattern using per-member channels.
- `internal/broker/replicator.go` — the follower-side `ReplicaFetcher` that tails the leader's log via `FetchRequest`.
- `internal/broker/offsets_topic.go` — how committed offsets become records in `__consumer_offsets`, keyed by `(group, topic, partition)`, replayed on startup.

## License

Educational use only.
