# Distributed Real-Time Drawing Board

## Mini-RAFT Consensus Project

A Dockerized collaborative drawing board built to demonstrate the core ideas
behind distributed systems:

- leader election
- replicated logs
- majority-based commits
- failure detection and recovery
- real-time WebSocket communication
- containerized services with hot reload

The project uses a small, classroom-friendly Raft-like protocol. It is designed
for learning and demonstration rather than production data durability.

## What the application does

Users draw on a shared browser canvas. Every stroke and clear operation is sent
to a WebSocket gateway. The gateway forwards commands to the current Raft
leader. The leader replicates each command to the other replicas and broadcasts
it to connected clients only after a majority of replicas acknowledge it.

If a leader fails, the remaining replicas elect a new leader and the gateway
automatically redirects new commands to that leader.

## Architecture

```text
                         WebSocket :8080
┌──────────────┐       ┌────────────────┐
│ Browser tabs │──────▶│    Gateway     │
└──────────────┘       │ HTTP :8081     │
                       └───────┬────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
       ┌──────▼─────┐   ┌──────▼─────┐   ┌──────▼─────┐
       │  Replica 1 │   │  Replica 2 │   │  Replica 3 │
       │   :4001    │   │   :4002    │   │   :4003    │
       └────────────┘   └────────────┘   └────────────┘
```

| Service | Port | Responsibility |
| --- | ---: | --- |
| `frontend` | `3000` | Serves the HTML canvas application through nginx |
| `gateway` | `8080` | Accepts WebSocket clients and routes drawing commands |
| `gateway` | `8081` | Provides health, broadcast, and leader-announcement endpoints |
| `replica1` | `4001` | Raft node |
| `replica2` | `4002` | Raft node |
| `replica3` | `4003` | Raft node |

All services communicate through the `raft-net` Docker network.

## Mini-RAFT protocol

Each replica can be in one of three states:

1. **Follower** — waits for heartbeats and votes for eligible candidates.
2. **Candidate** — starts an election after its timer expires.
3. **Leader** — accepts commands, replicates log entries, and commits them.

### Timing rules

- Election timeout: random value between 500 and 800 milliseconds
- Heartbeat interval: 150 milliseconds
- Cluster size: 3 replicas
- Required majority: 2 replicas

### Command flow

```text
Browser
  │
  │ WebSocket command
  ▼
Gateway
  │
  │ POST /command
  ▼
Leader
  │
  │ POST /append-entries
  ├──────────────▶ Follower 1
  └──────────────▶ Follower 2
          │
          │ majority acknowledgement
          ▼
       Commit entry
          │
          ├── apply command locally
          ├── notify gateway
          └── broadcast to WebSocket clients
```

Committed entries are applied in log order. A follower rejects stale terms and
resets its election timer whenever it receives a valid leader message.

## Replica API

The same API is available on ports `4001`, `4002`, and `4003`.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Lightweight health and role information |
| `GET` | `/status` | Detailed role, term, log, and commit information |
| `POST` | `/request-vote` | Candidate requests a vote |
| `POST` | `/append-entries` | Leader replicates log entries |
| `POST` | `/heartbeat` | Leader liveness message without log entries |
| `GET` | `/sync-log` | Read committed log entries for inspection or replay |
| `POST` | `/sync-log` | Leader-to-follower incremental catch-up |
| `POST` | `/command` | Leader accepts a drawing command |

### RequestVote payload

```json
{
  "term": 3,
  "candidateId": "replica2",
  "lastLogIndex": 7,
  "lastLogTerm": 3
}
```

### AppendEntries payload

```json
{
  "term": 3,
  "leaderId": "http://replica1:4001",
  "prevLogIndex": 6,
  "prevLogTerm": 3,
  "entries": [],
  "leaderCommit": 7
}
```

### Drawing command payload

```json
{
  "type": "stroke",
  "x1": 40,
  "y1": 60,
  "x2": 120,
  "y2": 90,
  "color": "#2563eb",
  "width": 6
}
```

Clear commands use the following shape:

```json
{
  "type": "clear"
}
```

## Requirements

- Docker Desktop
- Docker Compose
- A modern web browser

Docker must be running before starting the project.

## Run locally

Start all services and build the images:

```bash
docker compose up --build
```

Open the drawing board:

<http://localhost:3000>

Stop and remove the containers:

```bash
docker compose down
```

To rebuild after changing a dependency or Dockerfile:

```bash
docker compose up --build
```

## Hot reload

The source directories for the gateway and every replica are bind-mounted into
their containers. Each Node.js service runs with nodemon:

```text
gateway/src  →  /app/src
replica1/src →  /app/src
replica2/src →  /app/src
replica3/src →  /app/src
```

When a mounted source file changes:

1. nodemon stops the current Node.js process.
2. The process handles `SIGTERM` and closes its server and timers.
3. nodemon starts a fresh process.
4. The restarted replica joins the cluster as a follower.
5. The leader brings it up to date through replication or `/sync-log`.

## Failure demonstration

Start the project first, then stop one replica:

```bash
docker compose stop replica1
```

Two replicas remain, which is enough for a majority. The cluster can elect a
new leader and continue accepting drawing commands.

Restart the stopped replica:

```bash
docker compose start replica1
```

Inspect its state:

```bash
curl http://localhost:4001/status
curl http://localhost:4001/sync-log
```

The leader reports follower log length during heartbeats and sends missing
committed entries through `POST /sync-log`. This allows an empty restarted node
to catch up even when no new drawing command is submitted.

## Project structure

```text
.
├── docker-compose.yml
├── frontend/
│   └── index.html
├── gateway/
│   ├── Dockerfile
│   ├── package.json
│   └── src/
│       └── index.js
├── replica1/
│   ├── Dockerfile
│   ├── package.json
│   └── src/
│       └── index.js
├── replica2/
│   ├── Dockerfile
│   ├── package.json
│   └── src/
│       └── index.js
└── replica3/
    ├── Dockerfile
    ├── package.json
    └── src/
        └── index.js
```

The three replica source files share the same implementation. Their identity,
port, and peer list are supplied through Docker Compose environment variables.

## Learning checklist

Use this project to study:

- why a majority is required before committing an entry
- how terms prevent stale leaders from making decisions
- why election timeouts must be longer than heartbeat intervals
- how `nextIndex` and `matchIndex` repair a follower's log
- how a gateway can hide leader changes from WebSocket clients
- how health checks control container startup order
- how graceful shutdown supports hot reload and rolling replacement
- why in-memory state is not a substitute for durable storage

## Limitations

This is a Mini-RAFT educational implementation. It intentionally does not
provide:

- persistent log storage
- authentication or authorization
- network partition simulation
- production-grade backpressure
- multi-process durable state recovery

These limitations make the protocol easier to read and demonstrate. They are
also useful discussion points when comparing this project with systems such as
etcd, Consul, or Kubernetes control-plane components.

## License

This project is provided for educational use.
