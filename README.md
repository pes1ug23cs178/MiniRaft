# Distributed Drawing Board: a small Raft classroom project

This project is an intentionally small, readable example of a replicated drawing board. A browser sends drawing commands over WebSocket to a gateway. The gateway finds the current Raft leader, forwards the command, and broadcasts only committed commands back to connected browsers.

## Architecture

```text
browser (nginx :3000) --WebSocket--> gateway (:8080, health :8081)
                                      |-- replica1 (:4001)
                                      |-- replica2 (:4002)
                                      `-- replica3 (:4003)
```

Each replica is an independent Node.js process. It starts as a follower, uses a randomized 500–800 ms election timer, and sends leader heartbeats every 150 ms. A candidate requests votes and becomes leader after a majority. Leaders replicate commands with AppendEntries and commit an entry only after a majority acknowledges it.

## Run

Requirements: Docker and Docker Compose.

```bash
docker compose up --build
```

Open <http://localhost:3000>. Source folders are bind-mounted into containers and nodemon reloads changes during lessons. Stop with `docker compose down`.

## Protocol tour

- `POST /request-vote`: `{ term, candidateId, lastLogIndex, lastLogTerm }` -> `{ term, voteGranted }`.
- `POST /append-entries`: `{ term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit }`. This performs consistency checking, replication, and commit advancement.
- `POST /heartbeat`: the dedicated 150 ms leader liveness RPC. It never carries drawing entries.
- `GET /sync-log`: returns `{ term, entries, commitIndex }` as a read-only snapshot for gateway replay and inspection. `POST /sync-log`: leader-to-follower incremental catch-up with `{ term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit }`; it appends missing committed entries and returns `logLength`, `matchIndex`, and `commitIndex`. Leaders trigger it from heartbeat-reported follower lag, so an empty restarted node catches up without a new drawing command.
- `POST /command`: a leader accepts `{ type: "stroke", ... }` or `{ type: "clear" }` and returns a redirect from followers.
- `GET /status` and `GET /health`: inspect role, term, leader, log length, and commit index.
- Gateway `POST /broadcast` is used after apply; WebSocket clients receive the same JSON event and late clients receive the replay from `/sync-log`.

Example stroke payload:

```json
{"type":"stroke","x1":40,"y1":60,"x2":120,"y2":90,"color":"#38bdf8","width":6}
```

## Failure demonstration

In another terminal, stop one node:

```bash
docker compose stop replica1
```

The remaining two nodes still form a majority and can elect a leader. Draw while it is stopped, then restart it:

```bash
docker compose start replica1
curl http://localhost:4001/sync-log
```

The leader heartbeat reports follower log length, then sends committed gaps through `POST /sync-log`; the restarted node catches up even when no new drawing command arrives.

## Learning notes

1. Terms prevent an old leader from overwriting a newer decision. Every RPC rejects stale terms.
2. Election timers are longer than the 150 ms heartbeat interval, so healthy followers do not campaign.
3. `nextIndex` and `matchIndex` show how a leader repairs a lagging follower one entry at a time.
4. A command is visible to browsers only after majority commit and state-machine application.
5. This is a teaching implementation: it keeps state in memory and does not promise durable storage across container destruction.

## Layout

- `frontend/index.html` — canvas, controls, WebSocket client, and replica dashboard.
- `gateway/src/index.js` — WebSocket hub, leader discovery, routing, replay, and health API.
- `replica{1,2,3}/src/index.js` — same small Raft node with different environment identity.
- `docker-compose.yml` — one gateway, three replicas, nginx frontend, network, healthchecks, and bind mounts.
