import express from "express";
import { WebSocketServer } from "ws";
import fetch from "node-fetch";

const WEBSOCKET_PORT = 8080;
const HTTP_PORT = 8081;
const REQUEST_TIMEOUT_MS = 500;
const LEADER_RETRY_DELAY_MS = 200;
const MAX_FORWARD_ATTEMPTS = 5;

const replicas = (process.env.REPLICAS || "")
  .split(",")
  .map((url) => url.trim())
  .filter(Boolean);

const clients = new Set();
const replay = [];

let currentLeader = null;
let leaderDiscovery = null;

/**
 * Send an HTTP request with a timeout so an unavailable replica
 * cannot block the gateway indefinitely.
 */
async function request(url, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(
    () => controller.abort(),
    REQUEST_TIMEOUT_MS,
  );

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Check the cached leader first, then ask each replica which node is leader.
 */
async function discoverLeader() {
  if (currentLeader) {
    try {
      const response = await request(`${currentLeader}/health`);
      const status = await response.json();

      if (response.ok && status.role === "leader") {
        return currentLeader;
      }
    } catch (_) {
      // The cached leader may be down during a failover.
    }
  }

  for (const replica of replicas) {
    try {
      const response = await request(`${replica}/status`);
      const status = await response.json();

      if (response.ok && status.role === "leader") {
        currentLeader = replica;
        return currentLeader;
      }
    } catch (_) {
      // Try the next replica when this one is unavailable.
    }
  }

  currentLeader = null;
  return null;
}

/**
 * Share one discovery request between simultaneous WebSocket messages.
 */
async function getLeader() {
  if (!leaderDiscovery) {
    leaderDiscovery = discoverLeader().finally(() => {
      leaderDiscovery = null;
    });
  }

  return leaderDiscovery;
}

function broadcast(message) {
  const payload = JSON.stringify(message);

  for (const client of clients) {
    if (client.readyState === 1) {
      client.send(payload);
    }
  }
}

/**
 * Rebuild the gateway replay buffer from committed entries after restart.
 */
async function replayFromCluster() {
  const node = await getLeader();

  if (!node) {
    return;
  }

  try {
    const response = await request(`${node}/sync-log`);
    const data = await response.json();

    replay.length = 0;

    for (const entry of data.entries || []) {
      if (entry.command?.type !== "noop") {
        replay.push({
          ...entry.command,
          index: entry.index,
          term: entry.term,
        });
      }
    }
  } catch (_) {
    // A later client connection can retry the replay request.
  }
}

/**
 * Forward a drawing command to the current leader.
 * A 307 response means the contacted replica knows a better leader.
 */
async function forward(command) {
  for (let attempt = 0; attempt < MAX_FORWARD_ATTEMPTS; attempt += 1) {
    const node = await getLeader();

    if (!node) {
      await new Promise((resolve) =>
        setTimeout(resolve, LEADER_RETRY_DELAY_MS),
      );
      continue;
    }

    try {
      const response = await request(`${node}/command`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify(command),
      });

      if (response.status === 307) {
        const data = await response.json();
        currentLeader = data.leader || null;
        continue;
      }

      if (response.ok) {
        return true;
      }
    } catch (_) {
      // Clear the cache so the next attempt performs leader discovery.
    }

    currentLeader = null;
  }

  return false;
}

const websocketServer = new WebSocketServer({
  port: WEBSOCKET_PORT,
});

websocketServer.on("connection", async (socket) => {
  clients.add(socket);

  await replayFromCluster();

  for (const event of replay) {
    if (socket.readyState === 1) {
      socket.send(JSON.stringify(event));
    }
  }

  socket.on("message", async (rawMessage) => {
    try {
      const message = JSON.parse(rawMessage);

      if (message.type === "ping") {
        socket.send(JSON.stringify({ type: "pong" }));
        return;
      }

      await forward(message);
    } catch (_) {
      // Ignore malformed client messages without stopping the connection.
    }
  });

  socket.on("close", () => clients.delete(socket));
  socket.on("error", () => clients.delete(socket));
});

const app = express();
app.use(express.json({ limit: "1mb" }));

// Replicas call this after applying a committed command.
app.post("/broadcast", (req, res) => {
  const event = req.body;

  if (event.type === "clear") {
    replay.length = 0;
  } else if (event.type !== "noop") {
    replay.push(event);
  }

  broadcast(event);
  res.json({
    ok: true,
    clients: clients.size,
  });
});

// A newly elected leader announces itself to reduce discovery latency.
app.post("/leader-announce", (req, res) => {
  if (!req.body.leader) {
    return res.status(400).json({
      error: "leader is required",
    });
  }

  currentLeader = req.body.leader;
  res.json({
    ok: true,
    leader: currentLeader,
  });
});

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    leader: currentLeader,
    replicas,
    clients: clients.size,
    replayEvents: replay.length,
  });
});

const httpServer = app.listen(HTTP_PORT, () => {
  console.log(
    `[gateway] HTTP listening on ${HTTP_PORT}; `
    + `WebSocket listening on ${WEBSOCKET_PORT}`,
  );
});

function shutdown() {
  websocketServer.close();
  httpServer.close(() => process.exit(0));
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
