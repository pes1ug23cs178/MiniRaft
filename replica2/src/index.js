import express from "express";
import fetch from "node-fetch";

// -----------------------------------------------------------------------------
// Configuration and Raft timing
// -----------------------------------------------------------------------------
const id = process.env.REPLICA_ID;
const port = Number(process.env.PORT || 4001);
const peers = (process.env.PEERS || "").split(",").filter(Boolean);
const gateway = process.env.GATEWAY_URL || "http://gateway:8081";
const heartbeatMs = 150;
const electionDelay = () => 500 + Math.random() * 300;
const majority = Math.floor((peers.length + 1) / 2) + 1;

// -----------------------------------------------------------------------------
// Persistent-style Raft state (kept in memory for this teaching project)
// -----------------------------------------------------------------------------
let role = "follower";
let term = 0;
let votedFor = null;
let leaderId = null;
let log = [];
let commitIndex = -1;
let lastApplied = -1;
let electionTimer;
let heartbeatTimer;
let shuttingDown = false;

// Leader replication progress for each follower.
const nextIndex = Object.fromEntries(peers.map(peer => [peer, 0]));
const matchIndex = Object.fromEntries(peers.map(peer => [peer, -1]));

const app = express();
app.use(express.json({ limit: "1mb" }));

const address = `http://${id}:${port}`;
const lastEntry = () => log[log.length - 1] || { index: -1, term: 0 };

// All replica-to-replica HTTP calls use a short timeout so one failed node does
// not block elections or the leader heartbeat loop.
const send = async (url, body, timeout = 450) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);

  try {
    return await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal
    });
  } finally {
    clearTimeout(timer);
  }
};

// A follower starts a new election only after a randomized timeout without a
// valid heartbeat or vote. Resetting this timer prevents unnecessary campaigns.
const resetElection = () => {
  clearTimeout(electionTimer);
  if (!shuttingDown) {
    electionTimer = setTimeout(startElection, electionDelay());
  }
};

// Move to a newer term and follower role when an RPC reveals newer leadership.
function becomeFollower(newTerm = term, newLeader = null) {
  if (newTerm < term) return;
  if (newTerm > term) votedFor = null;

  term = newTerm;
  role = "follower";
  leaderId = newLeader;
  clearInterval(heartbeatTimer);
  heartbeatTimer = null;
  resetElection();
}

// Apply committed entries to the drawing state machine. Only the leader sends
// the resulting event to the gateway, avoiding duplicate browser broadcasts.
function applyCommitted() {
  while (lastApplied < commitIndex) {
    lastApplied += 1;
    const entry = log[lastApplied];

    if (role === "leader" && entry && entry.command.type !== "noop") {
      send(`${gateway}/broadcast`, {
        ...entry.command,
        index: entry.index,
        term: entry.term
      }).catch(() => {});
    }
  }
}

// AppendEntries replicates the suffix beginning at nextIndex for one follower.
// A failed consistency check backs nextIndex up so the leader can retry.
async function sendAppend(peer) {
  if (role !== "leader") return;

  const next = Math.max(0, nextIndex[peer] ?? log.length);
  const previous = log[next - 1] || { index: -1, term: 0 };
  const response = await send(`${peer}/append-entries`, {
    term,
    leaderId: address,
    prevLogIndex: previous.index,
    prevLogTerm: previous.term,
    entries: log.slice(next),
    leaderCommit: commitIndex
  });
  const data = await response.json();

  if (data.term > term) {
    becomeFollower(data.term);
    return;
  }
  if (data.success) {
    matchIndex[peer] = data.matchIndex;
    nextIndex[peer] = data.matchIndex + 1;
  } else if (nextIndex[peer] > 0) {
    nextIndex[peer] -= 1;
  }
}

// Send only the committed missing range through the explicit catch-up RPC.
// This lets an empty restarted follower recover without a new client command.
async function sendCommittedSync(peer, followerLogLength) {
  if (role !== "leader" || followerLogLength >= commitIndex + 1) return;

  const start = Math.max(0, followerLogLength);
  const previous = log[start - 1] || { index: -1, term: 0 };
  const response = await send(`${peer}/sync-log`, {
    term,
    leaderId: address,
    prevLogIndex: previous.index,
    prevLogTerm: previous.term,
    entries: log.slice(start, commitIndex + 1),
    leaderCommit: commitIndex
  });
  const data = await response.json();

  if (data.term > term) {
    becomeFollower(data.term);
    return;
  }
  if (data.accepted) {
    matchIndex[peer] = data.matchIndex;
    nextIndex[peer] = data.logLength;
  }
}

// Dedicated 150 ms heartbeat: it resets follower election timers and reports
// follower log length so the leader can trigger committed-entry catch-up.
async function heartbeat(peer) {
  try {
    const response = await send(`${peer}/heartbeat`, {
      term,
      leaderId: address
    });
    const data = await response.json();

    if (data.term > term) becomeFollower(data.term);
    if (role === "leader") {
      const followerLength = Number(data.logLength ?? 0);
      nextIndex[peer] = Math.min(nextIndex[peer] ?? log.length, followerLength);

      if (followerLength < commitIndex + 1) {
        await sendCommittedSync(peer, followerLength);
      } else if ((nextIndex[peer] ?? log.length) < log.length) {
        await sendAppend(peer);
      }
    }
  } catch (_) {
    // A temporarily unavailable replica is expected during a failure demo.
  }
}

function startHeartbeats() {
  clearInterval(heartbeatTimer);
  heartbeatTimer = setInterval(
    () => peers.forEach(peer => heartbeat(peer)),
    heartbeatMs
  );
  peers.forEach(peer => heartbeat(peer));
}

// A candidate becomes leader after receiving a majority of votes. The no-op
// entry establishes the new term and is replicated like any other log entry.
async function becomeLeader() {
  role = "leader";
  leaderId = address;
  clearTimeout(electionTimer);

  peers.forEach(peer => {
    nextIndex[peer] = log.length;
    matchIndex[peer] = -1;
  });

  log.push({ index: log.length, term, command: { type: "noop" } });
  await send(`${gateway}/leader-announce`, { leader: address }).catch(() => {});
  startHeartbeats();
}

// Start an election, vote for ourselves, and ask every peer for a vote. A
// higher term observed in a response immediately ends this campaign.
async function startElection() {
  if (shuttingDown || role === "leader") return;

  role = "candidate";
  term += 1;
  votedFor = id;
  leaderId = null;
  resetElection();

  const tail = lastEntry();
  const request = {
    term,
    candidateId: id,
    lastLogIndex: tail.index,
    lastLogTerm: tail.term
  };
  let votes = 1;

  await Promise.all(peers.map(async peer => {
    try {
      const response = await send(`${peer}/request-vote`, request, 350);
      const data = await response.json();

      if (data.term > term) {
        becomeFollower(data.term);
      } else if (data.voteGranted) {
        votes += 1;
      }
    } catch (_) {}
  }));

  if (role === "candidate" && votes >= majority) await becomeLeader();
}

// -----------------------------------------------------------------------------
// Inspection and log synchronization endpoints
// -----------------------------------------------------------------------------
app.get("/health", (req, res) => res.json({
  status: "ok", id, role, term, leaderId, logLength: log.length, commitIndex
}));

app.get("/status", (req, res) => res.json({
  id, role, term, leaderId, log, logLength: log.length, commitIndex, lastApplied
}));

app.get("/log", (req, res) => res.json({ entries: log, commitIndex }));
app.get("/sync-log", (req, res) => res.json({ term, leaderId, entries: log, commitIndex }));

// POST /sync-log is the leader-to-follower incremental catch-up endpoint.
app.post("/sync-log", (req, res) => {
  const body = req.body;

  if (Number(body.term) < term) {
    return res.status(409).json({
      term, accepted: false, logLength: log.length, matchIndex: log.length - 1
    });
  }

  becomeFollower(Number(body.term), body.leaderId);
  if (
    body.prevLogIndex >= 0 &&
    (!log[body.prevLogIndex] || log[body.prevLogIndex].term !== body.prevLogTerm)
  ) {
    return res.json({
      term, accepted: false, logLength: log.length, matchIndex: log.length - 1
    });
  }

  for (const entry of body.entries || []) {
    if (log[entry.index] && log[entry.index].term !== entry.term) {
      log.splice(entry.index);
    }
    if (!log[entry.index]) log.push(entry);
  }

  commitIndex = Math.min(
    Number(body.leaderCommit ?? commitIndex),
    log.length - 1
  );
  applyCommitted();
  resetElection();
  res.json({
    term, accepted: true, logLength: log.length,
    matchIndex: log.length - 1, commitIndex
  });
});

// -----------------------------------------------------------------------------
// Raft voting and replication endpoints
// -----------------------------------------------------------------------------
app.post("/request-vote", (req, res) => {
  const candidateTerm = Number(req.body.term || 0);
  if (candidateTerm > term) becomeFollower(candidateTerm);
  if (candidateTerm < term) return res.json({ term, voteGranted: false });

  const tail = lastEntry();
  const upToDate = req.body.lastLogTerm > tail.term || (
    req.body.lastLogTerm === tail.term && req.body.lastLogIndex >= tail.index
  );
  const canVote = (
    (votedFor === null || votedFor === req.body.candidateId) && upToDate
  );

  if (canVote) {
    votedFor = req.body.candidateId;
    resetElection();
  }
  res.json({ term, voteGranted: canVote });
});

function appendEntries(req, res) {
  const body = req.body;
  if (body.term < term) {
    return res.json({ term, success: false, matchIndex: log.length - 1 });
  }

  becomeFollower(body.term, body.leaderId);
  if (
    body.prevLogIndex >= 0 &&
    (!log[body.prevLogIndex] || log[body.prevLogIndex].term !== body.prevLogTerm)
  ) {
    return res.json({ term, success: false, matchIndex: log.length - 1 });
  }

  for (const entry of body.entries || []) {
    if (log[entry.index] && log[entry.index].term !== entry.term) {
      log.splice(entry.index);
    }
    if (!log[entry.index]) log.push(entry);
  }

  commitIndex = Math.min(
    Number(body.leaderCommit ?? commitIndex),
    log.length - 1
  );
  applyCommitted();
  res.json({ term, success: true, matchIndex: log.length - 1 });
}

app.post("/append-entries", appendEntries);

app.post("/heartbeat", (req, res) => {
  if (req.body.term < term) {
    return res.json({
      term, success: false, logLength: log.length, commitIndex
    });
  }

  becomeFollower(Number(req.body.term), req.body.leaderId);
  res.json({ term, success: true, logLength: log.length, commitIndex });
});

// -----------------------------------------------------------------------------
// Client command endpoints: only the current leader can commit a command.
// -----------------------------------------------------------------------------
async function acceptCommand(req, res) {
  if (role !== "leader") return res.status(307).json({ leader: leaderId });

  const entry = { index: log.length, term, command: req.body };
  log.push(entry);
  let acknowledgements = 1;

  await Promise.all(peers.map(async peer => {
    try {
      await sendAppend(peer);
      if (matchIndex[peer] >= entry.index) acknowledgements += 1;
    } catch (_) {}
  }));

  if (acknowledgements >= majority) {
    commitIndex = entry.index;
    applyCommitted();
    res.status(201).json({ accepted: true, index: entry.index, term });
  } else {
    res.status(503).json({ accepted: false, error: "majority unavailable" });
  }
}

app.post("/command", acceptCommand);
app.post("/stroke", acceptCommand);

// -----------------------------------------------------------------------------
// Startup and graceful shutdown
// -----------------------------------------------------------------------------
const server = app.listen(port, () => {
  console.log(`[${id}] listening on ${port}`);
  resetElection();
});

process.on("SIGTERM", () => {
  shuttingDown = true;
  clearTimeout(electionTimer);
  clearInterval(heartbeatTimer);
  server.close(() => process.exit(0));
});

process.on("SIGINT", () => process.emit("SIGTERM"));