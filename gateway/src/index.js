import express from "express";
import { WebSocketServer } from "ws";
import fetch from "node-fetch";

const replicas = (process.env.REPLICAS || "").split(",").filter(Boolean);
const clients = new Set();
const replay = [];
let leader = null;
let discovering = null;
const timeout = 500;
const request = async (url, options = {}) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try { return await fetch(url, { ...options, signal: controller.signal }); }
  finally { clearTimeout(timer); }
};
async function discoverLeader() {
  if (leader) { try { const response = await request(`${leader}/health`); if (response.ok && (await response.json()).role === "leader") return leader; } catch (_) {} }
  for (const replica of replicas) {
    try { const response = await request(`${replica}/status`); const status = await response.json(); if (response.ok && status.role === "leader") return leader = replica; } catch (_) {}
  }
  leader = null; return null;
}
async function getLeader() { if (!discovering) discovering = discoverLeader().finally(() => { discovering = null; }); return discovering; }
function broadcast(message) {
  const payload = JSON.stringify(message);
  for (const client of clients) if (client.readyState === 1) client.send(payload);
}
async function replayFromCluster() {
  const node = await getLeader(); if (!node) return;
  try { const response = await request(`${node}/sync-log`); const data = await response.json(); replay.length = 0; for (const entry of data.entries || []) if (entry.command?.type !== "noop") replay.push({ ...entry.command, index: entry.index, term: entry.term }); } catch (_) {}
}
async function forward(command) {
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const node = await getLeader(); if (!node) { await new Promise(resolve => setTimeout(resolve, 200)); continue; }
    try {
      const response = await request(`${node}/command`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(command) });
      if (response.status === 307) { leader = (await response.json()).leader; continue; }
      if (response.ok) return true;
    } catch (_) { leader = null; }
    leader = null;
  }
  return false;
}
const wss = new WebSocketServer({ port: 8080 });
wss.on("connection", async socket => {
  clients.add(socket); await replayFromCluster();
  for (const event of replay) if (socket.readyState === 1) socket.send(JSON.stringify(event));
  socket.on("message", async raw => { try { const message = JSON.parse(raw); if (message.type === "ping") return socket.send(JSON.stringify({ type: "pong" })); await forward(message); } catch (_) {} });
  socket.on("close", () => clients.delete(socket)); socket.on("error", () => clients.delete(socket));
});
const app = express(); app.use(express.json({ limit: "1mb" }));
app.post("/broadcast", (req, res) => { const event = req.body; if (event.type === "clear") replay.length = 0; else if (event.type !== "noop") replay.push(event); broadcast(event); res.json({ ok: true, clients: clients.size }); });
app.post("/leader-announce", (req, res) => { if (!req.body.leader) return res.status(400).json({ error: "leader is required" }); leader = req.body.leader; res.json({ ok: true, leader }); });
app.get("/health", (req, res) => res.json({ status: "ok", leader, replicas, clients: clients.size, replayEvents: replay.length }));
const server = app.listen(8081, () => console.log("[gateway] HTTP 8081, WebSocket 8080"));
process.on("SIGTERM", () => { wss.close(); server.close(() => process.exit(0)); });
process.on("SIGINT", () => process.emit("SIGTERM"));
