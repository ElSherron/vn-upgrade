// server.js: room-based voice relay for games
// Setup:  npm init -y && npm install ws && node server.js

const { WebSocketServer, WebSocket } = require("ws");

const PORT = 8080;
const MAX_ROOM_SIZE = 16;
const MAX_FRAME_BYTES = 64 * 1024;
const MAX_BUFFERED_BYTES = 256 * 1024;

const wss = new WebSocketServer({ port: PORT, maxPayload: MAX_FRAME_BYTES });
const rooms = new Map(); // roomName -> Set<ws>
let nextId = 1;

const send = (ws, obj) => ws.readyState === WebSocket.OPEN && ws.send(JSON.stringify(obj));

function toRoom(sender, data, binary) {
  const room = rooms.get(sender.room);
  if (!room) return;
  for (const c of room) {
    if (c === sender || c.readyState !== WebSocket.OPEN) continue;
    if (c.bufferedAmount > MAX_BUFFERED_BYTES) continue; // skip laggy clients
    c.send(data, { binary });
  }
}

function leaveRoom(ws) {
  const room = rooms.get(ws.room);
  if (!room) return;
  room.delete(ws);
  if (room.size === 0) rooms.delete(ws.room);
  else toRoom(ws, JSON.stringify({ type: "peer_leave", id: ws.id }), false);
  ws.room = null;
}

wss.on("connection", (ws) => {
  ws.id = nextId;
  nextId = (nextId % 65535) + 1; // ids fit in 2 bytes
  ws.room = null;
  ws.talking = false;

  ws.on("message", (data, isBinary) => {
    // Live audio: only relayed inside the sender's room, and only while they are talking
    if (isBinary) {
      if (!ws.room || !ws.talking) return;
      const header = Buffer.alloc(2);
      header.writeUInt16LE(ws.id, 0);
      toRoom(ws, Buffer.concat([header, data]), true);
      return;
    }

    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.type === "join" && !ws.room) {
      const roomName = String(msg.room || "lobby").slice(0, 32);
      const room = rooms.get(roomName) || new Set();
      if (room.size >= MAX_ROOM_SIZE) return send(ws, { type: "error", message: "Room is full" });

      ws.name = String(msg.name || "Player").slice(0, 24);
      const peers = [...room].map((c) => ({ id: c.id, name: c.name, talking: c.talking }));
      room.add(ws);
      rooms.set(roomName, room);
      ws.room = roomName;

      send(ws, { type: "welcome", id: ws.id, peers });
      toRoom(ws, JSON.stringify({ type: "peer_join", id: ws.id, name: ws.name }), false);
      console.log(`${ws.name} (#${ws.id}) joined "${roomName}" (${room.size})`);
    }

    if (msg.type === "talk_start" && ws.room && !ws.talking) {
      ws.talking = true;
      toRoom(ws, JSON.stringify({ type: "talk_start", id: ws.id }), false);
    }
    if (msg.type === "talk_end" && ws.room && ws.talking) {
      ws.talking = false;
      toRoom(ws, JSON.stringify({ type: "talk_end", id: ws.id }), false);
    }
  });

  ws.on("close", () => leaveRoom(ws));
  ws.on("error", (err) => console.error(`Socket error (#${ws.id}):`, err.message));
});

console.log(`Voice server running on ws://localhost:${PORT}`);
