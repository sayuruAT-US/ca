#!/usr/bin/env node
// Connects to ciap-api's realtime WebSocket (what the frontend popup consumes) and
// logs each CALL_EVENT / TRANSCRIPT message. Usage:
//   node scripts/ws_listen.js [ws://host:8080/ws/transcript] [seconds]
// Uses Node's built-in WebSocket (Node 22+); falls back to the `ws` package if present.
const url = process.argv[2] || process.env.WS_URL || "ws://localhost:8080/ws/transcript";
const seconds = Number(process.argv[3] || 30);
let WS = globalThis.WebSocket;
if (!WS) { try { WS = require("ws"); } catch { console.error("No global WebSocket (need Node 22+) and 'ws' not installed. Run: npm i ws"); process.exit(1); } }
const ws = new WS(url);
const onMsg = (raw) => {
  let m; try { m = JSON.parse(raw.toString()); } catch { m = raw.toString(); }
  const t = m && m.type;
  if (t === "CALL_EVENT")      console.log(`CALL_EVENT   eventName=${m.data?.eventName} number=${m.data?.number} linkedId=${m.data?.linkedId}`);
  else if (t === "TRANSCRIPT") console.log(`TRANSCRIPT   speaker=${m.data?.speaker} text="${m.data?.text}"`);
  else console.log("MSG", JSON.stringify(m).slice(0, 160));
};
ws.onopen ? (ws.onopen = () => console.log("WS connected ->", url)) : ws.on("open", () => console.log("WS connected ->", url));
ws.onmessage ? (ws.onmessage = (e) => onMsg(e.data)) : ws.on("message", onMsg);
if (ws.on) { ws.on("error", (e) => console.log("WS error:", e.message)); ws.on("close", () => console.log("WS closed")); }
else { ws.onerror = (e) => console.log("WS error:", e.message || e); ws.onclose = () => console.log("WS closed"); }
setTimeout(() => { try { ws.close(); } catch {} process.exit(0); }, seconds * 1000);
