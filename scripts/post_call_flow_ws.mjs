// End-to-end call-flow test for the NEW ingestion paths:
//   • call events   → ciap-kafka HTTP  POST /api/webhooks/bicom/events   (Kafka HTTP)
//   • live transcript → ciap-kafka WebSocket  /transcript                (Kafka WS)
//
// Both land on Kafka `telephony.events`; the ciap-kafka consumer forwards events to
// ciap-api /api/call-events and transcripts to /api/transcripts, which persist and
// then push to the browser over ciap-api's per-user WS (/ws/transcript).
//
// Run (resolves `ws` from ciap-web; override with WS_MODULE):
//   NUMBER=+12813308004 CONNECTED_NUM=101 CONNECTED_NAME="Greg Iphone" TENANT=200 \
//   DURATION=60 KAFKA_HTTP=http://localhost:8081 KAFKA_WS=ws://localhost:8081/transcript \
//   node scripts/post_call_flow_ws.mjs
// Resolve the `ws` module without a machine-specific absolute path:
//   WS_MODULE env override -> installed `ws` -> ws in a sibling ciap-web checkout.
async function loadWs() {
  if (process.env.WS_MODULE) return (await import(process.env.WS_MODULE)).default;
  try { return (await import("ws")).default; } catch { /* not installed here */ }
  const { fileURLToPath } = await import("node:url");
  const { dirname, resolve } = await import("node:path");
  const here = dirname(fileURLToPath(import.meta.url)); // <repo>/ciap-kafka/scripts
  return (await import(resolve(here, "../../ciap-web/node_modules/ws/index.js"))).default;
}
const WebSocket = await loadWs();

const KAFKA_HTTP = (process.env.KAFKA_HTTP || "http://localhost:8081").replace(/\/$/, "");
const KAFKA_WS = process.env.KAFKA_WS || "ws://localhost:8081/transcript";
const TENANT = Number(process.env.TENANT || 200);
const NUMBER = process.env.NUMBER || "+13129576371";        // customer external number
const DID = process.env.DID || "+17733212627";
const CONNECTED_NUM = process.env.CONNECTED_NUM || "101";     // agent extension
const CONNECTED_NAME = process.env.CONNECTED_NAME || "Maria Gomez";
const DURATION = Number(process.env.DURATION || 60);

const LINKED = `call-${Date.now()}`;
// Transcript linked_id on the wire. Default: the real LINKED (scenario: transcript has
// linked_id). Set TRANSCRIPT_LINKED_ID="%LINKED_ID%" to simulate PBXware's unrendered
// placeholder (scenario: events have linked_id but the transcript doesn't).
const TRANSCRIPT_LINKED_ID = process.env.TRANSCRIPT_LINKED_ID || LINKED;
const EVID = `evt-${Date.now()}`;
const COMPLETED = "conversation.item.input_audio_transcription.completed";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── Call events → Kafka HTTP webhook ────────────────────────────────────────
async function postEvent(event, state) {
  const body = {
    event, event_id: `${EVID}-${event}`, tenant_code: TENANT,
    payload: {
      linked_id: LINKED, uid: `${LINKED}.1`, call_type: "inbound", state,
      number: NUMBER, did: DID, connected_num: CONNECTED_NUM, connected_name: CONNECTED_NAME,
      is_incoming: true, record_status: "recording",
    },
  };
  const res = await fetch(`${KAFKA_HTTP}/api/webhooks/bicom/events`, {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
  });
  console.log(`  [HTTP ${res.status}] event ${event} (${state})`);
}

// ── Live transcript → Kafka WebSocket (completed frames only are forwarded) ──
function sendTranscript(ws, speaker, text, i) {
  const frame = {
    transcribed_data: {
      transcript: text, speaker, item_id: `${LINKED}-${i}`, event_id: `${EVID}-t${i}`,
      type: COMPLETED, content_index: 0, obfuscation: "",
    },
    metadata: {
      linked_id: TRANSCRIPT_LINKED_ID, caller_id: NUMBER, callee_name: CONNECTED_NAME,
      channel_name: `agent-${CONNECTED_NUM}`,
    },
  };
  ws.send(JSON.stringify(frame));
  console.log(`  [WS] ${speaker}: ${text}`);
}

const LINES = [
  ["caller", "Hi, I'm calling about my last invoice — it looks a lot higher than usual."],
  ["callee", "I can help with that. May I have the account number on the invoice, please?"],
  ["caller", "Sure, it's 4471-208."],
  ["callee", "Thank you. Let me pull that up… one moment."],
  ["callee", "I see the invoice dated the 1st. It's about forty dollars higher than last month."],
  ["caller", "Right, that's what I noticed. I didn't change my plan."],
  ["callee", "You're correct. There's a one-time setup fee that was applied by mistake."],
  ["caller", "Good, that explains it. Can you remove it?"],
  ["callee", "Absolutely. I'm crediting the setup fee back to your account right now."],
  ["caller", "Great, thank you. How long until I see the credit?"],
  ["callee", "It'll reflect within one billing cycle, and you'll get an email confirmation today."],
  ["caller", "Perfect. Will my next invoice be back to the normal amount?"],
  ["callee", "Yes — your next invoice returns to your regular monthly rate."],
  ["caller", "That's a relief. While I have you — is my autopay still active?"],
  ["callee", "It is, on the card ending 6411. Nothing else needs changing."],
  ["caller", "Wonderful. That's all I needed today."],
  ["callee", "Happy to help. I've noted the credit on your account. Have a great day!"],
  ["caller", "You too, thanks for sorting it out so quickly."],
];

function openWs() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(KAFKA_WS);
    ws.on("open", () => console.log(`WS connected -> ${KAFKA_WS}`));
    ws.on("message", (buf) => {
      try {
        const m = JSON.parse(buf.toString());
        if (m.type === "connection_ack") { console.log("  WS connection_ack"); resolve(ws); }
      } catch { /* ignore */ }
    });
    ws.on("error", (e) => reject(new Error(`WS error: ${e.message}`)));
    setTimeout(() => reject(new Error("WS connection_ack timeout")), 5000);
  });
}

async function main() {
  console.log(`== call ${LINKED} | agent ext ${CONNECTED_NUM} (${CONNECTED_NAME}) | tenant ${TENANT} ==`);
  const ws = await openWs();

  console.log("== call started (opens popup for the owning agent) ==");
  await postEvent("event_call_started", "RINGING"); await sleep(800);
  await postEvent("event_call_updated", "RINGING"); await sleep(600);
  await postEvent("event_call_connected", "UP"); await sleep(600);

  const gap = Math.max(200, Math.floor((DURATION * 1000) / LINES.length));
  console.log(`== live transcript over ~${DURATION}s (~${gap}ms/turn) via Kafka WS ==`);
  let i = 0;
  const mid = Math.floor(LINES.length / 2);
  for (const [speaker, text] of LINES) {
    i++;
    sendTranscript(ws, speaker, text, i);
    if (i === mid) await postEvent("event_call_updated", "UP");
    await sleep(gap);
  }

  console.log("== call finished ==");
  await postEvent("event_call_finished", "DOWN");
  await sleep(400);
  ws.close();
  console.log(`done. linkedId=${LINKED}`);
  process.exit(0);
}

main().catch((e) => { console.error(e.message); process.exit(1); });
