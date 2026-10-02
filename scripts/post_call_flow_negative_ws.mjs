// Kafka-path call-flow test — FULL NEGATIVE sentiment (angry, unresolved, escalating).
//   • call events    → ciap-kafka HTTP  POST /api/webhooks/bicom/events   (Kafka HTTP)
//   • live transcript → ciap-kafka WebSocket  /transcript                 (Kafka WS)
// Both land on Kafka telephony.events; the consumer forwards to ciap-api.
//
// Run (inputs = numbers + extension):
//   NUMBER=+13120000030 CONNECTED_NUM=100 CONNECTED_NAME="Admin One" TENANT=200 DURATION=60 \
//   KAFKA_HTTP=http://localhost:8081 KAFKA_WS=ws://localhost:8081/transcript \
//   node scripts/post_call_flow_negative_ws.mjs
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
const NUMBER = process.env.NUMBER || "+13120000030";        // customer external number
const DID = process.env.DID || "+17733212627";
const CONNECTED_NUM = process.env.CONNECTED_NUM || "100";    // agent extension (popup owner)
const CONNECTED_NAME = process.env.CONNECTED_NAME || "Admin One";
const DURATION = Number(process.env.DURATION || 60);

const LINKED = `neg-${Date.now()}`;
// Transcript linked_id on the wire (default the real LINKED; "%LINKED_ID%" = phone-match path).
const TRANSCRIPT_LINKED_ID = process.env.TRANSCRIPT_LINKED_ID || LINKED;
const EVID = `evt-${Date.now()}`;
const COMPLETED = "conversation.item.input_audio_transcription.completed";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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
  ["caller", "I am absolutely furious. I was charged twice and no one warned me. This is terrible."],
  ["callee", "I'm sorry to hear that. Let me look into the duplicate charge."],
  ["caller", "Sorry doesn't cut it. This is completely unacceptable and I am beyond frustrated."],
  ["callee", "I understand. I can see two charges on the account."],
  ["caller", "Of course there are. Your billing is a disaster and I've wasted my whole morning."],
  ["callee", "I apologize for the inconvenience. I can start a refund request."],
  ["caller", "A request? I want my money back now, not a request. This is outrageous."],
  ["callee", "I hear you. The refund typically takes a few days to process."],
  ["caller", "Days? That's ridiculous. I want to escalate this to a manager immediately."],
  ["callee", "I can escalate this for you right away."],
  ["caller", "You should. I am an angry customer and I'm ready to cancel subscription."],
  ["callee", "I don't want to lose you. Let me see what I can do."],
  ["caller", "It's too late. Your service has been terrible for months."],
  ["callee", "I'm truly sorry you've had this experience."],
  ["caller", "This is the worst support I have ever dealt with. I am extremely angry."],
  ["caller", "Just escalate it and call me back. I've had enough."],
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
  console.log(`== NEGATIVE call ${LINKED} | agent ext ${CONNECTED_NUM} | customer ${NUMBER} | tenant ${TENANT} ==`);
  const ws = await openWs();
  await postEvent("event_call_started", "RINGING"); await sleep(600);
  await postEvent("event_call_connected", "UP"); await sleep(400);
  const gap = Math.max(200, Math.floor((DURATION * 1000) / LINES.length));
  let i = 0;
  for (const [speaker, text] of LINES) { i++; sendTranscript(ws, speaker, text, i); await sleep(gap); }
  await postEvent("event_call_finished", "DOWN"); await sleep(400);
  ws.close();
  console.log(`done. linkedId=${LINKED} (NEGATIVE)`);
  process.exit(0);
}
main().catch((e) => { console.error(e.message); process.exit(1); });
