// Kafka-path call-flow test — NEGATIVE -> POSITIVE sentiment arc (starts angry,
// agent resolves, ends grateful).
//   • call events    → ciap-kafka HTTP  POST /api/webhooks/bicom/events   (Kafka HTTP)
//   • live transcript → ciap-kafka WebSocket  /transcript                 (Kafka WS)
// Both land on Kafka telephony.events; the consumer forwards to ciap-api.
//
// Run (inputs = numbers + extension):
//   NUMBER=+13120000031 CONNECTED_NUM=100 CONNECTED_NAME="Admin One" TENANT=200 DURATION=60 \
//   KAFKA_HTTP=http://localhost:8081 KAFKA_WS=ws://localhost:8081/transcript \
//   node scripts/post_call_flow_negative_to_positive_ws.mjs
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
const NUMBER = process.env.NUMBER || "+13120000031";        // customer external number
const DID = process.env.DID || "+17733212627";
const CONNECTED_NUM = process.env.CONNECTED_NUM || "100";    // agent extension (popup owner)
const CONNECTED_NAME = process.env.CONNECTED_NAME || "Admin One";
const DURATION = Number(process.env.DURATION || 60);

const LINKED = `n2p-${Date.now()}`;
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
  ["caller", "I am really upset. I was charged twice this month and it is unacceptable."],
  ["callee", "I'm sorry about that. Let me pull up your account and check the charges."],
  ["caller", "Please do. I've been frustrated all morning trying to reach someone."],
  ["callee", "I see the duplicate charge from the 1st. That was applied by mistake."],
  ["caller", "So it's your error. That is frustrating to hear, honestly."],
  ["callee", "You're right, and I apologize. I'm reversing the duplicate charge right now."],
  ["caller", "Okay, that's a start. How long until I get the money back?"],
  ["callee", "It'll reflect within one billing cycle, and I'll email a confirmation today."],
  ["caller", "Alright, that actually sounds reasonable. Thank you for sorting it out."],
  ["callee", "Of course. I've also added a small credit for the trouble."],
  ["caller", "Oh, that's really kind of you. I appreciate that."],
  ["callee", "You're very welcome. Is there anything else I can help with?"],
  ["caller", "No, that's everything. Good, this turned out much better than I expected."],
  ["callee", "I'm so glad. Have a wonderful day!"],
  ["caller", "You too. Thank you so much, this was excellent service."],
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
  console.log(`== NEGATIVE->POSITIVE call ${LINKED} | agent ext ${CONNECTED_NUM} | customer ${NUMBER} | tenant ${TENANT} ==`);
  const ws = await openWs();
  await postEvent("event_call_started", "RINGING"); await sleep(600);
  await postEvent("event_call_connected", "UP"); await sleep(400);
  const gap = Math.max(200, Math.floor((DURATION * 1000) / LINES.length));
  let i = 0;
  for (const [speaker, text] of LINES) { i++; sendTranscript(ws, speaker, text, i); await sleep(gap); }
  await postEvent("event_call_finished", "DOWN"); await sleep(400);
  ws.close();
  console.log(`done. linkedId=${LINKED} (NEGATIVE->POSITIVE)`);
  process.exit(0);
}
main().catch((e) => { console.error(e.message); process.exit(1); });
