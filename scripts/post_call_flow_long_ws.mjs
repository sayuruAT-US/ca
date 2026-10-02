// Kafka-path call-flow test — LONG generic support call (~50 turns, neutral -> satisfied).
//   events -> ciap-kafka HTTP /api/webhooks/bicom/events ; transcript -> ciap-kafka WS /transcript
//
// Run: NUMBER=+13120000040 CONNECTED_NUM=100 CONNECTED_NAME="Admin One" TENANT=200 DURATION=120 \
//   KAFKA_HTTP=http://localhost:8081 KAFKA_WS=ws://localhost:8081/transcript \
//   node scripts/post_call_flow_long_ws.mjs
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
const NUMBER = process.env.NUMBER || "+13120000040";
const DID = process.env.DID || "+17733212627";
const CONNECTED_NUM = process.env.CONNECTED_NUM || "100";
const CONNECTED_NAME = process.env.CONNECTED_NAME || "Admin One";
const DURATION = Number(process.env.DURATION || 120);

const LINKED = `long-${Date.now()}`;
const TRANSCRIPT_LINKED_ID = process.env.TRANSCRIPT_LINKED_ID || LINKED;
const EVID = `evt-${Date.now()}`;
const COMPLETED = "conversation.item.input_audio_transcription.completed";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function postEvent(event, state) {
  const body = { event, event_id: `${EVID}-${event}`, tenant_code: TENANT,
    payload: { linked_id: LINKED, uid: `${LINKED}.1`, call_type: "inbound", state,
      number: NUMBER, did: DID, connected_num: CONNECTED_NUM, connected_name: CONNECTED_NAME,
      is_incoming: true, record_status: "recording" } };
  const res = await fetch(`${KAFKA_HTTP}/api/webhooks/bicom/events`, {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  console.log(`  [HTTP ${res.status}] event ${event} (${state})`);
}
function sendTranscript(ws, speaker, text, i) {
  ws.send(JSON.stringify({
    transcribed_data: { transcript: text, speaker, item_id: `${LINKED}-${i}`, event_id: `${EVID}-t${i}`,
      type: COMPLETED, content_index: 0, obfuscation: "" },
    metadata: { linked_id: TRANSCRIPT_LINKED_ID, caller_id: NUMBER, callee_name: CONNECTED_NAME,
      channel_name: `agent-${CONNECTED_NUM}` } }));
  console.log(`  [WS] ${speaker}: ${text}`);
}

const LINES = [
  ["caller", "Hi, I'm calling about my latest invoice. It looks higher than what I usually pay."],
  ["callee", "I'd be happy to help. Can I start with the account number on the invoice?"],
  ["caller", "Sure, it's 4471-208."],
  ["callee", "Thank you. Let me pull that up for you. One moment please."],
  ["callee", "Alright, I have your account open. I see the invoice dated the first."],
  ["caller", "Yes, that's the one. It's about forty dollars more than last month."],
  ["callee", "Let me compare it against your previous statement line by line."],
  ["caller", "Okay, that works for me."],
  ["callee", "Your base plan is the same at fifty-nine ninety-nine."],
  ["caller", "Right, I haven't changed my plan."],
  ["callee", "I do see a one-time setup fee of thirty dollars applied this cycle."],
  ["caller", "A setup fee? I've been a customer for three years."],
  ["callee", "That's a fair point. It looks like it was added in error during a system update."],
  ["caller", "Okay, so can that be removed?"],
  ["callee", "Yes, I can credit that back to your account right now."],
  ["caller", "Great. What about the other ten dollars?"],
  ["callee", "That's a prorated charge for the two extra lines added on the fifteenth."],
  ["caller", "Oh right, my kids' phones. That one's legitimate."],
  ["callee", "Correct. So going forward only that prorated amount will remain."],
  ["caller", "That makes sense. When will the credit show up?"],
  ["callee", "It'll reflect within one billing cycle, and you'll get an email today."],
  ["caller", "Perfect. Will my next invoice be back to normal then?"],
  ["callee", "Your next invoice returns to your regular rate plus the two added lines."],
  ["caller", "Understood. Can you tell me the exact amount for next month?"],
  ["callee", "It will be sixty-nine ninety-nine before taxes."],
  ["caller", "Alright. And is my autopay still active on the card ending 6411?"],
  ["callee", "Yes, autopay is active on that card. Nothing needs to change."],
  ["caller", "Good. While I have you, can I add international calling?"],
  ["callee", "Absolutely. There's a ten dollar monthly add-on for that."],
  ["caller", "What countries does that cover?"],
  ["callee", "It covers Canada, Mexico, the UK, and most of Europe."],
  ["caller", "That's what I need. Please add it."],
  ["callee", "Done. It's active immediately and will appear on your next bill."],
  ["caller", "Thank you. Do I get a confirmation for that too?"],
  ["callee", "Yes, a separate confirmation email is on its way now."],
  ["caller", "Great. One more thing, can I switch my due date to the fifteenth?"],
  ["callee", "I can move your billing date to the fifteenth starting next cycle."],
  ["caller", "That would help a lot with my paychecks."],
  ["callee", "Understood. I've updated the due date to the fifteenth."],
  ["caller", "You've been really helpful today, thank you."],
  ["callee", "My pleasure. Let me quickly recap everything we did."],
  ["callee", "I removed the thirty dollar setup fee and credited your account."],
  ["caller", "Yes."],
  ["callee", "I added international calling for ten dollars a month."],
  ["caller", "Correct."],
  ["callee", "And I moved your billing date to the fifteenth."],
  ["caller", "That's everything, perfect."],
  ["callee", "Is there anything else I can help you with today?"],
  ["caller", "No, that covers it. Thanks again for your patience."],
  ["callee", "You're very welcome. Have a wonderful day!"],
];

function openWs() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(KAFKA_WS);
    ws.on("open", () => console.log(`WS connected -> ${KAFKA_WS}`));
    ws.on("message", (buf) => { try { const m = JSON.parse(buf.toString());
      if (m.type === "connection_ack") { console.log("  WS connection_ack"); resolve(ws); } } catch {} });
    ws.on("error", (e) => reject(new Error(`WS error: ${e.message}`)));
    setTimeout(() => reject(new Error("WS connection_ack timeout")), 5000);
  });
}
async function main() {
  console.log(`== LONG call ${LINKED} | agent ext ${CONNECTED_NUM} | customer ${NUMBER} | tenant ${TENANT} | ${LINES.length} turns / ${DURATION}s ==`);
  const ws = await openWs();
  await postEvent("event_call_started", "RINGING"); await sleep(600);
  await postEvent("event_call_connected", "UP"); await sleep(400);
  const gap = Math.max(200, Math.floor((DURATION * 1000) / LINES.length));
  let i = 0;
  for (const [speaker, text] of LINES) { i++; sendTranscript(ws, speaker, text, i); await sleep(gap); }
  await postEvent("event_call_finished", "DOWN"); await sleep(400);
  ws.close();
  console.log(`done. linkedId=${LINKED} (LONG, ${LINES.length} turns)`);
  process.exit(0);
}
main().catch((e) => { console.error(e.message); process.exit(1); });
