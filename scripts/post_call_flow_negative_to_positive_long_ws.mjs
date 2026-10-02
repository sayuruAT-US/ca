// Kafka-path call-flow test — LONG NEGATIVE -> POSITIVE call (~50 turns, starts angry,
// resolved, ends grateful).
//   events -> ciap-kafka HTTP /api/webhooks/bicom/events ; transcript -> ciap-kafka WS /transcript
//
// Run: NUMBER=+13120000042 CONNECTED_NUM=100 CONNECTED_NAME="Admin One" TENANT=200 DURATION=120 \
//   KAFKA_HTTP=http://localhost:8081 KAFKA_WS=ws://localhost:8081/transcript \
//   node scripts/post_call_flow_negative_to_positive_long_ws.mjs
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
const NUMBER = process.env.NUMBER || "+13120000042";
const DID = process.env.DID || "+17733212627";
const CONNECTED_NUM = process.env.CONNECTED_NUM || "100";
const CONNECTED_NAME = process.env.CONNECTED_NAME || "Admin One";
const DURATION = Number(process.env.DURATION || 120);

const LINKED = `n2plong-${Date.now()}`;
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
  ["caller", "I am really upset. I was charged twice this month and it is unacceptable."],
  ["callee", "I'm so sorry about that. Let me pull up your account and check right away."],
  ["caller", "Please do. I've been frustrated all morning trying to reach a human."],
  ["callee", "I apologize for the wait. I can see your account now."],
  ["caller", "Good. So what happened with the double charge?"],
  ["callee", "I see two identical charges on the first. One was applied by mistake."],
  ["caller", "So it's your error. That's honestly frustrating to hear."],
  ["callee", "You're absolutely right, and I take responsibility for that."],
  ["caller", "At least someone admits it. That's more than I expected."],
  ["callee", "I'm reversing the duplicate charge as we speak."],
  ["caller", "Okay. How long until the money is actually back?"],
  ["callee", "Normally three to five days, but I'm expediting it to twenty-four hours."],
  ["caller", "Twenty-four hours? That would genuinely help."],
  ["callee", "I've submitted the expedite request and it's approved."],
  ["caller", "Alright, that's a relief. I was really worried about rent."],
  ["callee", "I completely understand. Money matters like this are stressful."],
  ["caller", "It is. Thank you for actually taking it seriously."],
  ["callee", "Of course. I've also added a twenty dollar credit for the inconvenience."],
  ["caller", "Oh, you didn't have to do that. That's kind of you."],
  ["callee", "It's the least we can do after the trouble this caused."],
  ["caller", "I appreciate it. I was ready to cancel, honestly."],
  ["callee", "I'm really glad we could turn this around for you."],
  ["caller", "Me too. Can you confirm my next bill will be correct?"],
  ["callee", "Yes. Your next invoice will show only your normal monthly rate."],
  ["caller", "And the credit will show there as well?"],
  ["callee", "Correct, the twenty dollar credit will appear on that invoice."],
  ["caller", "That's perfect. You've been very patient with me."],
  ["callee", "You were understandably upset. I'd have felt the same."],
  ["caller", "Thanks for saying that. It's been a rough morning."],
  ["callee", "While I have you, is your autopay still on the card ending 6411?"],
  ["caller", "Yes, that's the one. Please keep it active."],
  ["callee", "Done. Autopay stays active and nothing else changes."],
  ["caller", "Good. Actually, could you email me a summary of all this?"],
  ["callee", "Absolutely. I'll send a full summary to your email on file now."],
  ["caller", "That would give me real peace of mind."],
  ["callee", "It's on its way. You should see it within a couple of minutes."],
  ["caller", "Got it, I just saw it come through. That was fast."],
  ["callee", "Wonderful. Does everything in the summary look correct?"],
  ["caller", "It does. Refund, credit, and the normal rate. All there."],
  ["caller", "Good, this turned out so much better than I expected."],
  ["callee", "I'm thrilled to hear that. Is there anything else at all?"],
  ["caller", "No, that's everything. You've been fantastic."],
  ["callee", "Thank you, that really means a lot to me."],
  ["caller", "I mean it. This is how support should always be."],
  ["callee", "I'll pass your kind words along to my team."],
  ["caller", "Please do. And thank you for fixing it so quickly."],
  ["callee", "My absolute pleasure. I'm glad we got it fully sorted."],
  ["caller", "You too. Have a great rest of your day."],
  ["callee", "You as well. Take care and thank you for your patience."],
  ["caller", "Thank you so much, this was truly excellent service."],
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
  console.log(`== LONG NEGATIVE->POSITIVE call ${LINKED} | agent ext ${CONNECTED_NUM} | customer ${NUMBER} | tenant ${TENANT} | ${LINES.length} turns / ${DURATION}s ==`);
  const ws = await openWs();
  await postEvent("event_call_started", "RINGING"); await sleep(600);
  await postEvent("event_call_connected", "UP"); await sleep(400);
  const gap = Math.max(200, Math.floor((DURATION * 1000) / LINES.length));
  let i = 0;
  for (const [speaker, text] of LINES) { i++; sendTranscript(ws, speaker, text, i); await sleep(gap); }
  await postEvent("event_call_finished", "DOWN"); await sleep(400);
  ws.close();
  console.log(`done. linkedId=${LINKED} (LONG NEGATIVE->POSITIVE, ${LINES.length} turns)`);
  process.exit(0);
}
main().catch((e) => { console.error(e.message); process.exit(1); });
