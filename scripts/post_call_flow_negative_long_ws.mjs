// Kafka-path call-flow test — LONG FULL-NEGATIVE call (~50 turns, angry, escalating, unresolved).
//   events -> ciap-kafka HTTP /api/webhooks/bicom/events ; transcript -> ciap-kafka WS /transcript
//
// Run: NUMBER=+13120000041 CONNECTED_NUM=100 CONNECTED_NAME="Admin One" TENANT=200 DURATION=120 \
//   KAFKA_HTTP=http://localhost:8081 KAFKA_WS=ws://localhost:8081/transcript \
//   node scripts/post_call_flow_negative_long_ws.mjs
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
const NUMBER = process.env.NUMBER || "+13120000041";
const DID = process.env.DID || "+17733212627";
const CONNECTED_NUM = process.env.CONNECTED_NUM || "100";
const CONNECTED_NAME = process.env.CONNECTED_NAME || "Admin One";
const DURATION = Number(process.env.DURATION || 120);

const LINKED = `neglong-${Date.now()}`;
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
  ["caller", "I am absolutely furious. I have been charged twice and nobody warned me."],
  ["callee", "I'm very sorry to hear that. Let me look into the duplicate charge right away."],
  ["caller", "Sorry doesn't fix anything. This is completely unacceptable."],
  ["callee", "I understand your frustration. Can I confirm the account number?"],
  ["caller", "You should already have it. This is ridiculous."],
  ["callee", "I do have it here. I can see two charges on the same day."],
  ["caller", "Of course you can. Your billing system is a total disaster."],
  ["callee", "I apologize. I can start a refund for the duplicate charge."],
  ["caller", "A refund request? I want my money back immediately, not a request."],
  ["callee", "I hear you. Refunds typically take three to five business days."],
  ["caller", "Days? That is outrageous. I needed that money this week."],
  ["callee", "I completely understand how frustrating that is."],
  ["caller", "No, you don't. You people never understand anything."],
  ["callee", "I'm truly sorry. Let me see if I can expedite it."],
  ["caller", "You should have expedited it the moment it happened."],
  ["callee", "I'll flag it as urgent on my end right now."],
  ["caller", "I've heard that before and nothing ever happens."],
  ["callee", "I understand your skepticism given the experience you've had."],
  ["caller", "This is the third time this year your billing has failed."],
  ["callee", "That's not the standard we aim for, and I apologize."],
  ["caller", "Apologies are worthless at this point. I am an angry customer."],
  ["callee", "I want to make this right. What would help most?"],
  ["caller", "What would help is your company not being incompetent."],
  ["callee", "I'll take that feedback seriously and note it on the account."],
  ["caller", "Noting it does nothing. I want to escalate this to a manager."],
  ["callee", "I can escalate this to a supervisor for you."],
  ["caller", "Then do it. I'm done talking to the front line."],
  ["callee", "A supervisor will be notified, though there may be a short wait."],
  ["caller", "A wait? Naturally. Everything with you people is a wait."],
  ["callee", "I understand. While we wait, may I add a credit for the trouble?"],
  ["caller", "I don't want a credit. I want to cancel subscription entirely."],
  ["callee", "I'd hate to see you go after three years with us."],
  ["caller", "You should have thought of that before charging me twice."],
  ["callee", "That's fair. Let me pull up the cancellation options."],
  ["caller", "Finally, some action instead of empty words."],
  ["callee", "There is an early termination consideration on your plan."],
  ["caller", "Of course there is. You people nickel and dime everything."],
  ["callee", "I can request a waiver of that fee given the circumstances."],
  ["caller", "You can request. Again with the requests. I'm sick of it."],
  ["callee", "I understand. I'll submit the waiver request now."],
  ["caller", "This has been the worst support experience of my life."],
  ["callee", "I'm genuinely sorry it's come to this."],
  ["caller", "Sorry is all I ever hear and nothing changes."],
  ["callee", "I'll make sure the supervisor has the full history."],
  ["caller", "They'd better call me back today, not next week."],
  ["callee", "I've marked it as a priority callback for today."],
  ["caller", "We'll see. I have zero confidence in that."],
  ["callee", "I understand. Is there a best number to reach you?"],
  ["caller", "The same number you double charged. Figure it out."],
  ["caller", "Just escalate it and have someone call me. I've had enough."],
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
  console.log(`== LONG NEGATIVE call ${LINKED} | agent ext ${CONNECTED_NUM} | customer ${NUMBER} | tenant ${TENANT} | ${LINES.length} turns / ${DURATION}s ==`);
  const ws = await openWs();
  await postEvent("event_call_started", "RINGING"); await sleep(600);
  await postEvent("event_call_connected", "UP"); await sleep(400);
  const gap = Math.max(200, Math.floor((DURATION * 1000) / LINES.length));
  let i = 0;
  for (const [speaker, text] of LINES) { i++; sendTranscript(ws, speaker, text, i); await sleep(gap); }
  await postEvent("event_call_finished", "DOWN"); await sleep(400);
  ws.close();
  console.log(`done. linkedId=${LINKED} (LONG NEGATIVE, ${LINES.length} turns)`);
  process.exit(0);
}
main().catch((e) => { console.error(e.message); process.exit(1); });
