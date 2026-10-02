// Verifies per-user realtime routing + threshold alerts end-to-end against a
// running ciap-api (:8080) + ciap-kafka webhook (:8081).
//
// Tokens: set one JWT per test user via env (TOKEN_<userId>), e.g.
//   TOKEN_1531=... TOKEN_2=... TOKEN_3=... TOKEN_1530=... TOKEN_1529=... TOKEN_564=...
// Obtain them from real logins (POST /api/auth/login) or a temporary dev token
// minter. If a TOKEN_<id> env is absent the script falls back to GET
// /api/dev/token?userId=<id> (only present when a dev token endpoint is enabled).
// Run: node scripts/verify_realtime_routing.mjs   (resolves `ws` from ciap-web)
// Resolve `ws` from ciap-web (override with WS_MODULE if it lives elsewhere).
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

const API = process.env.API || "http://localhost:8080";
const WS = process.env.WS || "ws://localhost:8080/ws/transcript";
const KAFKA = process.env.KAFKA_WEBHOOK || "http://localhost:8081";
const TENANT = 200;
const NUMBER = "+12813308004";        // customer
const AGENT_EXT = "101";              // owning agent extension (Greg Iphone / Tenant 200 Agent)
const AGENT_NAME = "Greg Iphone";

// userId -> label. From the live DB (tenant 4 / code 200).
const USERS = {
  1531: "agentOwner",     // ext 101, SELF  — owns the call
  2:    "agentOwnerDup",  // ext 101, SELF  — same extension, also owns
  3:    "agentOther",     // ext 102, SELF  — unrelated agent (should get nothing)
  1530: "supervisor",     // ext 301, TEAM  — admins dept 1 (contains ext 101)
  1529: "admin",          // ext 100, ALL   — covers everything
  564:  "otherTenant",    // tenant 3 admin — cross-tenant (should get nothing)
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function getToken(userId) {
  const envToken = process.env[`TOKEN_${userId}`];
  if (envToken) return { token: envToken, userId };
  const res = await fetch(`${API}/api/dev/token?userId=${userId}`);
  if (!res.ok) throw new Error(`no TOKEN_${userId} env and /api/dev/token -> ${res.status}`);
  return res.json();
}

function connect(label, tokenInfo) {
  return new Promise((resolve) => {
    const ws = new WebSocket(WS);
    const client = { label, ws, tokenInfo, msgs: [], authed: false };
    ws.on("message", (buf) => {
      let m; try { m = JSON.parse(buf.toString()); } catch { return; }
      client.msgs.push(m);
      if (m.type === "AUTHED") client.authed = true;
    });
    ws.on("open", () => {
      ws.send(JSON.stringify({ type: "AUTH", token: tokenInfo.token }));
      resolve(client);
    });
    ws.on("error", () => resolve(client));
  });
}

const types = (c) => c.msgs.map((m) => m.type);
const has = (c, t) => c.msgs.some((m) => m.type === t);
const countOf = (c, t) => c.msgs.filter((m) => m.type === t).length;

async function postEvent(linked, event, state) {
  await fetch(`${KAFKA}/api/webhooks/bicom/events`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      event, event_id: `${linked}-${event}`, tenant_code: TENANT,
      payload: { linked_id: linked, uid: `${linked}.1`, call_type: "inbound", state,
        number: NUMBER, did: "+17733212627", connected_num: AGENT_EXT, connected_name: AGENT_NAME,
        is_incoming: true, record_status: "recording" },
    }),
  });
}

async function postTranscript(linked, speaker, text, confidence) {
  const td = { transcript: text, speaker, item_id: `${linked}-${Math.random()}`, event_id: `${linked}-t-${Math.random()}`, type: "final" };
  if (confidence != null) td.confidence = confidence;
  await fetch(`${KAFKA}/api/webhooks/bicom/transcript`, {
    method: "POST", headers: { "Content-Type": "application/json", "x-connection-id": linked },
    body: JSON.stringify({ transcribed_data: td,
      metadata: { linked_id: linked, caller_id: NUMBER, callee_name: AGENT_NAME, channel_name: `agent-${AGENT_EXT}` } }),
  });
}

const results = [];
function check(name, cond, detail = "") {
  results.push({ name, pass: !!cond, detail });
  console.log(`  ${cond ? "PASS" : "FAIL"} — ${name}${detail ? "  (" + detail + ")" : ""}`);
}

async function main() {
  // 1. Connect + authenticate all clients.
  const tokens = {};
  for (const id of Object.keys(USERS)) tokens[id] = await getToken(id);
  const clients = {};
  for (const [id, label] of Object.entries(USERS)) clients[label] = await connect(label, tokens[id]);
  await sleep(500);
  console.log("\n== auth ==");
  for (const c of Object.values(clients)) check(`${c.label} authenticated`, c.authed);

  // Supervisor + admin opt in to alerts (agents do not).
  clients.supervisor.ws.send(JSON.stringify({ type: "SUBSCRIBE_ALERTS" }));
  clients.admin.ws.send(JSON.stringify({ type: "SUBSCRIBE_ALERTS" }));
  clients.otherTenant.ws.send(JSON.stringify({ type: "SUBSCRIBE_ALERTS" }));
  await sleep(300);

  const linked = `verify-${Date.now()}`;

  // 2. Call starts → only owning agents get CALL_EVENT (auto-open).
  console.log("\n== auto-open routing (event_call_started) ==");
  await postEvent(linked, "event_call_started", "RINGING");
  await sleep(700);
  check("owning agent (1531) got CALL_EVENT", has(clients.agentOwner, "CALL_EVENT"));
  check("duplicate-ext agent (2) got CALL_EVENT", has(clients.agentOwnerDup, "CALL_EVENT"));
  check("unrelated agent (3) got NO CALL_EVENT", !has(clients.agentOther, "CALL_EVENT"), types(clients.agentOther).join(","));
  check("supervisor got NO auto-open CALL_EVENT", !has(clients.supervisor, "CALL_EVENT"));
  check("admin got NO auto-open CALL_EVENT", !has(clients.admin, "CALL_EVENT"));
  check("cross-tenant user got NO CALL_EVENT", !has(clients.otherTenant, "CALL_EVENT"));

  await postEvent(linked, "event_call_connected", "UP");
  await sleep(400);

  // 3. Per-call subscribe authorization.
  console.log("\n== per-call subscribe authorization ==");
  clients.agentOwner.ws.send(JSON.stringify({ type: "SUBSCRIBE", linkedId: linked }));
  clients.supervisor.ws.send(JSON.stringify({ type: "SUBSCRIBE", linkedId: linked }));
  clients.agentOther.ws.send(JSON.stringify({ type: "SUBSCRIBE", linkedId: linked }));
  await sleep(500);
  check("owning agent SUBSCRIBED", has(clients.agentOwner, "SUBSCRIBED"));
  check("scoped supervisor SUBSCRIBED", has(clients.supervisor, "SUBSCRIBED"));
  check("unrelated agent SUBSCRIBE_DENIED", has(clients.agentOther, "SUBSCRIBE_DENIED"),
        (clients.agentOther.msgs.find(m=>m.type==="SUBSCRIBE_DENIED")||{}).reason || "");

  // 4. Live transcript flows only to subscribers of the call.
  console.log("\n== live transcript (per-call) ==");
  await postTranscript(linked, "callee", "Hello, how can I help you today?", 0.9);
  await sleep(500);
  check("owning agent got LIVE_TRANSCRIPT", has(clients.agentOwner, "LIVE_TRANSCRIPT"));
  check("scoped supervisor got LIVE_TRANSCRIPT", has(clients.supervisor, "LIVE_TRANSCRIPT"));
  check("unrelated agent got NO LIVE_TRANSCRIPT", !has(clients.agentOther, "LIVE_TRANSCRIPT"));
  check("cross-tenant got NO LIVE_TRANSCRIPT", !has(clients.otherTenant, "LIVE_TRANSCRIPT"));

  // 5. Threshold alert: a low-sentiment caller turn → scoped+subscribed supervisors/admins only.
  console.log("\n== threshold alert (low sentiment) ==");
  await postTranscript(linked, "caller", "This is absolutely terrible, I am furious and want a refund now.", 0.08);
  await sleep(800);
  check("supervisor got NOTIFICATION", has(clients.supervisor, "NOTIFICATION"),
        (clients.supervisor.msgs.find(m=>m.type==="NOTIFICATION")?.data?.kind) || "");
  check("admin got NOTIFICATION", has(clients.admin, "NOTIFICATION"));
  check("owning agent got NO NOTIFICATION (agents excluded)", !has(clients.agentOwner, "NOTIFICATION"));
  check("cross-tenant supervisor got NO NOTIFICATION", !has(clients.otherTenant, "NOTIFICATION"));
  check("alert fired once (not duplicated)", countOf(clients.supervisor, "NOTIFICATION") === 1,
        "count=" + countOf(clients.supervisor, "NOTIFICATION"));

  // 6. Finish.
  await postEvent(linked, "event_call_finished", "DOWN");
  await sleep(400);
  check("owning agent got CALL_FINISHED", has(clients.agentOwner, "CALL_EVENT") && has(clients.agentOwner, "CALL_FINISHED"));

  // Summary
  const passed = results.filter((r) => r.pass).length;
  console.log(`\n==== ${passed}/${results.length} checks passed ====`);
  for (const c of Object.values(clients)) c.ws.close();
  process.exit(passed === results.length ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(2); });
