# Dev test scripts — call-event → Kafka → ciap-api → WebSocket

End-to-end smoke tests for the Bicom → Kafka → consumer → ciap-api → realtime WebSocket
pipeline (drives the frontend incoming-call popup + live transcript).

Prereqs for all: ciap-kafka (:8081), a Kafka broker, ciap-api (:8080), PostgreSQL — all
running. Node scripts resolve `ws` from `ciap-web/node_modules` (or Node 22+ built-in).

## Ingestion path each script exercises

| Script | Call events | Live transcript |
|---|---|---|
| **`post_call_flow_ws.mjs`** | HTTP `POST /api/webhooks/bicom/events` (**Kafka HTTP**) | **WebSocket `/transcript`** (**Kafka WS**) — the PBXware path |
| `post_call_flow.sh` | HTTP `POST /api/webhooks/bicom/events` | HTTP `POST /api/webhooks/bicom/transcript` |
| `verify_realtime_routing.mjs` | drives ciap-api directly with JWTs | asserts per-user routing / alerts |
| `ws_listen.js` | — | a WS **client** that logs ciap-api's `/ws/transcript` (the frontend feed) |

> Direct-to-ciap-api tests (bypassing Kafka) live in the **ciap-api** repo:
> `ciap-api/scripts/post_call_flow_direct.sh`.

## Recommended run — real ingestion paths (events HTTP + transcript WS)

```bash
# 1. (optional) watch what the frontend popup would receive:
node scripts/ws_listen.js ws://localhost:8080/ws/transcript 30 &

# 2. Fire a mock inbound call — events over HTTP, transcript over the Kafka WS:
NUMBER=+12813308004 CONNECTED_NUM=101 CONNECTED_NAME="Greg Iphone" TENANT=200 DURATION=60 \
  KAFKA_HTTP=http://localhost:8081 KAFKA_WS=ws://localhost:8081/transcript \
  node scripts/post_call_flow_ws.mjs
```

`CONNECTED_NUM` is the agent extension the popup auto-opens for — log in as the user who
owns that extension (e.g. `agent1@test-example.com` owns 101) to see the popup.

## Legacy HTTP-only run

```bash
KAFKA_WEBHOOK=http://localhost:8081 ./scripts/post_call_flow.sh
```

Expected either way: on the WS listener, `CALL_EVENT event_call_started` (opens the popup),
the transcript lines, then `event_call_finished`; and in the DB a `FINISHED/ANSWERED`
`app.call_records` row with the transcript rows for that `linked_id`.
