#!/usr/bin/env bash
# Mock Bicom PBXware call flow -> ciap-kafka webhook -> Kafka -> ciap-api -> WS broadcast.
# Runs a ~DURATION-second live conversation: started -> connected -> N transcript
# turns paced across DURATION -> finished.
#   KAFKA_WEBHOOK=http://host:8081  DURATION=60  ./post_call_flow.sh
#
# Override the call parties to use REAL directory data so names resolve:
#   NUMBER=+12813308004 CONNECTED_NUM=101 CONNECTED_NAME="Greg Iphone" \
#   TENANT=200 DURATION=75 ./post_call_flow.sh
# (customer name resolves from the contact matching NUMBER; agent from CONNECTED_NUM)
set -e
KAFKA=${KAFKA_WEBHOOK:-http://localhost:8081}
DURATION=${DURATION:-60}
TENANT=${TENANT:-200}
NUMBER=${NUMBER:-+13129576371}
DID=${DID:-+17733212627}
CONNECTED_NUM=${CONNECTED_NUM:-101}
CONNECTED_NAME=${CONNECTED_NAME:-Maria Gomez}
LINKED="call-$(date +%s)"; EVID="evt-$(date +%s)"
post_event () { # $1=event $2=state
  curl -s -o /dev/null -w "  $1 -> HTTP %{http_code}\n" -X POST "$KAFKA/api/webhooks/bicom/events" \
    -H "Content-Type: application/json" -d "{
      \"event\":\"$1\",\"event_id\":\"$EVID-$1\",\"tenant_code\":$TENANT,
      \"payload\":{ \"linked_id\":\"$LINKED\",\"uid\":\"$LINKED.1\",\"call_type\":\"inbound\",
        \"state\":\"$2\",\"number\":\"$NUMBER\",\"did\":\"$DID\",
        \"connected_num\":\"$CONNECTED_NUM\",\"connected_name\":\"$CONNECTED_NAME\",
        \"is_incoming\":true,\"record_status\":\"recording\" } }"
}
i=0
post_t () { # $1=speaker $2=text
  i=$((i+1))
  curl -s -o /dev/null -w "  [%{http_code}] $1: $2\n" -X POST "$KAFKA/api/webhooks/bicom/transcript" \
    -H "Content-Type: application/json" -H "x-connection-id: $LINKED" -d "{
      \"transcribed_data\":{\"transcript\":\"$2\",\"speaker\":\"$1\",\"item_id\":\"$LINKED-$i\",\"event_id\":\"$EVID-t$i\",\"type\":\"final\"},
      \"metadata\":{\"linked_id\":\"$LINKED\",\"caller_id\":\"$NUMBER\",\"callee_name\":\"$CONNECTED_NAME\",\"channel_name\":\"agent-$CONNECTED_NUM\"} }"
}
# A realistic ~18-turn support conversation (speaker|text).
LINES=(
"caller|Hi, I'm calling about my last invoice — it looks a lot higher than usual."
"callee|I can help with that. May I have the account number on the invoice, please?"
"caller|Sure, it's 4471-208."
"callee|Thank you. Let me pull that up… one moment."
"callee|I see the invoice dated the 1st. It's about forty dollars higher than last month."
"caller|Right, that's what I noticed. I didn't change my plan."
"callee|You're correct. There's a one-time setup fee that was applied by mistake."
"caller|Okay, that explains it. Can you remove it?"
"callee|Absolutely. I'm crediting the setup fee back to your account right now."
"caller|Great, thank you. How long until I see the credit?"
"callee|It'll reflect within one billing cycle, and you'll get an email confirmation today."
"caller|Perfect. Will my next invoice be back to the normal amount?"
"callee|Yes — your next invoice returns to your regular monthly rate."
"caller|That's a relief. While I have you — is my autopay still active?"
"callee|It is, on the card ending 6411. Nothing else needs changing."
"caller|Wonderful. That's all I needed today."
"callee|Happy to help. I've noted the credit on your account. Have a great day!"
"caller|You too, thanks for sorting it out so quickly."
)
n=${#LINES[@]}
gap=$(awk "BEGIN{printf \"%.2f\", $DURATION/$n}")
# Full Bicom call-leg lifecycle: started -> updated -> connected -> (mid) updated -> finished.
echo "== call started (opens popup) =="; post_event event_call_started RINGING; sleep 1
echo "== call updated (still ringing) =="; post_event event_call_updated RINGING; sleep 1
echo "== call connected =="; post_event event_call_connected UP; sleep 1
echo "== live transcript: $n turns over ~${DURATION}s (~${gap}s each) =="
mid=$((n/2))
for entry in "${LINES[@]}"; do
  post_t "${entry%%|*}" "${entry#*|}"
  # Fire a mid-call update roughly halfway through the conversation.
  if [ "$i" -eq "$mid" ]; then echo "== call updated (mid-call) =="; post_event event_call_updated UP; fi
  sleep "$gap"
done
echo "== call finished =="; post_event event_call_finished DOWN
echo "done. linkedId=$LINKED  (~${DURATION}s)"
