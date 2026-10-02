package com.snet.transcriptprocessing.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snet.transcriptprocessing.service.BicomWebhookService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Live-transcription WebSocket ingestion — the Java replacement for the Node
 * {@code ciap-ingestion-service} {@code WS /transcript} endpoint.
 *
 * <p>PBXware's Live Transcription connects as a client and streams frames. Only a
 * <b>completed</b> transcript
 * ({@code transcribed_data.type == "conversation.item.input_audio_transcription.completed"})
 * is forwarded to Kafka; deltas are ignored — matching the Node behaviour. Each
 * connection gets a UUID used as the Kafka correlation key (Node's {@code connectionId}).
 * The frame is published via {@link BicomWebhookService#publishTranscript} so it lands
 * on Kafka in the exact {@code TelephonyEvent} shape this repo's consumer expects.
 */
public class TranscriptIngestHandler extends TextWebSocketHandler {

    private static final Logger logger = LoggerFactory.getLogger(TranscriptIngestHandler.class);
    private static final String COMPLETED_TYPE =
            "conversation.item.input_audio_transcription.completed";

    private final BicomWebhookService webhookService;
    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    public TranscriptIngestHandler(BicomWebhookService webhookService, ObjectMapper objectMapper) {
        this.webhookService = webhookService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        String connectionId = UUID.randomUUID().toString();
        session.getAttributes().put("connectionId", connectionId);
        sessions.put(session.getId(), session);
        logger.info("Live Transcription client connected | connectionId={} remote={}",
                connectionId, session.getRemoteAddress());
        sendJson(session, Map.of(
                "type", "connection_ack",
                "status", "success",
                "timestamp", Instant.now().toString()
        ));
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        String connectionId = (String) session.getAttributes().get("connectionId");
        String text = message.getPayload();

        boolean completed = false;
        try {
            JsonNode root = objectMapper.readTree(text);
            JsonNode type = root.path("transcribed_data").path("type");
            completed = COMPLETED_TYPE.equals(type.asText(null));
        } catch (Exception ignore) {
            // non-JSON frame — treat as not-completed (dropped), same as Node
        }

        if (!completed) {
            logger.debug("Live Transcription delta — not forwarded | connectionId={}", connectionId);
            return;
        }

        try {
            webhookService.publishTranscript(connectionId, text);
            // Track how many completed transcripts this connection delivered, so an
            // abnormal close (below) can say whether anything actually flowed before it.
            Object forwarded = session.getAttributes().merge(
                    "forwardedCount", 1, (a, b) -> ((Integer) a) + (Integer) b);
            logger.info("Transcript completed — forwarded to Kafka | connectionId={} length={} totalOnConnection={}",
                    connectionId, text.length(), forwarded);
        } catch (Exception e) {
            logger.error("Failed to forward transcript | connectionId={}: {}", connectionId, e.getMessage(), e);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session.getId());
        Object connectionId = session.getAttributes().get("connectionId");
        Object forwarded = session.getAttributes().getOrDefault("forwardedCount", 0);
        // 1006 (NO_CLOSE_FRAME) is synthesized when the peer drops the socket without a
        // proper close handshake — a PBXware/proxy disconnect, not a clean shutdown. Flag it
        // at WARN with how many transcripts already flowed: transcripts forwarded before the
        // drop are safe, but any frames PBXware would have sent AFTER this point are lost.
        if (status.getCode() == CloseStatus.NO_CLOSE_FRAME.getCode()) {
            logger.warn("Live Transcription connection closed ABNORMALLY (no close frame) | "
                    + "connectionId={} status={} transcriptsForwarded={} — peer dropped the socket; "
                    + "earlier transcripts are unaffected, any later frames were lost",
                    connectionId, status, forwarded);
        } else {
            logger.info("Live Transcription connection closed | connectionId={} status={} transcriptsForwarded={}",
                    connectionId, status, forwarded);
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        // getMessage() is frequently null for transport drops (EOFException /
        // ClosedChannelException / connection reset), which tells you nothing. Log the
        // exception TYPE — that's what separates a benign client disconnect from a real
        // fault — and keep the full stack at debug for when it's needed.
        logger.warn("Live Transcription WebSocket transport error | connectionId={} error={}",
                session.getAttributes().get("connectionId"),
                exception == null ? "null" : exception.toString());
        logger.debug("Live Transcription WebSocket transport error (stack) | connectionId={}",
                session.getAttributes().get("connectionId"), exception);
    }

    /** Keep idle sockets alive through proxies (Node pinged every 25s). */
    @Scheduled(fixedRateString = "${app.ws.ping-interval-ms:25000}")
    public void pingClients() {
        for (WebSocketSession session : sessions.values()) {
            if (!session.isOpen()) {
                sessions.remove(session.getId());
                continue;
            }
            try {
                synchronized (session) {
                    session.sendMessage(new PingMessage());
                }
            } catch (Exception e) {
                logger.debug("Ping failed | sessionId={}: {}", session.getId(), e.getMessage());
            }
        }
    }

    private void sendJson(WebSocketSession session, Object payload) throws Exception {
        if (!session.isOpen()) return;
        synchronized (session) {
            session.sendMessage(new TextMessage(objectMapper.writeValueAsString(payload)));
        }
    }
}
