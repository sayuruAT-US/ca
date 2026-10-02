package com.snet.transcriptprocessing.controller;

import com.snet.transcriptprocessing.service.BicomWebhookService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

/**
 * Bicom PBXware webhook receiver — the Java equivalent of the Node app's
 * {@code POST /events} (and {@code /pbx/event}) Express handler.
 *
 * <p>It accepts the raw webhook body (any content type), applies the same light
 * {@code {module,payload}} wrapping the Node app does, and forwards it straight
 * to the Kafka producer. This deliberately bypasses the DB-persisting
 * {@code /api/events} path — it is the "post directly to the Kafka producer"
 * endpoint.
 */
@RestController
@RequestMapping("/api/webhooks/bicom")
@ConditionalOnProperty(name = "app.producer.enabled", havingValue = "true", matchIfMissing = true)
public class BicomWebhookController {

    private static final Logger logger = LoggerFactory.getLogger(BicomWebhookController.class);

    private final BicomWebhookService webhookService;

    /**
     * Optional shared secret, mirroring Node's EVENT_MANAGER_SHARED_SECRET /
     * {@code x-shared-secret} header check. When blank, auth is skipped (Node
     * default behaviour).
     */
    private final String sharedSecret;

    public BicomWebhookController(
            BicomWebhookService webhookService,
            @Value("${app.webhook.shared-secret:}") String sharedSecret) {
        this.webhookService = webhookService;
        this.sharedSecret = sharedSecret;
    }

    /** Primary Bicom webhook path. Mirrors Node {@code POST /events}. */
    @PostMapping(value = {"/events", "/pbx/event"})
    public ResponseEntity<Map<String, Object>> receiveEvent(
            @RequestBody(required = false) String body,
            @RequestHeader(value = "x-shared-secret", required = false) String providedSecret) {

        ResponseEntity<Map<String, Object>> unauthorized = checkSecret(providedSecret);
        if (unauthorized != null) {
            return unauthorized;
        }

        try {
            webhookService.publishEvent(body == null ? "" : body);
            return ResponseEntity.ok(Map.of("ok", true));
        } catch (Exception e) {
            logger.error("Failed to process Bicom webhook event: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("ok", false, "error", "failed to process event"));
        }
    }

    /**
     * Transcript forwarding path. In the Node app transcripts arrive over a
     * WebSocket and are keyed by a per-connection UUID; over HTTP the caller may
     * supply the correlation key via {@code x-connection-id}, otherwise a UUID is
     * generated to preserve the same keying semantics.
     */
    @PostMapping(value = "/transcript")
    public ResponseEntity<Map<String, Object>> receiveTranscript(
            @RequestBody(required = false) String body,
            @RequestHeader(value = "x-connection-id", required = false) String connectionId,
            @RequestHeader(value = "x-shared-secret", required = false) String providedSecret) {

        ResponseEntity<Map<String, Object>> unauthorized = checkSecret(providedSecret);
        if (unauthorized != null) {
            return unauthorized;
        }

        String key = (connectionId == null || connectionId.isBlank())
                ? UUID.randomUUID().toString()
                : connectionId;

        try {
            webhookService.publishTranscript(key, body == null ? "" : body);
            return ResponseEntity.ok(Map.of("ok", true, "key", key));
        } catch (Exception e) {
            logger.error("Failed to process Bicom transcript: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("ok", false, "error", "failed to process transcript"));
        }
    }

    private ResponseEntity<Map<String, Object>> checkSecret(String provided) {
        if (sharedSecret == null || sharedSecret.isBlank()) {
            return null; // no secret configured -> allow (matches Node default)
        }
        if (!sharedSecret.equals(provided)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(Map.of("ok", false, "error", "unauthorized"));
        }
        return null;
    }
}
