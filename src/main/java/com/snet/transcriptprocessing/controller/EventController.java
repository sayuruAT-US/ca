package com.snet.transcriptprocessing.controller;

import com.snet.transcriptprocessing.dto.IncomingEventRequest;
import com.snet.transcriptprocessing.model.TelephonyEvent;
import com.snet.transcriptprocessing.service.EventIngestionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/events")
@ConditionalOnProperty(name = "app.producer.enabled", havingValue = "true", matchIfMissing = true)
public class EventController {

    private static final Logger logger = LoggerFactory.getLogger(EventController.class);

    private final EventIngestionService ingestionService;

    public EventController(EventIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> publishEvent(@RequestBody IncomingEventRequest request) {
        logger.info("Received event at POST /api/events | module={}", request.getModule());

        if (request.getModule() == null || request.getModule().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "ERROR",
                    "message", "Field 'module' is required. Allowed values: transcript, event"
            ));
        }

        if (request.getPayload() == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "ERROR",
                    "message", "Field 'payload' is required"
            ));
        }

        try {
            TelephonyEvent saved = ingestionService.ingest(request);
            return ResponseEntity.ok(Map.of(
                    "status", "SUCCESS",
                    "module", saved.getModule(),
                    "message", "Event ingested and published to Kafka"
            ));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "ERROR",
                    "message", e.getMessage()
            ));
        } catch (Exception e) {
            logger.error("Failed to process event: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "ERROR",
                    "message", "Failed to process event: " + e.getMessage()
            ));
        }
    }
}
