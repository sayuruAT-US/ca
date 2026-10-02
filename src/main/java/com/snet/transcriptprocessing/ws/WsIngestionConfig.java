package com.snet.transcriptprocessing.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snet.transcriptprocessing.service.BicomWebhookService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * Registers the live-transcription WebSocket ingestion endpoint. This whole role is
 * gated by {@code app.ws.enabled} (default true), so the same jar can be deployed as
 * a WS-ingestion node, or a producer/consumer node without it.
 *
 * <p>Path defaults to {@code /transcript} (matching the Node service) and can be
 * overridden with {@code app.ws.path}. Allowed origins default to {@code *} (PBXware
 * is a server-to-server client, not a browser) via {@code app.ws.allowed-origins}.
 */
@Configuration
@EnableWebSocket
@EnableScheduling
@ConditionalOnProperty(name = "app.ws.enabled", havingValue = "true", matchIfMissing = true)
public class WsIngestionConfig implements WebSocketConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(WsIngestionConfig.class);

    private final BicomWebhookService webhookService;
    private final ObjectMapper objectMapper;

    @Value("${app.ws.path:/transcript}")
    private String path;

    @Value("${app.ws.allowed-origins:*}")
    private String allowedOrigins;

    public WsIngestionConfig(BicomWebhookService webhookService, ObjectMapper objectMapper) {
        this.webhookService = webhookService;
        this.objectMapper = objectMapper;
    }

    @Bean
    public TranscriptIngestHandler transcriptIngestHandler() {
        return new TranscriptIngestHandler(webhookService, objectMapper);
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        String[] origins = (allowedOrigins == null || allowedOrigins.isBlank())
                ? new String[]{"*"}
                : java.util.Arrays.stream(allowedOrigins.split(","))
                        .map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
        registry.addHandler(transcriptIngestHandler(), path).setAllowedOrigins(origins);
        logger.info("Live Transcription WS ingestion enabled | path={} allowedOrigins={}",
                path, String.join(",", origins));
    }
}
