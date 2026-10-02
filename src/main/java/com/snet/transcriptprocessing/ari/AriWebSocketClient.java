package com.snet.transcriptprocessing.ari;

import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(name = "app.ari.enabled", havingValue = "true")
public class AriWebSocketClient extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(AriWebSocketClient.class);

    private final AriProperties properties;
    private final AriEventMapper mapper;
    private final AriEventPublisher publisher;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ari-ws-reconnect");
        t.setDaemon(true);
        return t;
    });

    private volatile WebSocketSession session;

    public AriWebSocketClient(AriProperties properties,
                              AriEventMapper mapper,
                              AriEventPublisher publisher) {
        this.properties = properties;
        this.mapper = mapper;
        this.publisher = publisher;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        running.set(true);
        connect();
    }

    private void connect() {
        if (!running.get()) {
            return;
        }
        String url = properties.buildEventsUrl();
        log.info("ARI connecting to {}", url.replaceAll("api_key=[^&]+", "api_key=***"));
        try {
            StandardWebSocketClient client = new StandardWebSocketClient();
            client.execute(this, url).whenComplete((sess, ex) -> {
                if (ex != null) {
                    log.error("ARI connection failed: {}", ex.getMessage());
                    scheduleReconnect();
                }
            });
        } catch (Exception e) {
            log.error("ARI connect error: {}", e.getMessage(), e);
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (!running.get()) {
            return;
        }
        long delay = properties.getReconnectDelayMs();
        log.info("ARI scheduling reconnect in {} ms", delay);
        scheduler.schedule(this::connect, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        this.session = session;
        log.info("ARI WebSocket connected | sessionId={}", session.getId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        String raw = message.getPayload();
        log.debug("ARI raw event: {}", raw);
        ObjectNode payload = mapper.toEventPayload(raw);
        if (payload != null) {
            publisher.publish(payload);
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.error("ARI transport error: {}", exception.getMessage());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        log.warn("ARI WebSocket closed | status={}", status);
        this.session = null;
        scheduleReconnect();
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        scheduler.shutdownNow();
        if (session != null && session.isOpen()) {
            try {
                session.close();
            } catch (Exception ignored) {
            }
        }
        log.info("ARI client stopped");
    }
}