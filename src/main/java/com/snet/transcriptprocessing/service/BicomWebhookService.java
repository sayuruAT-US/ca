package com.snet.transcriptprocessing.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snet.transcriptprocessing.config.EventsSourceProperties;
import com.snet.transcriptprocessing.dto.IncomingEventRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class BicomWebhookService {

    private static final Logger logger = LoggerFactory.getLogger(BicomWebhookService.class);

    private final EventIngestionService ingestionService;
    private final ObjectMapper objectMapper;
    private final EventsSourceProperties eventsSourceProperties;

    public BicomWebhookService(EventIngestionService ingestionService,
                               ObjectMapper objectMapper,
                               EventsSourceProperties eventsSourceProperties) {
        this.ingestionService = ingestionService;
        this.objectMapper = objectMapper;
        this.eventsSourceProperties = eventsSourceProperties;
    }

    public void publishEvent(String rawBody) throws Exception {
        if (!eventsSourceProperties.acceptPbxware()) {
            logger.info("PBXware event ignored | activeSource={}", eventsSourceProperties.getSource());
            return;
        }
        logger.info("PBXware webhook event received | length={}", rawBody == null ? 0 : rawBody.length());
        ingestionService.ingest(toRequest("event", rawBody));
    }

    public void publishTranscript(String connectionKey, String rawBody) throws Exception {
        logger.info("PBXware transcript received | key={} length={}", connectionKey,
                rawBody == null ? 0 : rawBody.length());
        ingestionService.ingest(toRequest("transcript", rawBody));
    }

    private IncomingEventRequest toRequest(String module, String rawBody) {
        IncomingEventRequest req = new IncomingEventRequest();
        req.setModule(module);
        JsonNode payload;
        try {
            payload = (rawBody == null || rawBody.isBlank())
                    ? objectMapper.createObjectNode()
                    : objectMapper.readTree(rawBody);
        } catch (Exception notJson) {
            payload = objectMapper.getNodeFactory().textNode(rawBody);
        }
        req.setPayload(payload);
        return req;
    }
}