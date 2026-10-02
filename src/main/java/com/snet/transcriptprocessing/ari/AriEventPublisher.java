package com.snet.transcriptprocessing.ari;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.snet.transcriptprocessing.config.EventsSourceProperties;
import com.snet.transcriptprocessing.dto.IncomingEventRequest;
import com.snet.transcriptprocessing.service.EventIngestionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class AriEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(AriEventPublisher.class);

    private final EventIngestionService ingestionService;
    private final EventsSourceProperties eventsSourceProperties;
    private final AriProperties ariProperties;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public AriEventPublisher(EventIngestionService ingestionService,
                             EventsSourceProperties eventsSourceProperties,
                             AriProperties ariProperties,
                             KafkaTemplate<String, String> kafkaTemplate,
                             ObjectMapper objectMapper) {
        this.ingestionService = ingestionService;
        this.eventsSourceProperties = eventsSourceProperties;
        this.ariProperties = ariProperties;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publish(ObjectNode payload) {
        if (payload == null) {
            return;
        }

        String callId = payload.path("linked_id").asText("unknown");

        if (eventsSourceProperties.acceptAri()) {
            try {
                IncomingEventRequest request = new IncomingEventRequest();
                request.setModule("event");
                request.setPayload(payload);
                ingestionService.ingest(request);
                log.info("ARI event ingested to telephony path | linked_id={} event_type={}",
                        callId, payload.path("event_type").asText());
            } catch (Exception e) {
                log.error("ARI event ingest failed | linked_id={} error={}", callId, e.getMessage(), e);
            }
        } else {
            log.debug("ARI event not forwarded to telephony | activeSource={}",
                    eventsSourceProperties.getSource());
        }

        if (ariProperties.isDebugTopicEnabled()) {
            try {
                String json = objectMapper.writeValueAsString(payload);
                kafkaTemplate.send(ariProperties.getDebugTopic(), callId, json);
            } catch (Exception e) {
                log.warn("ARI debug topic publish failed: {}", e.getMessage());
            }
        }
    }
}