package com.snet.transcriptprocessing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snet.transcriptprocessing.model.TelephonyEvent;
import com.snet.transcriptprocessing.repository.TelephonyEventRepository;
import com.snet.transcriptprocessing.service.CiapRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "app.consumer.enabled", havingValue = "true", matchIfMissing = true)
public class EventKafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(EventKafkaConsumer.class);

    private final CiapRestClient ciapRestClient;
    private final TelephonyEventRepository repository;
    private final ObjectMapper objectMapper;

    public EventKafkaConsumer(
            CiapRestClient ciapRestClient,
            TelephonyEventRepository repository,
            ObjectMapper objectMapper) {
        this.ciapRestClient = ciapRestClient;
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${app.kafka.topic:telephony.events}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "transcriptKafkaListenerContainerFactory"
    )
    public void consume(String messagePayload) {
        if (messagePayload == null || messagePayload.isBlank()) {
            logger.warn("Received empty message from Kafka, skipping");
            return;
        }
        logger.info("Received event from Kafka | length={}", messagePayload.length());

        try {
            TelephonyEvent event = objectMapper.readValue(messagePayload, TelephonyEvent.class);
            String payload = event.getPayload();
            String endpoint = event.getEndpoint();

            boolean success = ciapRestClient.send(payload, endpoint);

            if (success) {
                event.setStatus("SENT");
                logger.info("Successfully forwarded event to CIAP | id={} endpoint={}", event.getId(), endpoint);
            } else {
                event.setStatus("FAILED");
                logger.error("Failed to forward event to CIAP | id={} endpoint={}", event.getId(), endpoint);
            }

            repository.save(event);

        } catch (Exception e) {
            logger.error("Error processing Kafka message: {}", e.getMessage(), e);
        }
    }
}
