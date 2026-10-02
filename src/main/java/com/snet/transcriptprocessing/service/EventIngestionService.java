package com.snet.transcriptprocessing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snet.transcriptprocessing.dto.IncomingEventRequest;
import com.snet.transcriptprocessing.model.TelephonyEvent;
import com.snet.transcriptprocessing.producer.EventKafkaProducer;
import com.snet.transcriptprocessing.repository.TelephonyEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class EventIngestionService {

    private static final Logger logger = LoggerFactory.getLogger(EventIngestionService.class);

    private final TelephonyEventRepository repository;
    private final EventKafkaProducer producer;
    private final ObjectMapper objectMapper;

    public EventIngestionService(
            TelephonyEventRepository repository,
            EventKafkaProducer producer,
            ObjectMapper objectMapper) {
        this.repository = repository;
        this.producer = producer;
        this.objectMapper = objectMapper;
    }

    public TelephonyEvent ingest(IncomingEventRequest request) throws Exception {
        TelephonyEvent event = new TelephonyEvent();
        event.setModule(request.getModule());
        event.setPayload(objectMapper.writeValueAsString(request.getPayload()));
        event.setEndpoint(resolveEndpoint(request.getModule()));
        event.setStatus("PENDING");

        // Carry the caller's tenant id straight through as the source id.
        // (Tenant→source directory resolution now lives in ciap-api, not here.)
        event.setSourceId(request.getTenantId() != null ? request.getTenantId() : 0L);

        TelephonyEvent saved = repository.save(event);
        logger.info("Persisted TelephonyEvent | id={} module={} sourceId={}", saved.getId(), saved.getModule(), saved.getSourceId());

        producer.publish(String.valueOf(saved.getId()), objectMapper.writeValueAsString(saved));
        logger.info("Published TelephonyEvent to Kafka | id={}", saved.getId());

        return saved;
    }

    private String resolveEndpoint(String module) {
        return switch (module.toLowerCase()) {
            case "transcript" -> "/api/transcripts";
            case "event"      -> "/api/call-events";
            default -> throw new IllegalArgumentException("Unknown module type: " + module);
        };
    }
}
