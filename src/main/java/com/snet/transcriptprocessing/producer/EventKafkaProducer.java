package com.snet.transcriptprocessing.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EventKafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(EventKafkaProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public EventKafkaProducer(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topic:telephony.events}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publish(String key, String payload) {
        logger.info("Publishing event to Kafka | topic={} key={}", topic, key);
        kafkaTemplate.send(topic, key, payload).whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("Published to topic={} partition={} offset={}",
                        topic, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                logger.error("Failed to publish to topic={}: {}", topic, ex.getMessage(), ex);
            }
        });
    }
}
