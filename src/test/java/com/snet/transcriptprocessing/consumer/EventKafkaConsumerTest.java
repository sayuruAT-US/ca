package com.snet.transcriptprocessing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.snet.transcriptprocessing.model.TelephonyEvent;
import com.snet.transcriptprocessing.repository.TelephonyEventRepository;
import com.snet.transcriptprocessing.service.CiapRestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventKafkaConsumerTest {

    @Mock
    private CiapRestClient ciapRestClient;

    @Mock
    private TelephonyEventRepository repository;

    private ObjectMapper objectMapper;
    private EventKafkaConsumer consumer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        consumer = new EventKafkaConsumer(ciapRestClient, repository, objectMapper);
    }

    @Test
    void testConsume_TranscriptEvent_Success() throws Exception {
        TelephonyEvent event = new TelephonyEvent();
        event.setId(1L);
        event.setModule("transcript");
        event.setPayload("{\"delta\": \"Hello\"}");
        event.setEndpoint("/api/transcripts");
        event.setStatus("PENDING");

        String message = objectMapper.writeValueAsString(event);

        when(ciapRestClient.send(anyString(), eq("/api/transcripts"))).thenReturn(true);
        when(repository.save(any(TelephonyEvent.class))).thenReturn(event);

        consumer.consume(message);

        verify(ciapRestClient).send("{\"delta\": \"Hello\"}", "/api/transcripts");
        verify(repository).save(argThat(e -> "SENT".equals(e.getStatus())));
    }

    @Test
    void testConsume_CallEvent_CiapFails_StatusSetToFailed() throws Exception {
        TelephonyEvent event = new TelephonyEvent();
        event.setId(2L);
        event.setModule("event");
        event.setPayload("{\"event\": \"call_started\"}");
        event.setEndpoint("/api/call-events");
        event.setStatus("PENDING");

        String message = objectMapper.writeValueAsString(event);

        when(ciapRestClient.send(anyString(), eq("/api/call-events"))).thenReturn(false);
        when(repository.save(any(TelephonyEvent.class))).thenReturn(event);

        consumer.consume(message);

        verify(ciapRestClient).send("{\"event\": \"call_started\"}", "/api/call-events");
        verify(repository).save(argThat(e -> "FAILED".equals(e.getStatus())));
    }

    @Test
    void testConsume_EmptyMessage_Skipped() {
        consumer.consume(null);
        consumer.consume("  ");

        verifyNoInteractions(ciapRestClient);
        verifyNoInteractions(repository);
    }
}
