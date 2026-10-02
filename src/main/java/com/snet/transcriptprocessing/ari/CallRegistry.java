package com.snet.transcriptprocessing.ari;

import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class CallRegistry {

    private final Map<String, CallSession> byCallId = new ConcurrentHashMap<>();

    public CallSession getOrCreate(String callId) {
        return byCallId.computeIfAbsent(callId, CallSession::new);
    }

    public Optional<CallSession> find(String callId) {
        if (callId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(byCallId.get(callId));
    }

    public Collection<CallSession> all() {
        return byCallId.values();
    }

    public void remove(String callId) {
        if (callId != null) {
            byCallId.remove(callId);
        }
    }
}