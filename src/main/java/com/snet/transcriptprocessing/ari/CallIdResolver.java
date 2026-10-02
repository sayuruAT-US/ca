package com.snet.transcriptprocessing.ari;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class CallIdResolver {

    private final Map<String, String> channelToCallId = new ConcurrentHashMap<>();

    public String resolve(String channelId, String linkedId, String uniqueId) {
        if (linkedId != null && !linkedId.isBlank()) {
            bind(channelId, linkedId);
            return linkedId;
        }
        if (channelId != null) {
            String existing = channelToCallId.get(channelId);
            if (existing != null) {
                return existing;
            }
        }
        String callId = (uniqueId != null && !uniqueId.isBlank())
                ? uniqueId
                : (channelId != null && !channelId.isBlank() ? channelId : "unknown");
        bind(channelId, callId);
        return callId;
    }

    public void bind(String channelId, String callId) {
        if (channelId != null && !channelId.isBlank() && callId != null && !callId.isBlank()) {
            channelToCallId.put(channelId, callId);
        }
    }

    public void linkChannels(String channelIdA, String channelIdB) {
        if (channelIdA == null || channelIdB == null) {
            return;
        }
        String a = channelToCallId.get(channelIdA);
        String b = channelToCallId.get(channelIdB);
        if (a != null && b == null) {
            channelToCallId.put(channelIdB, a);
        } else if (b != null && a == null) {
            channelToCallId.put(channelIdA, b);
        } else if (a != null && b != null && !a.equals(b)) {
            String winner = a.compareTo(b) <= 0 ? a : b;
            String loser = winner.equals(a) ? b : a;
            channelToCallId.replaceAll((ch, id) -> loser.equals(id) ? winner : id);
        }
    }

    public String getCallId(String channelId) {
        return channelId == null ? null : channelToCallId.get(channelId);
    }

    public void removeChannel(String channelId) {
        if (channelId != null) {
            channelToCallId.remove(channelId);
        }
    }
}