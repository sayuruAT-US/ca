package com.snet.transcriptprocessing.ari;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CallSession {

    private final String callId;
    private String callerNumber;
    private String callerName;
    private String did;
    private String agentExtension;
    private Instant ringTime;
    private Instant answerTime;
    private Instant endTime;
    private final Map<String, String> channelRoles = new ConcurrentHashMap<>();

    public CallSession(String callId) {
        this.callId = callId;
    }

    public String getCallId() { return callId; }

    public String getCallerNumber() { return callerNumber; }
    public void setCallerNumber(String callerNumber) {
        if (this.callerNumber == null && callerNumber != null && !callerNumber.isBlank()) {
            this.callerNumber = callerNumber;
        }
    }

    public String getCallerName() { return callerName; }
    public void setCallerName(String callerName) {
        if (this.callerName == null && callerName != null && !callerName.isBlank()) {
            this.callerName = callerName;
        }
    }

    public String getDid() { return did; }
    public void setDid(String did) {
        if (this.did == null && did != null && !did.isBlank() && !"s".equals(did)) {
            this.did = did;
        }
    }

    public String getAgentExtension() { return agentExtension; }
    public void setAgentExtension(String agentExtension) {
        if (agentExtension != null && !agentExtension.isBlank()) {
            this.agentExtension = agentExtension;
        }
    }

    public Instant getRingTime() { return ringTime; }
    public void setRingTime(Instant ringTime) {
        if (this.ringTime == null && ringTime != null) {
            this.ringTime = ringTime;
        }
    }

    public Instant getAnswerTime() { return answerTime; }
    public void setAnswerTime(Instant answerTime) {
        if (this.answerTime == null && answerTime != null) {
            this.answerTime = answerTime;
        }
    }

    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant endTime) {
        if (this.endTime == null && endTime != null) {
            this.endTime = endTime;
        }
    }

    public void putChannelRole(String channelId, String role) {
        if (channelId != null && role != null) {
            channelRoles.put(channelId, role);
        }
    }

    public String getChannelRole(String channelId) {
        return channelRoles.get(channelId);
    }
}