package com.snet.transcriptprocessing.ari;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.UUID;

@Component
public class AriEventMapper {

    private static final Logger log = LoggerFactory.getLogger(AriEventMapper.class);

    private static final DateTimeFormatter ARI_TS = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .optionalStart().appendOffset("+HHMM", "+0000").optionalEnd()
            .optionalStart().appendOffset("+HH:MM", "+00:00").optionalEnd()
            .toFormatter();

    private final ObjectMapper objectMapper;
    private final CallIdResolver callIdResolver;
    private final CallRegistry callRegistry;

    public AriEventMapper(ObjectMapper objectMapper,
                          CallIdResolver callIdResolver,
                          CallRegistry callRegistry) {
        this.objectMapper = objectMapper;
        this.callIdResolver = callIdResolver;
        this.callRegistry = callRegistry;
    }

    public ObjectNode toEventPayload(String rawJson) {
        try {
            JsonNode root = objectMapper.readTree(rawJson);
            String type = text(root, "type");
            if (type == null) {
                return null;
            }
            if (!type.startsWith("Channel") && !type.startsWith("Bridge")) {
                log.debug("Ignoring ARI event type={}", type);
                return null;
            }

            JsonNode channel = root.path("channel");
            String channelId = text(channel, "id");
            String linkedIdField = text(channel, "linkedid");
            String uniqueId = text(channel, "uniqueid");
            if (uniqueId == null || uniqueId.isBlank()) {
                uniqueId = channelId;
            }

            if ("BridgeEnter".equals(type) || "ChannelEnteredBridge".equals(type)) {
                callIdResolver.linkChannels(channelId, text(root.path("bridge"), "id"));
            }

            String callId = callIdResolver.resolve(channelId, linkedIdField, uniqueId);
            CallSession session = callRegistry.getOrCreate(callId);

            String channelName = text(channel, "name");
            String state = text(channel, "state");
            String callerNumber = text(channel.path("caller"), "number");
            String callerName = text(channel.path("caller"), "name");
            String connectedNumber = text(channel.path("connected"), "number");
            String did = text(channel.path("dialplan"), "exten");
            String ts = text(root, "timestamp");
            Instant instant = parseTs(ts);
            long epochSec = instant != null ? instant.getEpochSecond() : Instant.now().getEpochSecond();

            String role = classifyRole(channelName);
            if ("caller".equals(role) || isTrunk(channelName)) {
                session.setCallerNumber(callerNumber);
                session.setCallerName(callerName);
                session.setDid(did);
            }
            if ("agent".equals(role)) {
                String ext = extractExtension(channelName);
                if (ext != null) {
                    session.setAgentExtension(ext);
                }
            }

            String pbxEvent = mapToPbxwareEvent(type, state, role, session);
            if (pbxEvent == null) {
                log.debug("No PBXware lifecycle mapping for type={} state={} role={}", type, state, role);
                return null;
            }

            if ("event_call_started".equals(pbxEvent)) {
                session.setRingTime(instant != null ? instant : Instant.now());
            }
            if ("event_call_connected".equals(pbxEvent)) {
                session.setAnswerTime(instant != null ? instant : Instant.now());
            }
            if ("event_call_finished".equals(pbxEvent)) {
                session.setEndTime(instant != null ? instant : Instant.now());
            }

            String number = session.getCallerNumber() != null ? session.getCallerNumber() : callerNumber;
            String didVal = session.getDid() != null ? session.getDid() : did;
            String agentExt = session.getAgentExtension() != null ? session.getAgentExtension() : connectedNumber;

            ObjectNode inner = objectMapper.createObjectNode();
            inner.put("linked_id", callId);
            inner.put("uid", callId);
            inner.put("number", nullToEmpty(number));
            inner.put("did", nullToEmpty(didVal));
            inner.put("connected_num", nullToEmpty(agentExt));
            inner.put("connected_name", nullToEmpty(session.getCallerName()));
            inner.put("state", nullToEmpty(state));
            inner.put("is_incoming", true);
            inner.put("is_pbx_inbound_call", true);
            inner.put("call_type", "incoming");
            if (session.getRingTime() != null) {
                inner.put("call_started_datetime", session.getRingTime().toString());
                inner.put("call_started_timestamp", session.getRingTime().getEpochSecond());
            }

            ObjectNode timeInfo = objectMapper.createObjectNode();
            timeInfo.put("timestamp", epochSec);

            ObjectNode out = objectMapper.createObjectNode();
            out.put("event", pbxEvent);
            out.put("event_id", "ari-" + callId + "-" + pbxEvent + "-" + UUID.randomUUID());
            out.set("event_time_info", timeInfo);
            out.set("payload", inner);
            out.put("source", "ari");
            return out;
        } catch (Exception e) {
            log.warn("Failed to map ARI event: {}", e.getMessage());
            return null;
        }
    }

    private static String mapToPbxwareEvent(String type, String state, String role, CallSession session) {
        if ("ChannelStateChange".equals(type) || "ChannelCreated".equals(type)) {
            if (("Ring".equalsIgnoreCase(state) || "Ringing".equalsIgnoreCase(state))
                    && session.getRingTime() == null) {
                return "event_call_started";
            }
            if ("Up".equalsIgnoreCase(state) && "agent".equals(role) && session.getAnswerTime() == null) {
                return "event_call_connected";
            }
        }
        if ("ChannelDestroyed".equals(type) && session.getEndTime() == null) {
            if (session.getRingTime() != null || session.getAnswerTime() != null
                    || session.getCallerNumber() != null) {
                return "event_call_finished";
            }
        }
        return null;
    }

    private static String classifyRole(String channelName) {
        if (channelName == null) {
            return null;
        }
        String n = channelName.toUpperCase();
        if (n.startsWith("PJSIP/") && n.matches("PJSIP/\\d+-.*")) {
            return "agent";
        }
        if (isTrunk(channelName)) {
            return "caller";
        }
        return "caller";
    }

    private static boolean isTrunk(String channelName) {
        if (channelName == null) {
            return false;
        }
        String n = channelName.toUpperCase();
        return n.contains("SBC") || n.contains("TRUNK") || n.contains("-IN-");
    }

    private static String extractExtension(String channelName) {
        try {
            if (channelName == null || !channelName.startsWith("PJSIP/")) {
                return null;
            }
            String rest = channelName.substring("PJSIP/".length());
            int dash = rest.indexOf('-');
            String ext = dash > 0 ? rest.substring(0, dash) : rest;
            return ext.matches("\\d+") ? ext : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static Instant parseTs(String ts) {
        if (ts == null || ts.isBlank()) {
            return null;
        }
        try {
            return Instant.from(ARI_TS.parse(ts));
        } catch (Exception e) {
            try {
                return Instant.parse(ts);
            } catch (Exception ignored) {
                return null;
            }
        }
    }

    private static String text(JsonNode node, String field) {
        if (node == null || node.isMissingNode()) {
            return null;
        }
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static String nullToEmpty(String v) {
        return v == null ? "" : v;
    }
}