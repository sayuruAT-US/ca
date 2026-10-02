package com.snet.transcriptprocessing.ari;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

@Component
public class AriRestClient {

    private static final Logger log = LoggerFactory.getLogger(AriRestClient.class);

    private final AriProperties properties;
    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate;

    public AriRestClient(AriProperties properties, ObjectMapper objectMapper, RestTemplateBuilder builder) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.restTemplate = builder
                .connectTimeout(Duration.ofSeconds(10))
                .readTimeout(Duration.ofSeconds(30))
                .build();
    }

    public String startSnoop(String channelId, String spy, String appName) {
        try {
            String url = UriComponentsBuilder
                    .fromHttpUrl(properties.buildRestBaseUrl() + "/channels/" + channelId + "/snoop")
                    .queryParam("spy", spy)
                    .queryParam("whisper", "none")
                    .queryParam("app", appName)
                    .toUriString();

            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.POST, new HttpEntity<>(authHeaders()), String.class);

            if (response.getBody() == null) {
                return null;
            }
            JsonNode node = objectMapper.readTree(response.getBody());
            String id = node.path("id").asText(null);
            log.info("ARI snoop started | channelId={} snoopId={} spy={}", channelId, id, spy);
            return id;
        } catch (Exception e) {
            log.error("ARI snoop failed | channelId={} error={}", channelId, e.getMessage());
            return null;
        }
    }

    public void hangup(String channelId) {
        try {
            String url = properties.buildRestBaseUrl() + "/channels/" + channelId;
            restTemplate.exchange(url, HttpMethod.DELETE, new HttpEntity<>(authHeaders()), String.class);
        } catch (Exception e) {
            log.warn("ARI hangup failed | channelId={} error={}", channelId, e.getMessage());
        }
    }

    private HttpHeaders authHeaders() {
        HttpHeaders headers = new HttpHeaders();
        String raw = properties.getUsername() + ":" + properties.getPassword();
        String basic = Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
        headers.set(HttpHeaders.AUTHORIZATION, "Basic " + basic);
        return headers;
    }
}