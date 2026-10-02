package com.snet.transcriptprocessing.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class CiapRestClient {

    private static final Logger logger = LoggerFactory.getLogger(CiapRestClient.class);

    private final RestTemplate restTemplate;
    private final String ciapBaseUrl;

    public CiapRestClient(
            RestTemplate restTemplate,
            @Value("${ciap.api.base-url}") String ciapBaseUrl) {
        this.restTemplate = restTemplate;
        this.ciapBaseUrl = ciapBaseUrl;
    }

    public boolean send(String payload, String endpointPath) {
        String fullUrl = ciapBaseUrl + (endpointPath.startsWith("/") ? endpointPath : "/" + endpointPath);
        logger.info("Sending payload to CIAP endpoint: {}", fullUrl);
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> requestEntity = new HttpEntity<>(payload, headers);
            ResponseEntity<Object> response = restTemplate.postForEntity(fullUrl, requestEntity, Object.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                logger.info("CIAP response OK | status={}", response.getStatusCode());
                return true;
            }
            logger.error("CIAP returned non-2xx status: {}", response.getStatusCode());
        } catch (Exception e) {
            logger.error("Error communicating with CIAP at {}: {}", fullUrl, e.getMessage(), e);
        }
        return false;
    }
}
