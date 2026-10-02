package com.snet.transcriptprocessing.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IncomingEventRequest {

    private String module;

    private JsonNode payload;

    @JsonProperty("tenant_id")
    @JsonAlias({"tenant_id", "tenantId"})
    private Long tenantId;

    public String getModule() { return module; }
    public void setModule(String module) { this.module = module; }

    public JsonNode getPayload() { return payload; }
    public void setPayload(JsonNode payload) { this.payload = payload; }

    public Long getTenantId() { return tenantId; }
    public void setTenantId(Long tenantId) { this.tenantId = tenantId; }
}
