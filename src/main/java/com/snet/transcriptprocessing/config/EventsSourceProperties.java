package com.snet.transcriptprocessing.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "app.events")
public class EventsSourceProperties {

    private String source = "pbxware";

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source == null ? "pbxware" : source.trim().toLowerCase();
    }

    public boolean acceptPbxware() {
        return "pbxware".equals(source) || "both".equals(source);
    }

    public boolean acceptAri() {
        return "ari".equals(source) || "both".equals(source);
    }
}