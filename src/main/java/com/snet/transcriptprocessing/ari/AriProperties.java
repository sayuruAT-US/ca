package com.snet.transcriptprocessing.ari;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.ari")
public class AriProperties {

    private boolean enabled = false;
    private String host = "localhost";
    private int port = 443;
    private boolean ssl = true;
    private String username = "";
    private String password = "";
    private String appName = "";
    private boolean subscribeAll = true;
    private long reconnectDelayMs = 5000;
    private boolean debugTopicEnabled = false;
    private String debugTopic = "ari.events";

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getHost() { return host; }
    public void setHost(String host) {
        if (host != null) {
            host = host.replaceFirst("^https?://", "")
                    .replaceFirst("^wss?://", "")
                    .replaceFirst("/.*$", "");
        }
        this.host = host;
    }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public boolean isSsl() { return ssl; }
    public void setSsl(boolean ssl) { this.ssl = ssl; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    public String getAppName() { return appName; }
    public void setAppName(String appName) { this.appName = appName; }

    public boolean isSubscribeAll() { return subscribeAll; }
    public void setSubscribeAll(boolean subscribeAll) { this.subscribeAll = subscribeAll; }

    public long getReconnectDelayMs() { return reconnectDelayMs; }
    public void setReconnectDelayMs(long reconnectDelayMs) { this.reconnectDelayMs = reconnectDelayMs; }

    public boolean isDebugTopicEnabled() { return debugTopicEnabled; }
    public void setDebugTopicEnabled(boolean debugTopicEnabled) { this.debugTopicEnabled = debugTopicEnabled; }

    public String getDebugTopic() { return debugTopic; }
    public void setDebugTopic(String debugTopic) { this.debugTopic = debugTopic; }

    public String buildEventsUrl() {
        String scheme = ssl ? "wss" : "ws";
        String h = host == null ? "localhost" : host;
        String q = "api_key=" + urlEncode(username) + ":" + urlEncode(password)
                + "&app=" + urlEncode(appName)
                + "&subscribeAll=" + subscribeAll;
        boolean defaultPort = (ssl && port == 443) || (!ssl && port == 80);
        String hostPart = defaultPort ? h : h + ":" + port;
        return scheme + "://" + hostPart + "/ari/events?" + q;
    }

    public String buildRestBaseUrl() {
        String scheme = ssl ? "https" : "http";
        String h = host == null ? "localhost" : host;
        boolean defaultPort = (ssl && port == 443) || (!ssl && port == 80);
        String hostPart = defaultPort ? h : h + ":" + port;
        return scheme + "://" + hostPart + "/ari";
    }

    private static String urlEncode(String value) {
        try {
            return java.net.URLEncoder.encode(value == null ? "" : value, java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            return value;
        }
    }
}