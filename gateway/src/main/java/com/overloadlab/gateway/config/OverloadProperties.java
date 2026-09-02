package com.overloadlab.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Every stage flag and tunable. All protections default to OFF: the fragile
 * configuration is the default, and a stage is an env file that turns things on.
 */
@ConfigurationProperties(prefix = "overload")
public class OverloadProperties {

    private int workers = 64;
    private int queueCapacity = 5000;
    private int httpPoolSize = 32;
    private int maxBatchSize = 500;
    private String downstreamUrl = "http://downstream-sim:9090/ingest";

    private final Http http = new Http();
    private final Queue queue = new Queue();
    private final Admission admission = new Admission();
    private final Breaker breaker = new Breaker();

    public static class Http {
        private final Timeouts timeouts = new Timeouts();
        public Timeouts getTimeouts() { return timeouts; }

        public static class Timeouts {
            private boolean enabled = false;
            private int connectMs = 500;
            private int responseMs = 250;
            private int leaseMs = 100;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }
            public int getConnectMs() { return connectMs; }
            public void setConnectMs(int connectMs) { this.connectMs = connectMs; }
            public int getResponseMs() { return responseMs; }
            public void setResponseMs(int responseMs) { this.responseMs = responseMs; }
            public int getLeaseMs() { return leaseMs; }
            public void setLeaseMs(int leaseMs) { this.leaseMs = leaseMs; }
        }
    }

    public static class Queue {
        private boolean bounded = false;
        public boolean isBounded() { return bounded; }
        public void setBounded(boolean bounded) { this.bounded = bounded; }
    }

    public static class Admission {
        private boolean enabled = false;
        private int limit = 40;
        private int window = 1;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public int getLimit() { return limit; }
        public void setLimit(int limit) { this.limit = limit; }
        public int getWindow() { return window; }
        public void setWindow(int window) { this.window = window; }
    }

    public static class Breaker {
        private boolean enabled = false;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }

    public int getWorkers() { return workers; }
    public void setWorkers(int workers) { this.workers = workers; }
    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
    public int getHttpPoolSize() { return httpPoolSize; }
    public void setHttpPoolSize(int httpPoolSize) { this.httpPoolSize = httpPoolSize; }
    public int getMaxBatchSize() { return maxBatchSize; }
    public void setMaxBatchSize(int maxBatchSize) { this.maxBatchSize = maxBatchSize; }
    public String getDownstreamUrl() { return downstreamUrl; }
    public void setDownstreamUrl(String downstreamUrl) { this.downstreamUrl = downstreamUrl; }
    public Http getHttp() { return http; }
    public Queue getQueue() { return queue; }
    public Admission getAdmission() { return admission; }
    public Breaker getBreaker() { return breaker; }
}
