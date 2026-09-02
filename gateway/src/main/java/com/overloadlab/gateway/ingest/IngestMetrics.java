package com.overloadlab.gateway.ingest;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Rejection reasons are a tag rather than separate counters so the dashboard can
 * stack error rate by type from a single series.
 */
@Component
public class IngestMetrics {

    public static final String REASON_VALIDATION = "validation";
    public static final String REASON_QUEUE_FULL = "queue_full";
    public static final String REASON_ADMISSION = "admission";
    public static final String REASON_BREAKER_OPEN = "breaker_open";
    public static final String REASON_DOWNSTREAM_TIMEOUT = "downstream_timeout";
    public static final String REASON_DOWNSTREAM_5XX = "downstream_5xx";
    public static final String REASON_DB_TIMEOUT = "db_timeout";

    private final MeterRegistry registry;
    private final Counter accepted;
    private final Timer e2e;

    public IngestMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.accepted = Counter.builder("overload.events.accepted")
                .description("Events accepted onto the work queue")
                .register(registry);
        this.e2e = Timer.builder("overload.event.e2e")
                .description("Enqueue to downstream ack")
                .publishPercentiles(0.5, 0.99, 0.999)
                .register(registry);
    }

    public void accepted(int count) {
        accepted.increment(count);
    }

    public void rejected(String reason, int count) {
        Counter.builder("overload.events.rejected")
                .tag("reason", reason)
                .description("Events rejected, by reason")
                .register(registry)
                .increment(count);
    }

    public void recordE2e(long startNanos) {
        e2e.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }
}
