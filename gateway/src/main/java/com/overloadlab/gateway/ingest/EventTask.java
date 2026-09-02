package com.overloadlab.gateway.ingest;

public record EventTask(
        String batchId,
        String eventId,
        String payload,
        long enqueuedNanos
) {}
