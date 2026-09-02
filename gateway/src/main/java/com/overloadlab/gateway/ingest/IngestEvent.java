package com.overloadlab.gateway.ingest;

import jakarta.validation.constraints.NotBlank;

public record IngestEvent(
        @NotBlank String eventId,
        @NotBlank String payload
) {}
