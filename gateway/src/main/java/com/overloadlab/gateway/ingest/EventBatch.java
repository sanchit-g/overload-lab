package com.overloadlab.gateway.ingest;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public record EventBatch(
        @NotBlank String batchId,
        @NotEmpty @Valid List<IngestEvent> events
) {}
