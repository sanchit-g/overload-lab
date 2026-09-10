package com.overloadlab.gateway.ingest;

import com.overloadlab.gateway.config.OverloadProperties;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class EventsController {

    private final IngestQueue queue;
    private final IngestMetrics metrics;
    private final OverloadProperties props;

    public EventsController(IngestQueue queue, IngestMetrics metrics, OverloadProperties props) {
        this.queue = queue;
        this.metrics = metrics;
        this.props = props;
    }

    @PostMapping("/events")
    public ResponseEntity<Map<String, Object>> ingest(@Valid @RequestBody EventBatch batch) {
        List<IngestEvent> events = batch.events();

        if (events.size() > props.getMaxBatchSize()) {
            metrics.rejected(IngestMetrics.REASON_VALIDATION, events.size());
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "batch too large",
                    "maxBatchSize", props.getMaxBatchSize()));
        }

        if (queue.remainingCapacity() < events.size()) {
            metrics.rejected(IngestMetrics.REASON_QUEUE_FULL, events.size());
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header("Retry-After", "1")
                    .body(Map.of("error", "queue full", "events", events.size()));
        }

        long now = System.nanoTime();
        int enqueued = 0;
        for (IngestEvent e : events) {
            if (!queue.submit(new EventTask(batch.batchId(), e.eventId(), e.payload(), now))) {
                break;
            }
            enqueued++;
        }

        if (enqueued < events.size()) {
            metrics.accepted(enqueued);
            metrics.rejected(IngestMetrics.REASON_QUEUE_FULL, events.size() - enqueued);
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header("Retry-After", "1")
                    .body(Map.of("error", "queue full", "accepted", enqueued));
        }

        metrics.accepted(enqueued);
        return ResponseEntity.accepted().body(Map.of("accepted", enqueued));
    }

    /**
     * Bean-validation failures return 400 by default but would not otherwise be counted,
     * leaving the "validation" rejection reason with no producer.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> onInvalid(MethodArgumentNotValidException e) {
        metrics.rejected(IngestMetrics.REASON_VALIDATION, 1);
        return ResponseEntity.badRequest().body(Map.of(
                "error", "validation failed",
                "details", e.getBindingResult().getAllErrors().size()));
    }
}
