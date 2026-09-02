package com.overloadlab.sim;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@RestController
public class SimController {


    private final AtomicReference<Knobs> knobs = new AtomicReference<>(Knobs.healthy());

    @PostMapping("/ingest")
    public ResponseEntity<Map<String, Object>> ingest(@RequestBody(required = false) Map<String, Object> body)
            throws InterruptedException {
        Knobs k = knobs.get();

        if (k.blackhole()) {
            // Accept the connection and never respond. Without a client-side response
            // timeout, the caller's thread parks here forever, still holding whatever
            // resources it acquired before the call.
            TimeUnit.HOURS.sleep(1);
        }

        long jitter = k.jitterMs() == 0 ? 0
                : ThreadLocalRandom.current().nextLong(-k.jitterMs(), k.jitterMs() + 1);
        long sleep = Math.max(0, k.latencyMs() + jitter);
        TimeUnit.MILLISECONDS.sleep(sleep);

        if (k.failureRate() > 0 && ThreadLocalRandom.current().nextDouble() < k.failureRate()) {
            return ResponseEntity.status(500).body(Map.of("status", "error"));
        }
        return ResponseEntity.ok(Map.of("status", "ok"));
    }

    @PostMapping("/control")
    public Knobs setControl(@RequestBody Knobs incoming) {
        knobs.set(incoming);
        log.info("knobs updated: {}", incoming);
        return incoming;
    }

    @GetMapping("/control")
    public Knobs getControl() {
        return knobs.get();
    }
}
