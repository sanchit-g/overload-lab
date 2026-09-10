package com.overloadlab.gateway.ingest;

import com.overloadlab.gateway.config.OverloadProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class IngestQueue {


    private final BlockingQueue<Runnable> queue;
    private final ThreadPoolExecutor executor;
    private final EventWriter writer;

    public IngestQueue(OverloadProperties props, EventWriter writer, MeterRegistry registry) {
        this.writer = writer;

        // s0: unbounded. Excess load is absorbed silently as latency debt and heap growth.
        // s2: bounded with an abort policy, so excess load becomes an immediate 429.
        this.queue = props.getQueue().isBounded()
                ? new ArrayBlockingQueue<>(props.getQueueCapacity())
                : new LinkedBlockingQueue<>();

        this.executor = new ThreadPoolExecutor(
                props.getWorkers(), props.getWorkers(),
                0L, TimeUnit.MILLISECONDS,
                queue,
                r -> {
                    Thread t = new Thread(r);
                    t.setName("ingest-worker-" + t.threadId());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.AbortPolicy());

        ExecutorServiceMetrics.monitor(registry, executor, "overload.workers");

        Gauge.builder("overload.queue.depth", queue, BlockingQueue::size)
                .description("Tasks waiting in the work queue").register(registry);
        Gauge.builder("overload.queue.capacity", this,
                        q -> props.getQueue().isBounded() ? props.getQueueCapacity() : -1)
                .description("Configured queue capacity; -1 means unbounded").register(registry);

        log.info("queue bounded={} capacity={} workers={}",
                props.getQueue().isBounded(),
                props.getQueue().isBounded() ? props.getQueueCapacity() : "unbounded",
                props.getWorkers());
    }

    @PostConstruct
    public void prestart() {
        // Prestart so executor.active and pool.size are meaningful from the first request
        // rather than ramping during the warmup window.
        executor.prestartAllCoreThreads();
    }

    /** @return true if accepted; false if the bounded queue refused it. */
    public boolean submit(EventTask task) {
        try {
            executor.execute(() -> writer.write(task));
            return true;
        } catch (java.util.concurrent.RejectedExecutionException e) {
            return false;
        }
    }

    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
