package com.overloadlab.gateway.ingest;

import com.overloadlab.gateway.config.OverloadProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;

@Slf4j
@Component
public class EventWriter {

    private static final String INSERT =
            "INSERT INTO events (event_id, batch_id, payload) VALUES (?, ?, ?)";

    private final DataSource dataSource;
    private final RestClient downstream;
    private final OverloadProperties props;
    private final IngestMetrics metrics;

    public EventWriter(DataSource dataSource, RestClient downstreamRestClient,
                       OverloadProperties props, IngestMetrics metrics) {
        this.dataSource = dataSource;
        this.downstream = downstreamRestClient;
        this.props = props;
        this.metrics = metrics;
    }

    public void write(EventTask task) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(INSERT)) {
                ps.setString(1, task.eventId());
                ps.setString(2, task.batchId());
                ps.setString(3, task.payload());
                ps.executeUpdate();
            }

            // The bug, stated plainly: a slow network call made while still holding a
            // pooled database connection. Everything downstream of this line is the lab.
            downstream.post()
                    .uri(props.getDownstreamUrl())
                    .body(Map.of("eventId", task.eventId(), "batchId", task.batchId()))
                    .retrieve()
                    .toBodilessEntity();

            conn.commit();
            metrics.recordE2e(task.enqueuedNanos());
        } catch (Exception e) {
            metrics.rejected(classify(e), 1);
            if (log.isDebugEnabled()) {
                log.debug("write failed for event {}: {}", task.eventId(), e.toString());
            }
        }
    }

    private String classify(Exception e) {
        String name = e.getClass().getName();
        String msg = String.valueOf(e.getMessage());
        if (name.contains("SQLTransientConnectionException") || msg.contains("Connection is not available")) {
            return IngestMetrics.REASON_DB_TIMEOUT;
        }
        if (name.contains("SocketTimeout") || name.contains("ConnectTimeout") || msg.contains("timeout")) {
            return IngestMetrics.REASON_DOWNSTREAM_TIMEOUT;
        }
        if (name.contains("CallNotPermitted")) {
            return IngestMetrics.REASON_BREAKER_OPEN;
        }
        return IngestMetrics.REASON_DOWNSTREAM_5XX;
    }
}
