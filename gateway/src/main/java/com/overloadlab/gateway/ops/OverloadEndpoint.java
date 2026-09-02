package com.overloadlab.gateway.ops;

import com.overloadlab.gateway.config.OverloadProperties;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
@Endpoint(id = "overload")
public class OverloadEndpoint {

    private final OverloadProperties props;
    private final DataSource dataSource;

    public OverloadEndpoint(OverloadProperties props, DataSource dataSource) {
        this.props = props;
        this.dataSource = dataSource;
    }

    @ReadOperation
    public Map<String, Object> state() {
        Map<String, Object> protections = new LinkedHashMap<>();
        protections.put("timeouts", props.getHttp().getTimeouts().isEnabled());
        protections.put("boundedQueue", props.getQueue().isBounded());
        protections.put("admission", props.getAdmission().isEnabled());
        protections.put("breaker", props.getBreaker().isEnabled());

        Map<String, Object> tunables = new LinkedHashMap<>();
        tunables.put("workers", props.getWorkers());
        tunables.put("queueCapacity", props.getQueueCapacity());
        tunables.put("httpPoolSize", props.getHttpPoolSize());
        tunables.put("responseTimeoutMs", props.getHttp().getTimeouts().getResponseMs());
        tunables.put("admissionLimit", props.getAdmission().getLimit());
        if (dataSource instanceof HikariDataSource hikari) {
            tunables.put("hikariMaxPoolSize", hikari.getMaximumPoolSize());
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("protections", protections);
        out.put("tunables", tunables);
        return out;
    }
}
