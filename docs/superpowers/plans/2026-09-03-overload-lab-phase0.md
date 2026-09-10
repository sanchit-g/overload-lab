# overload-lab Phase 0 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the lab foundation and produce a reproducible, instrumented collapse of the fragile gateway (stage s0), with a calibrated knee rate.

**Architecture:** Two Spring Boot 3.3 / Java 21 services (`gateway`, `downstream-sim`) under one aggregator pom, plus Postgres and Redis, in one compose stack; Prometheus and Grafana in a second stack on a shared external network. The gateway holds a Hikari connection across a downstream HTTP call, so capacity is `hikariMax / downstreamLatency` and the unbounded queue converts any excess into latency debt and eventual OOM. The rate limiter under test is a pinned git submodule, built into the gateway image by a multi-stage Dockerfile.

**Tech Stack:** Java 21, Spring Boot 3.3.5, Apache HttpClient 5, HikariCP, PostgreSQL 16, Redis 7, Micrometer/Prometheus, Grafana 11, k6, Docker Compose.

**Scope note:** Phase 0 implements stage s0 (fragile) plus the flag/actuator scaffolding for all four protections, and the queue component with both unbounded and bounded branches (one conditional). Timeouts, admission control and the circuit breaker land in Phase 1, which has its own plan.

**Testing note:** By explicit decision there is no unit test suite. Verification is by real runnable checks — build succeeds, container becomes healthy, endpoint returns expected status, metric appears in `/actuator/prometheus`. The preflight assertion in Task 19 is the guard that protects measurement validity.

---

## File Structure

| File | Responsibility |
|---|---|
| `pom.xml` | Aggregator; pins Java 21, Boot 3.3.5 |
| `vendor/distributed-rate-limiter/` | Submodule, pinned `2789431`; dependency + working copy |
| `downstream-sim/src/.../DownstreamSimApplication.java` | Boot entrypoint |
| `downstream-sim/src/.../Knobs.java` | Mutable latency/failure/mode state |
| `downstream-sim/src/.../SimController.java` | `POST /ingest`, `POST /control`, `GET /control` |
| `db/init.sql` | `events` table |
| `gateway/src/.../GatewayApplication.java` | Boot entrypoint |
| `gateway/src/.../config/OverloadProperties.java` | All four stage flags + tunables |
| `gateway/src/.../config/DownstreamClientConfig.java` | HC5 pool + `RestClient`; pool gauges |
| `gateway/src/.../ingest/IngestEvent.java` | One event |
| `gateway/src/.../ingest/EventBatch.java` | Request body + validation constraints |
| `gateway/src/.../ingest/EventsController.java` | `POST /events`; 202/400/429 |
| `gateway/src/.../ingest/IngestQueue.java` | Executor + queue; bounded/unbounded by flag |
| `gateway/src/.../ingest/EventWriter.java` | **The bug**: Hikari connection held across HTTP call |
| `gateway/src/.../ingest/IngestMetrics.java` | Counters, timers, gauges |
| `gateway/src/.../ops/OverloadEndpoint.java` | `/actuator/overload` — preflight truth source |
| `gateway/Dockerfile` | Multi-stage; installs submodule then builds gateway |
| `compose/app.yml` | gateway, downstream-sim, postgres, redis; cpu/mem limits |
| `compose/obs.yml` | prometheus, grafana |
| `prometheus/prometheus.yml` | 1s scrape |
| `grafana/provisioning/**`, `grafana/dashboards/overload.json` | Auto-provisioned dashboard |
| `k6/steady.js`, `k6/knee.js` | Open-model load |
| `tools/capture/` | Prometheus range API -> series JSON + PNGs |
| `stages/s0..s4.env` | Stage flag sets |
| `scripts/run-stage.sh` | Restart, preflight assert, warm up, load, capture |

---

### Task 1: Repository scaffolding and pinned submodule

**Files:**
- Create: `.gitignore`, `pom.xml`, `README.md`

- [x] **Step 1: Create `.gitignore`**

```gitignore
target/
*.class
.idea/
*.iml
.DS_Store
results/**/raw/
docs/img/*.png
!docs/img/.gitkeep
```

- [x] **Step 2: Add the rate limiter as a pinned submodule**

```bash
git submodule add https://github.com/sanchit-g/distributed-rate-limiter.git vendor/distributed-rate-limiter
git -C vendor/distributed-rate-limiter checkout 2789431a184f977e08d4466969f133681bc10a16
```

Expected: `vendor/distributed-rate-limiter` populated, detached at `2789431`.

- [x] **Step 3: Create aggregator `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <!-- spring-boot-starter-parent gives us, for free: managed dependency versions,
         the spring-boot-maven-plugin with its repackage goal already bound, Lombok
         excluded from the repackaged jar, and resource filtering with @..@ delimiters
         so Spring's own ${...} placeholders in application.yml are left alone. -->
    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.5</version>
        <relativePath/>
    </parent>

    <groupId>com.overloadlab</groupId>
    <artifactId>overload-lab-parent</artifactId>
    <version>0.1.0</version>
    <packaging>pom</packaging>

    <modules>
        <module>gateway</module>
        <module>downstream-sim</module>
    </modules>

    <properties>
        <java.version>21</java.version>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>
    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.springframework.boot</groupId>
                    <artifactId>spring-boot-maven-plugin</artifactId>
                    <!-- starter-parent binds the repackage goal for us, but does NOT
                         exclude Lombok: <optional>true</optional> governs transitive
                         resolution, not packaging, so Lombok would otherwise ship
                         inside BOOT-INF/lib. Declared once here; both modules inherit. -->
                    <configuration>
                        <excludes>
                            <exclude>
                                <groupId>org.projectlombok</groupId>
                                <artifactId>lombok</artifactId>
                            </exclude>
                        </excludes>
                    </configuration>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>
</project>
```

- [x] **Step 4: Verify toolchain is present**

```bash
java -version && mvn -v && docker --version && k6 version
```

Expected: Java 21+, Maven 3.8+, Docker, k6. If k6 is missing: `brew install k6`.

- [x] **Step 5: Commit**

```bash
git add .gitignore pom.xml .gitmodules vendor/distributed-rate-limiter docs/
git commit -m "chore: scaffold overload-lab with pinned rate limiter submodule"
```

---

### Task 2: downstream-sim service

**Files:**
- Create: `downstream-sim/pom.xml`
- Create: `downstream-sim/src/main/java/com/overloadlab/sim/DownstreamSimApplication.java`
- Create: `downstream-sim/src/main/java/com/overloadlab/sim/Knobs.java`
- Create: `downstream-sim/src/main/java/com/overloadlab/sim/SimController.java`
- Create: `downstream-sim/src/main/resources/application.yml`

- [x] **Step 1: Create `downstream-sim/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.overloadlab</groupId>
        <artifactId>overload-lab-parent</artifactId>
        <version>0.1.0</version>
    </parent>
    <artifactId>downstream-sim</artifactId>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [x] **Step 2: Create `DownstreamSimApplication.java`**

```java
package com.overloadlab.sim;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DownstreamSimApplication {
    public static void main(String[] args) {
        SpringApplication.run(DownstreamSimApplication.class, args);
    }
}
```

- [x] **Step 3: Create `Knobs.java`**

```java
package com.overloadlab.sim;

/**
 * Runtime-mutable fault knobs. Replaced wholesale via POST /control so a scenario
 * can shift the fault mid-run at a known timestamp.
 *
 * mode NORMAL   - sleep latencyMs +/- jitterMs, then respond
 * mode BLACKHOLE - accept the connection and never respond
 */
public record Knobs(long latencyMs, long jitterMs, double failureRate, String mode) {

    public static Knobs healthy() {
        return new Knobs(25, 10, 0.0, "NORMAL");
    }

    public boolean blackhole() {
        return "BLACKHOLE".equalsIgnoreCase(mode);
    }
}
```

- [x] **Step 4: Create `SimController.java`**

```java
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
```

- [x] **Step 5: Create `downstream-sim/src/main/resources/application.yml`**

```yaml
spring:
  application:
    name: downstream-sim
  threads:
    virtual:
      enabled: false
server:
  port: 9090
  tomcat:
    threads:
      max: 400
logging:
  level:
    root: INFO
```

- [x] **Step 6: Build and smoke test**

```bash
mvn -q -f downstream-sim/pom.xml package -DskipTests
java -jar downstream-sim/target/downstream-sim-0.1.0.jar &
sleep 12
curl -s -w ' [%{http_code}] %{time_total}s\n' -X POST localhost:9090/ingest -H 'Content-Type: application/json' -d '{}'
curl -s -X POST localhost:9090/control -H 'Content-Type: application/json' \
  -d '{"latencyMs":400,"jitterMs":0,"failureRate":0,"mode":"NORMAL"}'
curl -s -w ' [%{http_code}] %{time_total}s\n' -X POST localhost:9090/ingest -H 'Content-Type: application/json' -d '{}'
kill %1
```

Expected: first `/ingest` ~0.02-0.04s with `[200]`; after `/control`, ~0.40s with `[200]`.

- [x] **Step 7: Commit**

```bash
git add downstream-sim
git commit -m "feat(sim): downstream simulator with runtime latency and failure knobs"
```

---

### Task 3: downstream-sim Dockerfile

**Files:**
- Create: `downstream-sim/Dockerfile`
- Create: `.dockerignore`

- [x] **Step 1: Create `downstream-sim/Dockerfile`**

Build context is the repo root so the aggregator pom resolves.

```dockerfile
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY downstream-sim/pom.xml downstream-sim/
COPY downstream-sim/src downstream-sim/src
RUN mvn -q -B -f downstream-sim/pom.xml package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/downstream-sim/target/downstream-sim-0.1.0.jar app.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

**Step 1b: Create `.dockerignore`**

The build context is the repo root, so without this every image build uploads `target/` directories and the entire `.git` history to the Docker daemon. `vendor/` must NOT be ignored — Task 13's gateway Dockerfile needs the submodule in its context to build the rate limiter starter.

```
.git
.gitignore
target/
**/target/
docs/
results/
*.md
.idea/
*.iml
.DS_Store
```

- [x] **Step 2: Verify the image builds**

```bash
docker build -f downstream-sim/Dockerfile -t overload-lab/downstream-sim:dev .
```

Expected: build succeeds, image tagged.

- [x] **Step 3: Commit**

```bash
git add downstream-sim/Dockerfile .dockerignore
git commit -m "build(sim): multi-stage Dockerfile for downstream-sim"
```

---

### Task 4: Postgres schema

**Files:**
- Create: `db/init.sql`

- [x] **Step 1: Create `db/init.sql`**

```sql
-- Deliberately trivial. Postgres is in the write path so that a Hikari connection
-- is held across the downstream HTTP call; it is not itself meant to be a bottleneck.
CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL PRIMARY KEY,
    event_id    TEXT        NOT NULL,
    batch_id    TEXT        NOT NULL,
    payload     TEXT        NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_batch ON events (batch_id);
```

- [x] **Step 2: Commit**

```bash
git add db/init.sql
git commit -m "feat(db): events table schema"
```

---

### Task 5: Gateway module skeleton

**Files:**
- Create: `gateway/pom.xml`
- Create: `gateway/src/main/java/com/overloadlab/gateway/GatewayApplication.java`
- Create: `gateway/src/main/resources/application.yml`

- [x] **Step 1: Create `gateway/pom.xml`**

`rate-limiter-spring-boot-starter` is declared now but unused until Phase 1; it verifies the submodule build wiring works from the start.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>com.overloadlab</groupId>
        <artifactId>overload-lab-parent</artifactId>
        <version>0.1.0</version>
    </parent>
    <artifactId>gateway</artifactId>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.httpcomponents.client5</groupId>
            <artifactId>httpclient5</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>com.ratelimiter</groupId>
            <artifactId>rate-limiter-spring-boot-starter</artifactId>
            <version>1.0.0</version>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [x] **Step 2: Create `GatewayApplication.java`**

```java
package com.overloadlab.gateway;

import com.overloadlab.gateway.config.OverloadProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(OverloadProperties.class)
public class GatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}
```

- [x] **Step 3: Create `gateway/src/main/resources/application.yml`**

Note `spring.threads.virtual.enabled: false` — the demonstration depends on a bounded pool of platform threads.

```yaml
spring:
  application:
    name: gateway
  threads:
    virtual:
      enabled: false
  datasource:
    url: jdbc:postgresql://postgres:5432/overload
    username: overload
    password: overload
    hikari:
      maximum-pool-size: ${OVERLOAD_HIKARI_MAX:20}
      minimum-idle: ${OVERLOAD_HIKARI_MAX:20}
      connection-timeout: 30000
      pool-name: gateway-pool
      register-mbeans: false
  data:
    redis:
      host: redis
      port: 6379

server:
  port: 8080
  tomcat:
    threads:
      max: 200

overload:
  workers: ${OVERLOAD_WORKERS:64}
  queue-capacity: ${OVERLOAD_QUEUE_CAPACITY:5000}
  downstream-url: ${OVERLOAD_DOWNSTREAM_URL:http://downstream-sim:9090/ingest}
  http-pool-size: ${OVERLOAD_HTTP_POOL:32}
  max-batch-size: 500
  http:
    timeouts:
      enabled: ${OVERLOAD_TIMEOUTS_ENABLED:false}
      connect-ms: ${OVERLOAD_CONNECT_MS:500}
      response-ms: ${OVERLOAD_RESPONSE_MS:250}
      lease-ms: ${OVERLOAD_LEASE_MS:100}
  queue:
    bounded: ${OVERLOAD_QUEUE_BOUNDED:false}
  admission:
    enabled: ${OVERLOAD_ADMISSION_ENABLED:false}
    limit: ${OVERLOAD_ADMISSION_LIMIT:40}
    window: ${OVERLOAD_ADMISSION_WINDOW:1}
  breaker:
    enabled: ${OVERLOAD_BREAKER_ENABLED:false}

management:
  endpoints:
    web:
      exposure:
        include: health,prometheus,overload
  endpoint:
    health:
      show-details: always
  metrics:
    tags:
      application: gateway
      instance: ${HOSTNAME:local}
    distribution:
      percentiles-histogram:
        http.server.requests: true
        overload.event.e2e: true
      minimum-expected-value:
        overload.event.e2e: 1ms
      maximum-expected-value:
        overload.event.e2e: 120s

logging:
  level:
    root: WARN
    com.overloadlab: INFO
```

- [x] **Step 4: Commit**

```bash
git add gateway/pom.xml gateway/src/main/java/com/overloadlab/gateway/GatewayApplication.java gateway/src/main/resources/application.yml
git commit -m "feat(gateway): module skeleton and stage-flag configuration"
```

---

### Task 6: Configuration properties and the preflight endpoint

**Files:**
- Create: `gateway/src/main/java/com/overloadlab/gateway/config/OverloadProperties.java`
- Create: `gateway/src/main/java/com/overloadlab/gateway/ops/OverloadEndpoint.java`

- [x] **Step 1: Create `OverloadProperties.java`**

```java
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
```

- [x] **Step 2: Create `OverloadEndpoint.java`**

This is the truth source the preflight assertion reads. It reports what is *actually* wired, not what was requested, so a flag that silently failed to take effect is caught before a five-minute run is wasted.

```java
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
```

- [x] **Step 3: Commit**

```bash
git add gateway/src/main/java/com/overloadlab/gateway/config gateway/src/main/java/com/overloadlab/gateway/ops
git commit -m "feat(gateway): stage flags and /actuator/overload preflight endpoint"
```

---

### Task 7: Ingest domain and validation

**Files:**
- Create: `gateway/src/main/java/com/overloadlab/gateway/ingest/IngestEvent.java`
- Create: `gateway/src/main/java/com/overloadlab/gateway/ingest/EventBatch.java`
- Create: `gateway/src/main/java/com/overloadlab/gateway/ingest/EventTask.java`

- [x] **Step 1: Create `IngestEvent.java`**

```java
package com.overloadlab.gateway.ingest;

import jakarta.validation.constraints.NotBlank;

public record IngestEvent(
        @NotBlank String eventId,
        @NotBlank String payload
) {}
```

- [x] **Step 2: Create `EventBatch.java`**

```java
package com.overloadlab.gateway.ingest;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public record EventBatch(
        @NotBlank String batchId,
        @NotEmpty @Valid List<IngestEvent> events
) {}
```

- [x] **Step 3: Create `EventTask.java`**

`enqueuedNanos` is the start of the end-to-end clock: the true latency of an event is measured from the moment it is accepted, not from when a worker happens to pick it up.

```java
package com.overloadlab.gateway.ingest;

public record EventTask(
        String batchId,
        String eventId,
        String payload,
        long enqueuedNanos
) {}
```

- [x] **Step 4: Commit**

```bash
git add gateway/src/main/java/com/overloadlab/gateway/ingest
git commit -m "feat(gateway): ingest domain types"
```

---

### Task 8: Metrics

**Files:**
- Create: `gateway/src/main/java/com/overloadlab/gateway/ingest/IngestMetrics.java`

- [x] **Step 1: Create `IngestMetrics.java`**

```java
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
```

- [x] **Step 2: Commit**

```bash
git add gateway/src/main/java/com/overloadlab/gateway/ingest/IngestMetrics.java
git commit -m "feat(gateway): ingest metrics with rejection reasons and e2e timer"
```

---

### Task 9: Downstream HTTP client

**Files:**
- Create: `gateway/src/main/java/com/overloadlab/gateway/config/DownstreamClientConfig.java`

- [x] **Step 1: Create `DownstreamClientConfig.java`**

Two things matter here. The pool is sized **above** Hikari's max so it never becomes a second constraint — at most 20 workers are ever past the connection gate. And at s0 the timeouts are not merely long, they are an hour: a worker that calls a black-holed downstream parks effectively forever, which is exactly the failure being demonstrated.

```java
package com.overloadlab.gateway.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.util.Timeout;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
public class DownstreamClientConfig {

    /** Stand-in for "no timeout" at stage s0. Long enough to be indistinguishable from infinite. */
    private static final Timeout EFFECTIVELY_INFINITE = Timeout.ofHours(1);

    @Bean
    public PoolingHttpClientConnectionManager connectionManager(OverloadProperties props,
                                                               MeterRegistry registry) {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(props.getHttpPoolSize());
        cm.setDefaultMaxPerRoute(props.getHttpPoolSize());

        // Gauges are registered here rather than in a second @Bean: two beans of the same
        // type would make the injection in httpClient() ambiguous.
        Gauge.builder("overload.http.pool.leased", cm, m -> m.getTotalStats().getLeased())
                .description("HTTP connections currently leased").register(registry);
        Gauge.builder("overload.http.pool.pending", cm, m -> m.getTotalStats().getPending())
                .description("Threads waiting for an HTTP connection").register(registry);
        Gauge.builder("overload.http.pool.available", cm, m -> m.getTotalStats().getAvailable())
                .description("Idle HTTP connections").register(registry);

        boolean timeouts = props.getHttp().getTimeouts().isEnabled();
        cm.setDefaultConnectionConfig(ConnectionConfig.custom()
                .setConnectTimeout(timeouts
                        ? Timeout.ofMilliseconds(props.getHttp().getTimeouts().getConnectMs())
                        : EFFECTIVELY_INFINITE)
                .build());
        return cm;
    }

    @Bean
    public CloseableHttpClient httpClient(PoolingHttpClientConnectionManager cm, OverloadProperties props) {
        boolean timeouts = props.getHttp().getTimeouts().isEnabled();
        RequestConfig requestConfig = RequestConfig.custom()
                .setResponseTimeout(timeouts
                        ? Timeout.ofMilliseconds(props.getHttp().getTimeouts().getResponseMs())
                        : EFFECTIVELY_INFINITE)
                .setConnectionRequestTimeout(timeouts
                        ? Timeout.ofMilliseconds(props.getHttp().getTimeouts().getLeaseMs())
                        : EFFECTIVELY_INFINITE)
                .build();
        return HttpClients.custom()
                .setConnectionManager(cm)
                .setDefaultRequestConfig(requestConfig)
                .build();
    }

    @Bean
    public RestClient downstreamRestClient(CloseableHttpClient httpClient) {
        return RestClient.builder()
                .requestFactory(new HttpComponentsClientHttpRequestFactory(httpClient))
                .build();
    }

}
```

- [x] **Step 2: Commit**

```bash
git add gateway/src/main/java/com/overloadlab/gateway/config/DownstreamClientConfig.java
git commit -m "feat(gateway): pooled HTTP client with stage-controlled timeouts"
```

---

### Task 10: EventWriter — the deliberate bug

**Files:**
- Create: `gateway/src/main/java/com/overloadlab/gateway/ingest/EventWriter.java`

- [x] **Step 1: Create `EventWriter.java`**

This is the entire point of the lab. The connection is acquired, used, and then **held across a network call** before commit. Capacity becomes `hikariMax / downstreamLatency`, governed by a pool that has nothing to do with how fast Postgres is. Do not "fix" this by moving the HTTP call outside the try-with-resources; Phase 1 measures it as-is.

```java
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
```

- [x] **Step 2: Commit**

```bash
git add gateway/src/main/java/com/overloadlab/gateway/ingest/EventWriter.java
git commit -m "feat(gateway): event writer holding a DB connection across the downstream call"
```

---

### Task 11: Work queue and worker pool

**Files:**
- Create: `gateway/src/main/java/com/overloadlab/gateway/ingest/IngestQueue.java`

- [x] **Step 1: Create `IngestQueue.java`**

The unbounded branch is stage s0. The bounded branch is one conditional and is included now so the queue component is complete; Phase 1 measures it.

```java
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
```

- [x] **Step 2: Commit**

```bash
git add gateway/src/main/java/com/overloadlab/gateway/ingest/IngestQueue.java
git commit -m "feat(gateway): worker pool with flag-driven bounded/unbounded queue"
```

---

### Task 12: Ingest controller

**Files:**
- Create: `gateway/src/main/java/com/overloadlab/gateway/ingest/EventsController.java`

- [x] **Step 1: Create `EventsController.java`**

Batch admission is all-or-nothing: a partially-accepted batch would make accepted-throughput accounting ambiguous, and ambiguity in the numbers is the one thing this repo cannot afford. The pre-check races, so `submit()` returning false is still handled as a backstop.

```java
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
```

- [x] **Step 2: Build the gateway**

The submodule must be installed to the local repo first, since `rate-limiter-spring-boot-starter` is not on Maven Central.

```bash
mvn -q -B -f vendor/distributed-rate-limiter/pom.xml -pl rate-limiter-spring-boot-starter -am install -DskipTests
mvn -q -B -pl gateway -am package -DskipTests
ls -la gateway/target/gateway-0.1.0.jar
```

Expected: both builds succeed; the jar exists.

- [x] **Step 3: Commit**

```bash
git add gateway/src/main/java/com/overloadlab/gateway/ingest/EventsController.java
git commit -m "feat(gateway): POST /events with all-or-nothing batch admission"
```

---

### Task 13: Gateway Dockerfile

**Files:**
- Create: `gateway/Dockerfile`

- [x] **Step 1: Create `gateway/Dockerfile`**

The submodule is built and installed inside the image, so a clean clone needs no local `mvn install` and no Maven Central publication. `-pl rate-limiter-spring-boot-starter -am` skips the starter's demo module.

```dockerfile
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

COPY vendor/distributed-rate-limiter vendor/distributed-rate-limiter
RUN mvn -q -B -f vendor/distributed-rate-limiter/pom.xml \
        -pl rate-limiter-spring-boot-starter -am install -DskipTests

COPY pom.xml .
COPY gateway/pom.xml gateway/
COPY gateway/src gateway/src
RUN mvn -q -B -f gateway/pom.xml package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/gateway/target/gateway-0.1.0.jar app.jar
EXPOSE 8080
# 256MB heap so stage s0 reaches OOM in minutes rather than an hour.
ENV JAVA_OPTS="-Xmx256m -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
```

- [x] **Step 2: Verify the image builds**

```bash
docker build -f gateway/Dockerfile -t overload-lab/gateway:dev .
```

Expected: build succeeds. The starter install step must print no errors.

**Step 2b: Boot smoke test (addition beyond the plan)**

The gateway had never actually been started before this task, only compiled and packaged. Its `application.yml` resolves `postgres`, `redis`, and `downstream-sim` by hostname, so this test runs everything on one throwaway Docker network using exactly those container names as network aliases, as a first real boot check before the compose stack (Task 14) exists.

```bash
docker rm -f gw-smoke pg-smoke redis-smoke sim-smoke 2>/dev/null
docker network rm overload-smoke 2>/dev/null
docker network create overload-smoke

docker run -d --name pg-smoke --network overload-smoke --network-alias postgres \
  -e POSTGRES_DB=overload -e POSTGRES_USER=overload -e POSTGRES_PASSWORD=overload \
  -v "$PWD/db/init.sql:/docker-entrypoint-initdb.d/init.sql:ro" postgres:16-alpine
docker run -d --name redis-smoke --network overload-smoke --network-alias redis redis:7-alpine
docker run -d --name sim-smoke --network overload-smoke --network-alias downstream-sim \
  overload-lab/downstream-sim:dev

for i in $(seq 1 30); do docker exec pg-smoke pg_isready -U overload >/dev/null 2>&1 && break; sleep 1; done

docker run -d --name gw-smoke --network overload-smoke -p 8080:8080 overload-lab/gateway:dev
for i in $(seq 1 60); do curl -sf localhost:8080/actuator/health >/dev/null 2>&1 && break; sleep 2; done
```

Then verify: `/actuator/health` is `UP`; `/actuator/overload` shows all four protections (`timeouts`, `boundedQueue`, `admission`, `breaker`) `false` at stage s0, with `hikariMaxPoolSize: 20`; `cat /proc/1/cmdline` inside the container shows `-Xmx256m -XX:+ExitOnOutOfMemoryError` actually applied; `POST /events` with a non-empty batch returns `202` and the row lands in Postgres (`SELECT count(*) FROM events;`); `POST /events` with an empty batch returns `400`; and every metric name the Grafana dashboard (Task 16) and capture script (Task 18) depend on is present in `/actuator/prometheus`: `overload_events_accepted_total`, `overload_events_rejected_total`, `overload_event_e2e_seconds_bucket`, `overload_event_e2e_seconds_count`, `overload_queue_depth`, `overload_queue_capacity`, `executor_active_threads`, `hikaricp_connections_active`, `hikaricp_connections_pending`, `overload_http_pool_leased`, `overload_http_pool_pending`, `overload_http_pool_available`, `jvm_memory_used_bytes`.

Ran once during Task 13: all checks passed on the first boot (Spring Boot started in ~2s; health `UP`; all four protections `false`; heap flags confirmed on PID 1; `POST /events` → 202 with the row visible in Postgres; empty batch → 400; all thirteen metric names present). Clean up afterward:

```bash
docker rm -f gw-smoke pg-smoke redis-smoke sim-smoke
docker network rm overload-smoke
```

- [x] **Step 3: Commit**

```bash
git add gateway/Dockerfile
git commit -m "build(gateway): multi-stage Dockerfile building the pinned starter"
```

---

### Task 14: Application compose stack

**Files:**
- Create: `compose/app.yml`

- [x] **Step 1: Create `compose/app.yml`**

CPU and memory limits are not incidental — they make the saturation point a property of the configuration rather than of whatever else the laptop is doing. k6 deliberately runs on the host, outside this budget, so the load generator never competes with the system under test.

```yaml
name: overload-lab

networks:
  overload:
    name: overload-lab

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: overload
      POSTGRES_USER: overload
      POSTGRES_PASSWORD: overload
    volumes:
      - ../db/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    ports: ["5432:5432"]
    networks: [overload]
    deploy:
      resources:
        limits: { cpus: "1.0", memory: 512M }
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U overload"]
      interval: 2s
      timeout: 2s
      retries: 30

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
    networks: [overload]
    deploy:
      resources:
        limits: { cpus: "0.5", memory: 256M }
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 2s
      retries: 30

  downstream-sim:
    build:
      context: ..
      dockerfile: downstream-sim/Dockerfile
    ports: ["9090:9090"]
    networks: [overload]
    deploy:
      resources:
        limits: { cpus: "1.0", memory: 512M }
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:9090/control || exit 1"]
      interval: 2s
      timeout: 2s
      retries: 30

  gateway:
    build:
      context: ..
      dockerfile: gateway/Dockerfile
    ports: ["8080:8080"]
    networks: [overload]
    depends_on:
      postgres: { condition: service_healthy }
      redis: { condition: service_healthy }
      downstream-sim: { condition: service_healthy }
    environment:
      JAVA_OPTS: "-Xmx256m -XX:+ExitOnOutOfMemoryError"
      OVERLOAD_WORKERS: "${OVERLOAD_WORKERS:-64}"
      OVERLOAD_HIKARI_MAX: "${OVERLOAD_HIKARI_MAX:-20}"
      OVERLOAD_HTTP_POOL: "${OVERLOAD_HTTP_POOL:-32}"
      OVERLOAD_QUEUE_BOUNDED: "${OVERLOAD_QUEUE_BOUNDED:-false}"
      OVERLOAD_QUEUE_CAPACITY: "${OVERLOAD_QUEUE_CAPACITY:-5000}"
      OVERLOAD_TIMEOUTS_ENABLED: "${OVERLOAD_TIMEOUTS_ENABLED:-false}"
      OVERLOAD_CONNECT_MS: "${OVERLOAD_CONNECT_MS:-500}"
      OVERLOAD_RESPONSE_MS: "${OVERLOAD_RESPONSE_MS:-250}"
      OVERLOAD_LEASE_MS: "${OVERLOAD_LEASE_MS:-100}"
      OVERLOAD_ADMISSION_ENABLED: "${OVERLOAD_ADMISSION_ENABLED:-false}"
      OVERLOAD_ADMISSION_LIMIT: "${OVERLOAD_ADMISSION_LIMIT:-40}"
      OVERLOAD_ADMISSION_WINDOW: "${OVERLOAD_ADMISSION_WINDOW:-1}"
      OVERLOAD_BREAKER_ENABLED: "${OVERLOAD_BREAKER_ENABLED:-false}"
    # No restart policy: stage s0 is expected to die of OOM, and that death is data.
    restart: "no"
    deploy:
      resources:
        limits: { cpus: "2.0", memory: 512M }
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8080/actuator/health | grep -q UP"]
      interval: 2s
      timeout: 2s
      retries: 60
```

- [x] **Step 2: Bring the stack up and verify end to end**

```bash
docker compose -f compose/app.yml up -d --build
sleep 45
docker compose -f compose/app.yml ps
curl -s localhost:8080/actuator/overload | python3 -m json.tool
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"batchId":"b1","events":[{"eventId":"e1","payload":"hello"}]}'
docker compose -f compose/app.yml exec -T postgres psql -U overload -d overload -c 'SELECT count(*) FROM events;'
```

Expected: all services healthy; `/actuator/overload` shows all four protections `false` and `hikariMaxPoolSize: 20`; POST returns `202`; the row count is `1`.

- [x] **Step 3: Commit**

```bash
git add compose/app.yml
git commit -m "build: application compose stack with explicit CPU and memory limits"
```

---

### Task 15: Observability stack

**Files:**
- Create: `prometheus/prometheus.yml`
- Create: `compose/obs.yml`
- Create: `grafana/provisioning/datasources/prometheus.yml`
- Create: `grafana/provisioning/dashboards/dashboards.yml`

- [x] **Step 1: Create `prometheus/prometheus.yml`**

The 1-second interval is load-bearing. At Prometheus's default 15s, a per-second admission sawtooth is averaged into a flat line and you would conclude the rate limiter was smooth. Do not raise this.

```yaml
global:
  scrape_interval: 1s
  scrape_timeout: 900ms
  evaluation_interval: 15s

scrape_configs:
  - job_name: gateway
    metrics_path: /actuator/prometheus
    dns_sd_configs:
      - names: ["gateway"]
        type: A
        port: 8080
        refresh_interval: 5s
```

- [x] **Step 2: Create `compose/obs.yml`**

```yaml
name: overload-lab-obs

networks:
  overload:
    external: true
    name: overload-lab

services:
  prometheus:
    image: prom/prometheus:v2.54.1
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.retention.time=6h
      - --web.enable-admin-api
    volumes:
      - ../prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports: ["9091:9090"]
    networks: [overload]

  grafana:
    image: grafana/grafana:11.2.0
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
    volumes:
      - ../grafana/provisioning:/etc/grafana/provisioning:ro
      - ../grafana/dashboards:/var/lib/grafana/dashboards:ro
    ports: ["3000:3000"]
    networks: [overload]
    depends_on: [prometheus]
```

- [x] **Step 3: Create `grafana/provisioning/datasources/prometheus.yml`**

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

- [x] **Step 4: Create `grafana/provisioning/dashboards/dashboards.yml`**

```yaml
apiVersion: 1
providers:
  - name: overload-lab
    orgId: 1
    folder: ""
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    options:
      path: /var/lib/grafana/dashboards
```

- [x] **Step 5: Verify Prometheus is scraping**

```bash
docker compose -f compose/obs.yml up -d
sleep 20
curl -s 'http://localhost:9091/api/v1/targets' | python3 -c "import sys,json; [print(t['labels'],t['health']) for t in json.load(sys.stdin)['data']['activeTargets']]"
curl -s 'http://localhost:9091/api/v1/query?query=overload_queue_depth' | python3 -m json.tool | head -20
```

Expected: one target with health `up`; the query returns a result with value `0`.

- [x] **Step 6: Commit**

```bash
git add prometheus compose/obs.yml grafana/provisioning
git commit -m "feat(obs): Prometheus at 1s scrape and provisioned Grafana"
```

---

### Task 16: Grafana dashboard

**Files:**
- Create: `grafana/dashboards/overload.json`

- [x] **Step 1: Create `grafana/dashboards/overload.json`**

Panel 1 places both latency families on one axis. That single panel carries the stage-0 argument: the edge line stays flat and healthy while the end-to-end line climbs without bound.

```json
{
  "uid": "overload-lab",
  "title": "Overload Lab",
  "tags": ["overload"],
  "timezone": "browser",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "5s",
  "time": { "from": "now-15m", "to": "now" },
  "panels": [
    {
      "id": 1,
      "type": "timeseries",
      "title": "Latency: edge vs end-to-end (p50/p99/p999)",
      "description": "The edge lies. HTTP latency is flat because /events returns 202 on enqueue; e2e is the truth.",
      "gridPos": { "h": 9, "w": 24, "x": 0, "y": 0 },
      "fieldConfig": { "defaults": { "unit": "s", "custom": { "fillOpacity": 0 } }, "overrides": [] },
      "targets": [
        { "refId": "A", "expr": "histogram_quantile(0.50, sum(rate(http_server_requests_seconds_bucket{uri=\"/events\"}[10s])) by (le))", "legendFormat": "edge p50" },
        { "refId": "B", "expr": "histogram_quantile(0.99, sum(rate(http_server_requests_seconds_bucket{uri=\"/events\"}[10s])) by (le))", "legendFormat": "edge p99" },
        { "refId": "C", "expr": "histogram_quantile(0.50, sum(rate(overload_event_e2e_seconds_bucket[10s])) by (le))", "legendFormat": "e2e p50" },
        { "refId": "D", "expr": "histogram_quantile(0.99, sum(rate(overload_event_e2e_seconds_bucket[10s])) by (le))", "legendFormat": "e2e p99" },
        { "refId": "E", "expr": "histogram_quantile(0.999, sum(rate(overload_event_e2e_seconds_bucket[10s])) by (le))", "legendFormat": "e2e p999" }
      ]
    },
    {
      "id": 2,
      "type": "timeseries",
      "title": "Accepted throughput (events/s)",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 9 },
      "fieldConfig": { "defaults": { "unit": "ops" }, "overrides": [] },
      "targets": [
        { "refId": "A", "expr": "sum(rate(overload_events_accepted_total[10s]))", "legendFormat": "accepted" }
      ]
    },
    {
      "id": 3,
      "type": "timeseries",
      "title": "Rejections by reason (events/s)",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 9 },
      "fieldConfig": { "defaults": { "unit": "ops", "custom": { "fillOpacity": 40, "stacking": { "mode": "normal" } } }, "overrides": [] },
      "targets": [
        { "refId": "A", "expr": "sum by (reason) (rate(overload_events_rejected_total[10s]))", "legendFormat": "{{reason}}" }
      ]
    },
    {
      "id": 4,
      "type": "timeseries",
      "title": "Queue depth",
      "description": "At s0 this grows linearly at (arrival - drain) until the heap is gone.",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 17 },
      "fieldConfig": { "defaults": { "unit": "short" }, "overrides": [] },
      "targets": [
        { "refId": "A", "expr": "overload_queue_depth", "legendFormat": "depth" },
        { "refId": "B", "expr": "overload_queue_capacity > 0", "legendFormat": "capacity" }
      ]
    },
    {
      "id": 5,
      "type": "timeseries",
      "title": "Worker threads active",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 17 },
      "fieldConfig": { "defaults": { "unit": "short" }, "overrides": [] },
      "targets": [
        { "refId": "A", "expr": "executor_active_threads{name=\"overload.workers\"}", "legendFormat": "active" },
        { "refId": "B", "expr": "executor_pool_size_threads{name=\"overload.workers\"}", "legendFormat": "pool size" }
      ]
    },
    {
      "id": 6,
      "type": "timeseries",
      "title": "Connection pools: Hikari vs HTTP",
      "description": "Hikari pending pegs at (workers - poolSize) and stays flat. Postgres is idle. The pool is the constraint, not the database.",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 25 },
      "fieldConfig": { "defaults": { "unit": "short" }, "overrides": [] },
      "targets": [
        { "refId": "A", "expr": "hikaricp_connections_active{pool=\"gateway-pool\"}", "legendFormat": "hikari active" },
        { "refId": "B", "expr": "hikaricp_connections_pending{pool=\"gateway-pool\"}", "legendFormat": "hikari pending" },
        { "refId": "C", "expr": "overload_http_pool_leased", "legendFormat": "http leased" },
        { "refId": "D", "expr": "overload_http_pool_pending", "legendFormat": "http pending" }
      ]
    },
    {
      "id": 7,
      "type": "timeseries",
      "title": "JVM heap used",
      "description": "The actual cause of death at s0.",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 25 },
      "fieldConfig": { "defaults": { "unit": "bytes" }, "overrides": [] },
      "targets": [
        { "refId": "A", "expr": "sum(jvm_memory_used_bytes{area=\"heap\"})", "legendFormat": "heap used" },
        { "refId": "B", "expr": "sum(jvm_memory_max_bytes{area=\"heap\"})", "legendFormat": "heap max" }
      ]
    }
  ]
}
```

- [x] **Step 2: Verify the dashboard provisions**

```bash
docker compose -f compose/obs.yml restart grafana
sleep 15
curl -s -u admin:admin http://localhost:3000/api/dashboards/uid/overload-lab | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['dashboard']['title'], len(d['dashboard']['panels']), 'panels')"
```

Expected: `Overload Lab 7 panels`.

- [x] **Step 3: Commit**

```bash
git add grafana/dashboards/overload.json
git commit -m "feat(obs): Grafana dashboard with edge-vs-e2e latency panel"
```

---

### Task 17: k6 load scripts

**Files:**
- Create: `k6/steady.js`
- Create: `k6/knee.js`

- [x] **Step 1: Create `k6/steady.js`**

`constant-arrival-rate` is an **open** model and is not negotiable. A closed model (fixed VUs waiting on responses) self-throttles the moment the system slows, which would hide the entire phenomenon this repo exists to demonstrate.

```javascript
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const RATE = Number(__ENV.RATE || 40);
const DURATION = __ENV.DURATION || '5m';
const BATCH = Number(__ENV.BATCH || 20);
const PAYLOAD_BYTES = Number(__ENV.PAYLOAD_BYTES || 1024);
const BASE = __ENV.BASE_URL || 'http://localhost:8080';

const accepted = new Counter('batches_accepted');
const shed = new Counter('batches_shed');
const failed = new Counter('batches_failed');

const PAD = 'x'.repeat(PAYLOAD_BYTES);

export const options = {
  // k6's default summaryTrendStats omits p(99) entirely (avg,min,med,max,p90,p95).
  // This project measures p50/p99/p999, and RESULTS.md commits client-side percentiles
  // alongside Prometheus server-side ones, so they must be requested explicitly.
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(99)', 'p(99.9)', 'max'],
  discardResponseBodies: true,
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.max(50, RATE * 2),
      maxVUs: Math.max(500, RATE * 20),
gracefulStop: '30s',
    },
  },
};

export default function () {
  const batchId = `b-${__VU}-${__ITER}`;
  const events = [];
  for (let i = 0; i < BATCH; i++) {
    events.push({ eventId: `${batchId}-${i}`, payload: PAD });
  }

  const res = http.post(`${BASE}/events`, JSON.stringify({ batchId, events }), {
    headers: { 'Content-Type': 'application/json' },
    timeout: '120s',
  });

  if (res.status === 202) accepted.add(1);
  else if (res.status === 429) shed.add(1);
  else failed.add(1);

  check(res, { 'not 5xx': (r) => r.status < 500 || r.status === 0 });
}

export function handleSummary(data) {
  // A Counter that never received a data point is omitted from the summary entirely
  // rather than reported as 0. Seed the known ones so every committed run has the
  // same shape and "absent" never has to be interpreted as "zero" downstream.
  for (const k of ['batches_accepted', 'batches_shed', 'batches_failed']) {
    if (!data.metrics[k]) {
      data.metrics[k] = { type: 'counter', contains: 'default', values: { count: 0, rate: 0 } };
    }
  }
  const out = __ENV.SUMMARY_OUT || 'summary.json';
  return { [out]: JSON.stringify(data, null, 2), stdout: '' };
}
```

- [x] **Step 2: Create `k6/knee.js`**

```javascript
import http from 'k6/http';

const BATCH = Number(__ENV.BATCH || 20);
const PAYLOAD_BYTES = Number(__ENV.PAYLOAD_BYTES || 1024);
const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const PAD = 'x'.repeat(PAYLOAD_BYTES);

export const options = {
  // k6's default summaryTrendStats omits p(99) entirely (avg,min,med,max,p90,p95).
  // This project measures p50/p99/p999, and RESULTS.md commits client-side percentiles
  // alongside Prometheus server-side ones, so they must be requested explicitly.
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(99)', 'p(99.9)', 'max'],
  discardResponseBodies: true,
  scenarios: {
    knee: {
      executor: 'ramping-arrival-rate',
      startRate: 5,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 2000,
      stages: [
        { target: 10, duration: '100s' },
        { target: 20, duration: '100s' },
        { target: 30, duration: '100s' },
        { target: 40, duration: '100s' },
        { target: 55, duration: '100s' },
        { target: 80, duration: '100s' },
      ],
    },
  },
};

export default function () {
  const batchId = `k-${__VU}-${__ITER}`;
  const events = [];
  for (let i = 0; i < BATCH; i++) {
    events.push({ eventId: `${batchId}-${i}`, payload: PAD });
  }
  http.post(`${BASE}/events`, JSON.stringify({ batchId, events }), {
    headers: { 'Content-Type': 'application/json' },
    timeout: '120s',
  });
}

export function handleSummary(data) {
  const out = __ENV.SUMMARY_OUT || 'knee-summary.json';
  return { [out]: JSON.stringify(data, null, 2), stdout: '' };
}
```

- [x] **Step 3: Smoke test at a rate well below capacity**

```bash
mkdir -p results/smoke
k6 run -e RATE=5 -e DURATION=30s -e SUMMARY_OUT=results/smoke/k6-summary.json k6/steady.js
python3 -c "
import json; d=json.load(open('results/smoke/k6-summary.json'))
print('batches accepted:', d['metrics'].get('batches_accepted',{}).get('count'))
print('http p99 ms:', round(d['metrics']['http_req_duration']['values']['p(99)'],1))
"
```

Expected: ~150 batches accepted, no shed, p99 in the low tens of milliseconds.

- [x] **Step 4: Commit**

```bash
git add k6
git commit -m "feat(k6): open-model steady and knee-finding load scripts"
```

---

### Task 18: Capture tooling

**Files:**
- Create: `tools/capture/Dockerfile`
- Create: `tools/capture/capture.py`

Everything in RESULTS.md must be regenerable by re-running a stage. Capture runs in a container so a clean clone needs no host Python dependencies.

- [ ] **Step 1: Create `tools/capture/Dockerfile`**

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir requests==2.32.3 matplotlib==3.9.2
WORKDIR /app
COPY capture.py .
ENTRYPOINT ["python", "/app/capture.py"]
```

- [ ] **Step 2: Create `tools/capture/capture.py`**

```python
#!/usr/bin/env python3
"""Query the Prometheus range API for one run and emit raw series plus PNGs.

Usage:
  capture.py --prom http://prometheus:9090 --start <epoch> --end <epoch> \
             --step 1 --outdir /out/<stage> --imgdir /img --label s0
"""
import argparse
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import requests

QUERIES = {
    "edge_p50":      'histogram_quantile(0.50, sum(rate(http_server_requests_seconds_bucket{uri="/events"}[10s])) by (le))',
    "edge_p99":      'histogram_quantile(0.99, sum(rate(http_server_requests_seconds_bucket{uri="/events"}[10s])) by (le))',
    "e2e_p50":       'histogram_quantile(0.50, sum(rate(overload_event_e2e_seconds_bucket[10s])) by (le))',
    "e2e_p99":       'histogram_quantile(0.99, sum(rate(overload_event_e2e_seconds_bucket[10s])) by (le))',
    "e2e_p999":      'histogram_quantile(0.999, sum(rate(overload_event_e2e_seconds_bucket[10s])) by (le))',
    "accepted":      'sum(rate(overload_events_accepted_total[10s]))',
    "rejected":      'sum by (reason) (rate(overload_events_rejected_total[10s]))',
    "queue_depth":   'overload_queue_depth',
    "workers_active":'executor_active_threads{name="overload.workers"}',
    "hikari_active": 'hikaricp_connections_active{pool="gateway-pool"}',
    "hikari_pending":'hikaricp_connections_pending{pool="gateway-pool"}',
    "http_leased":   'overload_http_pool_leased',
    "http_pending":  'overload_http_pool_pending',
    "heap_used":     'sum(jvm_memory_used_bytes{area="heap"})',
}


def query_range(prom, expr, start, end, step):
    r = requests.get(f"{prom}/api/v1/query_range",
                     params={"query": expr, "start": start, "end": end, "step": step},
                     timeout=60)
    r.raise_for_status()
    return r.json()["data"]["result"]


def series_of(result):
    """Return [(label, xs, ys)] with xs as seconds elapsed from the first sample."""
    out = []
    for s in result:
        vals = s.get("values", [])
        if not vals:
            continue
        t0 = float(vals[0][0])
        label = s["metric"].get("reason") or s["metric"].get("pool") or ""
        xs = [float(t) - t0 for t, _ in vals]
        ys = [float("nan") if v in ("NaN", "+Inf") else float(v) for _, v in vals]
        out.append((label, xs, ys))
    return out


def plot(path, title, ylabel, named_series, logy=False):
    fig, ax = plt.subplots(figsize=(11, 4.5))
    plotted = False
    for name, result in named_series:
        for label, xs, ys in series_of(result):
            ax.plot(xs, ys, linewidth=1.3, label=f"{name} {label}".strip())
            plotted = True
    if not plotted:
        plt.close(fig)
        return False
    if logy:
        ax.set_yscale("log")
    ax.set_title(title)
    ax.set_xlabel("seconds into run")
    ax.set_ylabel(ylabel)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8, ncol=3)
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--prom", required=True)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--step", default="1")
    p.add_argument("--outdir", required=True)
    p.add_argument("--imgdir", required=True)
    p.add_argument("--label", required=True)
    a = p.parse_args()

    os.makedirs(a.outdir, exist_ok=True)
    os.makedirs(a.imgdir, exist_ok=True)

    raw = {}
    for name, expr in QUERIES.items():
        try:
            raw[name] = query_range(a.prom, expr, a.start, a.end, a.step)
        except Exception as exc:  # a stage that OOMed loses its target; that is data
            print(f"WARN {name}: {exc}")
            raw[name] = []

    with open(os.path.join(a.outdir, "series.json"), "w") as fh:
        json.dump({"label": a.label, "queries": QUERIES, "data": raw}, fh)

    charts = [
        ("latency", "Latency: edge vs end-to-end", "seconds",
         [("edge p50", raw["edge_p50"]), ("edge p99", raw["edge_p99"]),
          ("e2e p50", raw["e2e_p50"]), ("e2e p99", raw["e2e_p99"]),
          ("e2e p999", raw["e2e_p999"])], True),
        ("throughput", "Accepted throughput and rejections", "events/s",
         [("accepted", raw["accepted"]), ("rejected", raw["rejected"])], False),
        ("queue-heap", "Queue depth and JVM heap", "count / bytes",
         [("queue depth", raw["queue_depth"]), ("heap used", raw["heap_used"])], True),
        ("pools", "Pools and workers", "count",
         [("hikari active", raw["hikari_active"]), ("hikari pending", raw["hikari_pending"]),
          ("http leased", raw["http_leased"]), ("http pending", raw["http_pending"]),
          ("workers active", raw["workers_active"])], False),
    ]

    for slug, title, ylabel, named, logy in charts:
        path = os.path.join(a.imgdir, f"{a.label}-{slug}.png")
        if plot(path, f"[{a.label}] {title}", ylabel, named, logy):
            print(f"wrote {path}")

    print(f"wrote {os.path.join(a.outdir, 'series.json')}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Build the capture image**

```bash
docker build -t overload-lab/capture:dev tools/capture
```

Expected: image builds.

- [ ] **Step 4: Commit**

```bash
git add tools/capture
git commit -m "feat(capture): Prometheus range capture to committed series and PNGs"
```

---

### Task 19: Stage files and the run harness

**Files:**
- Create: `stages/s0.env` … `stages/s4.env`
- Create: `scripts/run-stage.sh`

- [ ] **Step 1: Create the five stage files**

Stages are cumulative. `s1`–`s4` are defined now so the harness is complete; Phase 1 implements and measures them.

`stages/s0.env`:
```bash
STAGE_NAME=s0-fragile
OVERLOAD_TIMEOUTS_ENABLED=false
OVERLOAD_QUEUE_BOUNDED=false
OVERLOAD_ADMISSION_ENABLED=false
OVERLOAD_BREAKER_ENABLED=false
```

`stages/s1.env`:
```bash
STAGE_NAME=s1-timeouts
OVERLOAD_TIMEOUTS_ENABLED=true
OVERLOAD_CONNECT_MS=500
OVERLOAD_RESPONSE_MS=250
OVERLOAD_LEASE_MS=100
OVERLOAD_QUEUE_BOUNDED=false
OVERLOAD_ADMISSION_ENABLED=false
OVERLOAD_BREAKER_ENABLED=false
```

`stages/s2.env`:
```bash
STAGE_NAME=s2-bounded-queue
OVERLOAD_TIMEOUTS_ENABLED=true
OVERLOAD_CONNECT_MS=500
OVERLOAD_RESPONSE_MS=250
OVERLOAD_LEASE_MS=100
OVERLOAD_QUEUE_BOUNDED=true
OVERLOAD_QUEUE_CAPACITY=5000
OVERLOAD_ADMISSION_ENABLED=false
OVERLOAD_BREAKER_ENABLED=false
```

`stages/s3.env`:
```bash
STAGE_NAME=s3-admission
OVERLOAD_TIMEOUTS_ENABLED=true
OVERLOAD_CONNECT_MS=500
OVERLOAD_RESPONSE_MS=250
OVERLOAD_LEASE_MS=100
OVERLOAD_QUEUE_BOUNDED=true
OVERLOAD_QUEUE_CAPACITY=5000
OVERLOAD_ADMISSION_ENABLED=true
OVERLOAD_ADMISSION_LIMIT=40
OVERLOAD_ADMISSION_WINDOW=1
OVERLOAD_BREAKER_ENABLED=false
```

`stages/s4.env`:
```bash
STAGE_NAME=s4-breaker
OVERLOAD_TIMEOUTS_ENABLED=true
OVERLOAD_CONNECT_MS=500
OVERLOAD_RESPONSE_MS=250
OVERLOAD_LEASE_MS=100
OVERLOAD_QUEUE_BOUNDED=true
OVERLOAD_QUEUE_CAPACITY=5000
OVERLOAD_ADMISSION_ENABLED=true
OVERLOAD_ADMISSION_LIMIT=40
OVERLOAD_ADMISSION_WINDOW=1
OVERLOAD_BREAKER_ENABLED=true
```

- [ ] **Step 2: Create `scripts/run-stage.sh`**

The preflight assertion is the guard that makes every number in RESULTS.md citable. A stage flag that silently fails to take effect would produce a confident, wrong "no measurable delta" — the single worst outcome this project can have.

```bash
#!/usr/bin/env bash
set -euo pipefail

STAGE="${1:?usage: run-stage.sh <s0|s1|s2|s3|s4> [profile]}"
PROFILE="${2:-A}"
RATE="${RATE:-40}"
DURATION="${DURATION:-5m}"
WARMUP="${WARMUP:-60}"
BATCH="${BATCH:-20}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# shellcheck disable=SC1090
set -a; source "stages/${STAGE}.env"; set +a

RUN_ID="${STAGE_NAME}-${PROFILE}-rate${RATE}"
OUTDIR="results/${RUN_ID}"
mkdir -p "$OUTDIR" docs/img

echo "==> [${RUN_ID}] recreating gateway"
docker compose -f compose/app.yml up -d --force-recreate --no-deps gateway

echo "==> waiting for health"
for i in $(seq 1 90); do
  if curl -sf localhost:8080/actuator/health | grep -q UP; then break; fi
  sleep 2
  if [ "$i" = 90 ]; then echo "gateway never became healthy"; exit 1; fi
done

echo "==> preflight: asserting active protections match ${STAGE}.env"
curl -sf localhost:8080/actuator/overload > "$OUTDIR/preflight.json"
python3 - "$OUTDIR/preflight.json" <<'PY'
import json, os, sys
actual = json.load(open(sys.argv[1]))["protections"]
expected = {
    "timeouts":     os.environ.get("OVERLOAD_TIMEOUTS_ENABLED", "false") == "true",
    "boundedQueue": os.environ.get("OVERLOAD_QUEUE_BOUNDED", "false") == "true",
    "admission":    os.environ.get("OVERLOAD_ADMISSION_ENABLED", "false") == "true",
    "breaker":      os.environ.get("OVERLOAD_BREAKER_ENABLED", "false") == "true",
}
bad = {k: (expected[k], actual.get(k)) for k in expected if expected[k] != actual.get(k)}
if bad:
    print("PREFLIGHT FAILED (expected, actual):", bad)
    sys.exit(1)
print("preflight OK:", actual)
PY

echo "==> resetting downstream knobs to healthy"
curl -sf -X POST localhost:9090/control -H 'Content-Type: application/json' \
  -d '{"latencyMs":25,"jitterMs":10,"failureRate":0,"mode":"NORMAL"}' > /dev/null

echo "==> warmup ${WARMUP}s at rate ${RATE} (discarded)"
k6 run --quiet -e RATE="$RATE" -e DURATION="${WARMUP}s" -e BATCH="$BATCH" \
  -e SUMMARY_OUT=/dev/null k6/steady.js > /dev/null 2>&1 || true

START=$(date +%s)
echo "==> measuring ${DURATION} at rate ${RATE}"
set +e
k6 run -e RATE="$RATE" -e DURATION="$DURATION" -e BATCH="$BATCH" \
  -e SUMMARY_OUT="$OUTDIR/k6-summary.json" k6/steady.js
K6_RC=$?
set -e
END=$(date +%s)

STATE=$(docker inspect -f '{{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' overload-lab-gateway-1 2>/dev/null || echo unknown)
echo "==> gateway container state: ${STATE}"
echo "$STATE" > "$OUTDIR/container-state.txt"
echo "k6 exit=${K6_RC}" >> "$OUTDIR/container-state.txt"

echo "==> capturing Prometheus series and rendering PNGs"
docker run --rm --network overload-lab \
  -v "$ROOT/results:/out" -v "$ROOT/docs/img:/img" \
  overload-lab/capture:dev \
  --prom http://prometheus:9090 --start "$START" --end "$END" --step 1 \
  --outdir "/out/${RUN_ID}" --imgdir /img --label "$RUN_ID"

echo "==> done: $OUTDIR"
```

- [ ] **Step 3: Make it executable and run stage s0 below capacity**

```bash
chmod +x scripts/run-stage.sh
docker compose -f compose/app.yml up -d --build
docker compose -f compose/obs.yml up -d
RATE=10 DURATION=90s WARMUP=20 ./scripts/run-stage.sh s0
ls -la results/s0-fragile-A-rate10/ docs/img/
```

Expected: preflight prints `preflight OK` with all four `False`; `k6-summary.json`, `series.json`, `preflight.json` written; four PNGs in `docs/img/`.

- [ ] **Step 4: Verify the preflight actually catches a mismatch**

This proves the guard works rather than assuming it.

```bash
OVERLOAD_QUEUE_BOUNDED=true ./scripts/run-stage.sh s0 || echo "EXPECTED FAILURE - guard works"
```

Expected: the run aborts with `PREFLIGHT FAILED` before any load is generated. Then re-run Step 3 to leave the stack in a known state.

- [ ] **Step 5: Commit**

```bash
git add stages scripts/run-stage.sh
git commit -m "feat(harness): stage files and run-stage.sh with preflight assertion"
```

---

### Task 20: Calibration — find the knee and produce the collapse

**Files:**
- Create: `RESULTS.md`
- Modify: `docs/superpowers/specs/2026-09-03-overload-lab-design.md` (calibration table)

- [ ] **Step 1: Run the knee-finding sweep**

```bash
docker compose -f compose/app.yml up -d --force-recreate gateway
sleep 30
mkdir -p results/knee
KNEE_START=$(date +%s)
k6 run -e SUMMARY_OUT=results/knee/k6-summary.json k6/knee.js
KNEE_END=$(date +%s)
docker run --rm --network overload-lab -v "$PWD/results:/out" -v "$PWD/docs/img:/img" \
  overload-lab/capture:dev --prom http://prometheus:9090 \
  --start "$KNEE_START" --end "$KNEE_END" --step 1 --outdir /out/knee --imgdir /img --label knee
```

- [ ] **Step 2: Compute the knee from the captured series**

Applying the spec's definition: the lowest arrival rate at which e2e p99 exceeds 2x its low-load baseline, sustained 30s.

```bash
python3 - <<'PY'
import json
d = json.load(open('results/knee/series.json'))
vals = d['data']['e2e_p99'][0]['values']
pts = [(float(t), float(v)) for t, v in vals if v not in ('NaN', '+Inf')]
t0 = pts[0][0]
baseline = sorted(p[1] for p in pts[:60])[30]   # median of the first minute
thresh = 2 * baseline
print(f"baseline e2e p99 = {baseline*1000:.1f} ms; threshold = {thresh*1000:.1f} ms")
run = 0
for t, v in pts:
    run = run + 1 if v > thresh else 0
    if run >= 30:
        elapsed = t - t0
        stage_rates = [(100,10),(200,20),(300,30),(400,40),(500,55),(600,80)]
        rate = next((r for cutoff, r in stage_rates if elapsed <= cutoff), 80)
        print(f"KNEE at ~{elapsed:.0f}s into ramp -> approx {rate} rps ({rate*20} events/s)")
        break
else:
    print("no knee found; raise the ramp ceiling in k6/knee.js")
PY
```

Expected: a knee near 40 rps / 800 events/s. **Record the measured number — every later run is a multiple of it, not of the prediction.**

- [ ] **Step 3: Run the collapse at 1x, 2x and 4x the measured knee**

Substitute the measured knee for `40` below if it differs.

```bash
KNEE=40
for mult in 1 2 4; do
  RATE=$((KNEE * mult)) DURATION=6m WARMUP=45 ./scripts/run-stage.sh s0
  echo "--- multiple ${mult}x done ---"
done
grep -h . results/s0-fragile-A-rate*/container-state.txt
```

Expected: 1x survives; 2x and 4x end with the gateway container `exited` and a non-zero exit code from `ExitOnOutOfMemoryError`. Time-to-death should be shorter at 4x.

If neither 2x nor 4x dies within the run, the calibration is off: reduce `OVERLOAD_HIKARI_MAX` or raise `PAYLOAD_BYTES` so queue growth outpaces the heap sooner, then re-run.

- [ ] **Step 4: Verify the central claim — Postgres was idle while the pool saturated**

```bash
docker compose -f compose/app.yml exec -T postgres psql -U overload -d overload -c \
  "SELECT count(*) AS rows_written FROM events;"
docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}'
```

Expected: rows written far below what was offered, and Postgres CPU low. This is the evidence for "the pool was the constraint, not the database."

- [ ] **Step 5: Write `RESULTS.md`**

```markdown
# Results

All numbers produced by `./scripts/run-stage.sh <stage>` against the committed
configuration. Every graph is regenerable: re-run the stage.

## Method

- Load is generated by k6 using a **constant-arrival-rate (open) model**. A closed model
  self-throttles when the system slows and would hide the phenomenon entirely.
- Containers carry explicit CPU and memory limits so the saturation point is a property of
  the configuration, not of the host. k6 runs on the host, outside that budget.
- Prometheus scrapes at **1s**. At the default 15s a per-second waveform is averaged flat.
- 60s of warmup is discarded before each measurement window.
- Every run asserts, before generating load, that the protections actually active match the
  stage file (`results/<run>/preflight.json`).
- Rate limiter under test: `sanchit-g/distributed-rate-limiter` pinned at `2789431`.

## Configuration

| Parameter | Value |
|---|---|
| workers | 64 |
| Hikari max pool | 20 |
| HTTP client pool | 32 |
| downstream latency | 25ms +/- 10ms |
| batch size | 20 events x ~1KB |
| gateway heap | 256MB |
| measured knee | _fill in from Task 20 Step 2_ |

## Stage s0 — fragile

_Insert `docs/img/s0-fragile-A-rate*-latency.png` and the queue/heap chart._

Findings to write up from the captured data:

1. The divergence between edge and end-to-end latency, with both numbers.
2. Queue growth rate against the arithmetic prediction of `arrival - drain`.
3. Hikari active and pending, and the fact that pending is **flat** at `workers - poolSize`
   rather than climbing — the pool is saturated but not queueing further.
4. Postgres CPU and rows written, establishing that the database was not the constraint.
5. Time-to-OOM at each load multiple.

| Multiple | Offered (ev/s) | Accepted (ev/s) | Edge p99 | e2e p99 | Time to death |
|---|---|---|---|---|---|
| 1x | | | | | survived |
| 2x | | | | | |
| 4x | | | | | |

## Stages s1–s4

Phase 1.
```

- [ ] **Step 6: Update the spec's calibration table with measured values**

Replace the predicted values in `docs/superpowers/specs/2026-09-03-overload-lab-design.md` under "Calibration targets" with what was actually measured, keeping the predictions in a "predicted" column so the spec records where the arithmetic was right and where it was wrong.

- [ ] **Step 7: Commit**

```bash
git add RESULTS.md results docs/img docs/superpowers/specs
git commit -m "docs: calibrated knee and stage s0 collapse with captured evidence"
```

---

## Phase 0 Definition of Done

- [ ] `docker compose -f compose/app.yml up -d --build` brings up four healthy services from a clean clone
- [ ] `/actuator/overload` reports all four protections `false` at s0
- [ ] The preflight assertion has been shown to fail on a deliberate mismatch
- [ ] The knee has been measured, not assumed, and recorded
- [ ] s0 at 2x and 4x ends in OOM, with time-to-death recorded for each
- [ ] Postgres is demonstrably near-idle at collapse
- [ ] `results/<run>/` holds `k6-summary.json`, `series.json`, `preflight.json`, `container-state.txt` for every run
- [ ] `docs/img/` holds regenerated PNGs
- [ ] RESULTS.md carries the method, the configuration and the s0 table

## Deferred to Phase 1: the HTTP-starved variant

`http-pool-size` is already bound to `${OVERLOAD_HTTP_POOL:32}`, sized above Hikari's 20 so
the HTTP pool cannot become a second constraint. HttpClient 5's own default is
`maxTotal=25, defaultMaxPerRoute=5` (verified against httpclient5-5.3.1) -- had we accepted
it, only 5 workers could be mid-call at once, so only 5 DB connections would ever be held,
Hikari would never saturate, and capacity would land near 200 events/s instead of 800. The
graphs would have shown HTTP connections queueing while Hikari sat half idle: a plausible
result measuring the wrong pool.

Excluding that is experimental control, not a claim that it is unrealistic -- HTTP pool
exhaustion from default settings is more common in production than the DB variant.

So run it deliberately, as a variant of profile A in Phase 1: `OVERLOAD_HTTP_POOL=5`, same
code, same load, same dashboards. Expected: Hikari comfortable, HTTP pending climbing,
capacity ~200 events/s. It costs one stage env file and no code, and it demonstrates the
method rather than a single finding -- change which pool is scarcest and the collapse
relocates exactly where the arithmetic said it would. It also answers the sceptical reader
who assumes the numbers were chosen to produce the desired answer.

## Not in Phase 0

Timeouts (s1), bounded-queue measurement (s2), admission control via the starter (s3), the
circuit breaker (s4), profiles B1/B2/C, the three sweeps, and all eight starter findings.
These belong to the Phase 1 plan.
