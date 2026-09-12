# overload-lab

A reproducible benchmark that makes a write-heavy Spring Boot ingestion service fail in one
specific, explainable way — and measures what each protection against it is actually worth.

The goal is not to survive load. It is to produce a bottleneck you can explain.

## The finding

Each worker takes a database connection, INSERTs, and then calls a downstream service **while
still holding that connection**. Capacity is therefore `poolSize / holdTime` — a ceiling set
by a connection pool rather than by the database. (That the database is not the constraint is
an *inference, not a measurement* — its CPU was never scraped. What supports it is capacity
scaling linearly with pool size all the way to 1,508 ev/s, which a database-bound system
would not do.)

Push past it and the service returns **HTTP 202 to 100% of requests right up until the JVM
dies of heap exhaustion**. Every conventional signal reads healthy while that happens:

| signal | at 2x capacity | |
|---|---|---|
| HTTP p99 at the edge | **1.5 ms** | *down* from 2.0 ms at 1x; it only rises at 4x (16.2 ms), once GC degrades everything |
| HikariCP active / pending | **20 / 46** | pegged flat from the first minute |
| accepted throughput | **1,480 ev/s** | exactly what was offered |
| completed throughput | **719 ev/s** | all the service actually drained — already under the 754 ev/s ceiling, from GC |
| end-to-end p99 | **91 seconds** | |
| queue depth | **180,668** | and climbing |
| time to death | **228 s** | JVM heap, `exit=3` |

The only two lines that move are queue depth and heap. Neither appears on a default Spring
Boot dashboard.

Capacity also scales with the pool: measured 178, 366, 748 and 1,508 ev/s at pool sizes 5, 10,
20 and 40 — `capacity = c / hold_time` to within 5.6% over an eightfold range. Only the
smallest pool demonstrably plateaued, so the larger figures are probably slight
underestimates; [RESULTS.md](RESULTS.md) carries the qualification.

## Quickstart

Requires Docker, Java 21, Maven and k6.

```bash
git clone --recurse-submodules https://github.com/sanchit-g/overload-lab.git
cd overload-lab

docker compose -f compose/app.yml up -d --build   # gateway, downstream-sim, postgres, redis
docker compose -f compose/obs.yml up -d           # prometheus (:9091), grafana (:3000)

RATE=74 DURATION=5m ./scripts/run-stage.sh s0     # 2x capacity — watch it die
```

Grafana is at `localhost:3000`, anonymous access, dashboard **Overload Lab**. Panel 1 is the
one that matters: edge latency and end-to-end latency on a shared axis, diverging by three
orders of magnitude while the service looks healthy.

Each run writes `results/<run>/` — raw Prometheus series, the k6 summary, the preflight
assertion, the container's exit state — and renders PNGs into `docs/img/`.

## What's in here

```
gateway/           the service under test; the bug lives in EventWriter.java
downstream-sim/    dependency simulator with runtime latency/failure knobs
vendor/            distributed-rate-limiter, pinned as a submodule; not yet called from the gateway
compose/           two stacks: app and observability
k6/                open-model load scripts (constant-arrival-rate, not closed)
scripts/           run-stage.sh, run-sweep.sh
tools/             containerised Prometheus capture + assertions
results/           committed raw series per run
```

## How the measurements are kept honest

- **Open-model load.** A closed model self-throttles when the system slows and would hide the
  phenomenon entirely.
- **A preflight assertion** fails the run *before* generating load if the configuration in
  effect does not match what the run intends. Exercisable with `PREFLIGHT_SELFTEST=1`.
- **An offered-rate gate** proves the load generator actually delivered the rate the run
  claims, using the server-side p90 rather than k6's death-distorted average.
- **Capture assertions** fail on ambiguity — more than one series where one is expected, or a
  counter running backwards, which would mean the gateway restarted mid-window.
- **1-second Prometheus scrape.** At the 15s default, per-second waveforms average flat.

## Status

**Done — the fragile baseline.** Collapse characterised at n=3 with randomized run order and
the table truncated between runs. Capacity, the causal chain, GC's contribution, and the
pool-size scaling are measured. See [RESULTS.md](RESULTS.md).

**Not done — the protections.** Client timeouts, a bounded queue, token-bucket admission
control and a circuit breaker are specified and flagged in configuration, but **none has a
measured delta yet**. Timeouts and the bounded queue are implemented behind flags; admission
control and the breaker are not yet wired. That is the next body of work.

**Working notes.** [docs/lab-notebook.md](docs/lab-notebook.md) is the full record — every
batch, every review, every claim that was withdrawn and why. It is deliberately unpolished.
