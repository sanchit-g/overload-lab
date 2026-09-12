# overload-lab — Design

**Date:** 2026-09-03
**Status:** Approved

## Goal

Produce and document a specific, explainable bottleneck in a write-heavy Spring Boot
ingestion service, then reintroduce protections one at a time so each carries a measured
delta. The deliverable is evidence, not a product. The system is not expected to survive
load; it is expected to fail in a way that is understood, reproducible, and explained.

## Non-goals

- Maximising throughput. Every stage tops out at the same goodput by design.
- Production readiness of the gateway. It is an instrument.
- Benchmarking Postgres, Redis, or Spring Boot against alternatives.

## Repository layout

`overload-lab` is a standalone repo. The rate limiter under test is consumed as a pinned
git submodule, which doubles as the working copy where its fixes are developed.

```
overload-lab/
  vendor/distributed-rate-limiter/   submodule, pinned 2789431
  gateway/                           ingestion service under test
  downstream-sim/                    dependency simulator
  compose/app.yml compose/obs.yml
  stages/s0..s4.env
  k6/knee.js k6/steady.js
  prometheus/ grafana/ tools/capture/
  results/<scenario>/                k6 summary + Prometheus series
  docs/img/                          generated PNGs
  RESULTS.md
```

The starter stays Java 17 (library reach) and moves to Spring Boot 3.3.x (3.2.1 is off the
supported line). The lab runs Java 21 / Boot 3.3.x. A Boot 3.2/3.3 starter resolves cleanly
inside the lab since it depends only on `spring-boot-starter`, `-aop`, `-data-redis`,
all version-managed by the consumer. `@RateLimit` remains backward compatible throughout.

## The write path and the bottleneck

```
POST /events -> [admission filter, s3] -> validate -> enqueue N -> 202 Accepted
                                                          |
                                                    [work queue]
                                                          |
                                            worker (1 of 64 platform threads)
                                              conn = hikari.get()     <- max 20
                                              INSERT INTO events
                                              POST downstream-sim     <- no timeout at s0
                                              commit / close
```

A Hikari connection is held across a network call. Capacity is therefore
`hikariMax / downstreamLatency` = `20 / 25ms` ~= **800 events/s**, governed by a pool that
has nothing to do with how fast Postgres is. Postgres sits near-idle at collapse.

Virtual threads are explicitly disabled (`spring.threads.virtual.enabled=false`). The
demonstration depends on a bounded pool of platform threads; Loom would dissolve it.

### Pool behaviour (corrected)

Hikari pending does **not** climb. With 64 workers and a pool of 20, exactly 44 workers are
blocked on `getConnection()` — pending pegs at 44 and stays flat. Wait time is
`44/20 x 26ms ~= 57ms`, far below Hikari's 30s `connectionTimeout`, which therefore never
fires at baseline. It is a latent landmine, not the failure mode.

This makes stage 0 sharper: every pool metric reads healthy and correctly sized, Postgres is
idle, HTTP p99 is single-digit ms — and the service dies anyway. The unbounded queue is the
only thing moving.

The HTTP client pool is sized to 32, above Hikari's 20, so it never becomes a second
constraint. At most 20 workers are ever past the connection gate making HTTP calls at once.

## Two latency families

Because `/events` returns 202 on enqueue, HTTP latency stays flat while the system falls
arbitrarily behind. Both are graphed on one panel:

- `http.server.requests` — what the client sees (flat, misleading)
- `overload.event.e2e` — enqueue timestamp to downstream ack (true state)

Their divergence is the headline artifact of stage 0.

## Stage flags

All default OFF. A stage is an env file; switching stages is a container restart.

| Flag | Stage | Fragile default |
|---|---|---|
| `overload.http.timeouts.enabled` | s1 | infinite connect/response/lease |
| `overload.queue.bounded` | s2 | unbounded `LinkedBlockingQueue` |
| `overload.admission.enabled` | s3 | no token bucket |
| `overload.breaker.enabled` | s4 | direct downstream call |

Stages are cumulative. Flags also permit ablation runs (e.g. s4 with timeouts off).

## Load profiles

| Profile | Downstream | Load | Exercises |
|---|---|---|---|
| A — overload | healthy 25ms +/-10ms | 1x / 2x / 4x knee | s2, s3 |
| B1 — brownout | 25ms -> 400ms at t=60s | 1x | s4 slow-call detection |
| B2 — black hole | accepts, never responds, at t=60s | 1x | s1 |
| C — distributed | healthy | 1x / 2x | s3 cross-instance correctness |

Profile C runs 3 gateway replicas behind nginx sharing one Redis bucket. The claim under
test is that total admitted across replicas equals the configured limit, not 3x it — the
central and currently unevidenced claim of the "distributed" rate limiter.

Profile C is a correctness experiment, not a capacity one. All other profiles run a single
gateway to keep the bottleneck story clean.

## Sub-experiments

- **Queue capacity sweep** (s2): capacity in {500, 5000, 50000} shows
  `e2e p99 ~= capacity / drain rate` — Little's Law, drawn.
- **Timeout value sweep** (s1, profile B1): response timeout in {100ms, 250ms, 1s, none}.
  Expected to show an aggressive timeout converting partial degradation into total outage.
- **Breaker ablation** (s4): breaker on, timeouts off. Expected: never trips, because a
  latency fault produces no errors until timeouts fire.

## Metrics

| Metric | Source |
|---|---|
| p50/p99/p999 edge + e2e | `http.server.requests`, `overload.event.e2e` |
| accepted throughput | `overload.events.accepted` |
| error rate by type | `overload.events.rejected{reason}` |
| queue depth + capacity | `overload.queue.depth`, `.capacity` |
| thread-pool active | `executor.active` |
| connection pools | `hikaricp.connections.{active,pending}`, HTTP pool gauges |
| admission decisions | `overload.admission.{allowed,denied}`, Redis call timer |

Rejection reasons: `validation`, `queue_full`, `admission`, `breaker_open`,
`downstream_timeout`, `downstream_5xx`, `db_timeout`.

## Measurement method

- k6 uses **constant-arrival-rate** (open model) throughout. A closed model self-throttles
  and would hide the phenomenon. Documented as method.
- **Knee definition:** the lowest arrival rate at which e2e p99 exceeds 2x its low-load
  baseline, sustained 30s. `knee.js` ramps 5->80 rps over 10 minutes.
- **Prometheus scrapes at 1s**, not the default 15s. A 15s interval cannot represent a
  per-second sawtooth and would average the s3 waveform into a flat line.
- 60s warmup discarded before each measurement window; 5 min measured.
- Both k6 client-side and Prometheus server-side percentiles are committed, with their
  divergence explained rather than hidden.
- Containers carry explicit `cpus:` and `mem_limit:` so the saturation point is a property
  of the config, not the host. k6 runs on the host, outside the constrained budget.
- Gateway runs at `-Xmx256m` with ~1KB events so stage 0 reaches OOM in ~5 min at 2x and
  ~2 min at 4x. Time-to-OOM against load multiple is a published table.

### Preflight calibration

There is no unit test suite. In its place, `run-stage.sh` asserts before every run that the
protections actually active match the stage file, via a gateway actuator endpoint
(`/actuator/overload`). A stage flag silently failing to take effect would produce a
confident, wrong "no measurable delta" result; this is the guard against that.

## Findings in starter v1.0.0 (pinned 2789431)

These are **predictions from reading the source**, not measurements. Nothing in this table has
been observed under load, because the rate limiter has never been called by the gateway. They
become measurements in Phase 1 and fixes in Phase 2.

| # | Finding | Evidence |
|---|---|---|
| F1 | Refill is second-granular; Lua discards `redis_time[2]` microseconds, so `elapsed` is whole seconds and no refill occurs within a second. Produces per-second burst admission, not smoothing. | s3 sawtooth waveform |
| F2 | Fails closed with HTTP 500: a Lettuce exception is not `RateLimitExceededException`, so it bypasses the 429 handler. Demo pool uses `max-wait: -1ms`. | Redis-fault run |
| F3 | Every rejection captures a stack trace, on the hot path exactly when CPU is scarcest. | CPU under heavy shed |
| F4 | `RateLimiterAutoConfiguration` has no `@ConditionalOnProperty`; cannot be disabled. | required by stage flags |
| F5 | Lua returns 0/1 only — no remaining tokens, no retry-after, so no meaningful 429 headers. | — |
| F6 | Annotation takes compile-time `int` constants; rate cannot come from config, and AOP cannot reject before body parsing. | s3 must use a filter |
| F7 | `requested` is hardcoded to `1` in `RateLimiterService`; the Lua supports N tokens but the API hides it. Batch/weighted admission unreachable. | s3 must pin batch size |
| F8 | No Micrometer instrumentation. | — |

F5, F6 and F7 collapse into one API change.

## Capability additions

Each must ship with a graph or it does not go in.

- **Local pre-check tier.** Cache `denyUntil` per key from Redis's `retryAfterMs` and
  short-circuit locally, capped at 50ms. Collapses Redis ops under sustained shed from every
  request to roughly one per window. Measured against its honest cost in under-admission
  (~40 events worst case at 800/s).
- **Burst separated from rate.** Today capacity *is* the limit and refill is `limit/window`,
  welding burst tolerance to sustained rate. Splitting them makes burst a direct dial on
  queue-depth peak.
- **Shipped filter, config-driven policies, real 429 headers.**
  `ratelimiter.policies.<name>.{limit,window,burst}` plus a `RateLimitFilter` emitting
  `Retry-After` and `X-RateLimit-Remaining`.

GCRA was considered and rejected: F1 plus separated burst recovers most of its benefit
without a second code path.

## Phases

Each is separately shippable. Measure first, fix second, so every Phase 2 delta is
attributable to a specific commit rather than accumulated drift.

| Phase | Delivers | Status |
|---|---|---|
| 0 | Repo, submodule, both compose stacks, downstream-sim, gateway at s0, obs stack, k6, capture tooling. Reproducible collapse + calibrated knee. | **DONE** (2026-09-11). Capacity 754 ev/s; collapse characterised at n=3; see `RESULTS.md`. |
| 1 | Stages s1-s4, each with a measured delta against the s0 baseline. | **NOT STARTED.** No s1/s2/s3/s4 run exists. Timeouts (`DownstreamClientConfig`) and the bounded queue (`IngestQueue`) are implemented behind flags and need only runs. **Admission control is not wired** -- the rate limiter is a declared dependency that is never called. **The circuit breaker is absent** -- Resilience4j is not in the build. |
| 2 | The eight starter findings fixed as PRs, submodule bump, re-measure. | **NOT STARTED**, and blocked on Phase 1: the findings are predictions about a library that has not yet been measured under load. |

### Work done outside the phase plan

After Phase 0 completed, three measurement batches and three review cycles hardened the
baseline: instrumentation (GC series, capture assertions, an offered-rate gate), a controlled
re-measurement at n=3 with randomized order, and a pool-size sweep. This produced the capacity
scaling result and corrected several claims, and it is recorded in `docs/lab-notebook.md`.

It also consumed the effort that Phase 1 was meant to receive. Remaining rigor items --
container CPU via cAdvisor, the pool-size sweep re-run against measured capacity, a long
stability run -- are **deliberately parked** behind Phase 1. They are real and the notebook
records exactly what each would settle, but none is a prerequisite for measuring a protection.

## Calibration targets

Verified at the end of Phase 0; every number below is a starting point to be replaced by a
measured one.

| Parameter | Value |
|---|---|
| workers | 64 |
| Hikari max pool | 20 |
| HTTP client pool | 32 |
| downstream baseline | 25ms +/- 10ms |
| predicted ceiling | ~800 events/s |
| batch size | 20 events, ~1KB each |
| predicted knee | ~40 rps |
| gateway heap | 256MB |
