#!/usr/bin/env python3
"""Query the Prometheus range API for one run and emit raw series plus PNGs.

Usage:
  capture.py --prom http://prometheus:9090 --start <epoch> --end <epoch> \
             --step 1 --outdir /out/<stage> --imgdir /img --label s0 [--no-validate]

Fault tolerance is deliberate: a stage that dies of heap exhaustion loses its scrape
target mid-run, so missing data is expected and is warned about rather than raised.
Validation therefore fails only on AMBIGUITY (more than one series where one is
expected), never on absence -- and series.json is always written first, so a failed
validation never destroys the capture it was checking.
"""
import argparse
import json
import os
import sys

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
    # True COMPLETION rate, as distinct from the acceptance rate above. The unbounded
    # queue accepts everything offered, so "accepted" tracks offered load right up until
    # the JVM dies while only a fraction is actually being written. The gap between these
    # two series is the leak, and it is the headline finding.
    "completed":     'sum(rate(overload_event_e2e_seconds_count[10s]))',
    "rejected":      'sum by (reason) (rate(overload_events_rejected_total[10s]))',
    "queue_depth":   'sum(overload_queue_depth)',
    # Recorded for provenance, not plotted: the sentinel -1 means "unbounded queue",
    # which makes each run self-describing without cross-referencing its stage env file.
    "queue_capacity":'max(overload_queue_capacity)',
    "workers_active":'sum(executor_active_threads{name="overload.workers"})',
    "hikari_active": 'sum(hikaricp_connections_active{pool="gateway-pool"})',
    "hikari_pending":'sum(hikaricp_connections_pending{pool="gateway-pool"})',
    "http_leased":   'sum(overload_http_pool_leased)',
    "http_pending":  'sum(overload_http_pool_pending)',
    "heap_used":     'sum(jvm_memory_used_bytes{area="heap"})',
    # GC. The claim that capacity itself degrades under extreme overload was previously
    # inferred by elimination; these make it a correlation that can be plotted against
    # the completion rate. gc_pause_rate is seconds of pause per second of wall clock.
    "gc_pause_rate": 'sum(rate(jvm_gc_pause_seconds_sum[10s]))',
    "gc_count_rate": 'sum(rate(jvm_gc_pause_seconds_count[10s]))',
    "gc_alloc_rate": 'sum(rate(jvm_gc_memory_allocated_bytes_total[10s]))',
    # Raw counter, used by validation to prove the window holds exactly one container
    # lifetime: a restart resets it, which rate() would silently mask.
    "accepted_total":'sum(overload_events_accepted_total)',
}

# Queries that legitimately return more than one series.
MULTI_SERIES_OK = {"rejected"}

# Not plotted; captured for provenance or validation only.
UNPLOTTED = {"queue_capacity", "accepted_total"}


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


def validate(raw, label):
    """Return (problems, warnings). Problems are ambiguity; warnings are absence."""
    problems, warnings = [], []

    for name, res in raw.items():
        n = len(res)
        if n == 0:
            warnings.append(f"{name}: no data (expected if the run died mid-window)")
        elif n > 1 and name not in MULTI_SERIES_OK:
            labels = [s["metric"] for s in res][:3]
            problems.append(
                f"{name}: {n} series where 1 was expected -- the window is ambiguous. "
                f"Sample labels: {labels}")

    # Exactly one container lifetime: a gateway restart resets the counter, and rate()
    # would hide that. This is the check that would have caught the instance-tag churn.
    at = raw.get("accepted_total") or []
    if at:
        vals = [float(v) for _, v in at[0].get("values", []) if v not in ("NaN", "+Inf")]
        drops = [(i, vals[i - 1], vals[i]) for i in range(1, len(vals)) if vals[i] < vals[i - 1]]
        if drops:
            i, a, b = drops[0]
            problems.append(
                f"accepted_total went backwards at sample {i} ({a:.0f} -> {b:.0f}): the "
                f"gateway restarted inside the measurement window, so this run mixes "
                f"two container lifetimes.")

    # The capacity sentinel must be -1 (unbounded) or a positive size, never 0/absent.
    qc = raw.get("queue_capacity") or []
    if not qc:
        warnings.append("queue_capacity: absent, cannot record whether the queue was bounded")
    else:
        vs = {float(v) for _, v in qc[0].get("values", []) if v not in ("NaN", "+Inf")}
        if not vs:
            warnings.append("queue_capacity: no numeric samples")
        elif vs != {-1.0} and any(v <= 0 for v in vs):
            problems.append(f"queue_capacity: unexpected values {sorted(vs)} "
                            f"(expected exactly -1 for unbounded, or all positive)")
    return problems, warnings


def plot(path, title, ylabel, named_series, logy=False):
    fig, ax = plt.subplots(figsize=(11, 4.5))
    plotted = False
    for name, result in named_series:
        for lbl, xs, ys in series_of(result):
            ax.plot(xs, ys, linewidth=1.3, label=f"{name} {lbl}".strip())
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


def plot_gc_vs_throughput(path, title, gc, completed):
    """Twin axes: GC pause seconds per second against completion rate.

    This is the evidence for 'overload makes the system slower'. If completion falls
    as GC pause time rises, the capacity loss is GC pressure rather than anything else.
    """
    gs, cs = series_of(gc), series_of(completed)
    if not gs or not cs:
        return False
    fig, ax = plt.subplots(figsize=(11, 4.5))
    _, xs, ys = gs[0]
    ax.plot(xs, ys, linewidth=1.4, color="tab:red", label="GC pause (s/s)")
    ax.set_xlabel("seconds into run")
    ax.set_ylabel("GC pause seconds per second", color="tab:red")
    ax.tick_params(axis="y", labelcolor="tab:red")
    ax.grid(alpha=0.3)
    ax2 = ax.twinx()
    _, xs2, ys2 = cs[0]
    ax2.plot(xs2, ys2, linewidth=1.4, color="tab:blue", label="completed (events/s)")
    ax2.set_ylabel("completed events/s", color="tab:blue")
    ax2.tick_params(axis="y", labelcolor="tab:blue")
    ax.set_title(title)
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
    p.add_argument("--no-validate", action="store_true",
                   help="skip assertions; for ad-hoc windows spanning several container lifetimes")
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

    series_path = os.path.join(a.outdir, "series.json")
    with open(series_path, "w") as fh:
        json.dump({"label": a.label, "queries": QUERIES, "data": raw}, fh)
    print(f"wrote {series_path}")

    charts = [
        ("latency", "Latency: edge vs end-to-end", "seconds",
         [("edge p50", raw["edge_p50"]), ("edge p99", raw["edge_p99"]),
          ("e2e p50", raw["e2e_p50"]), ("e2e p99", raw["e2e_p99"]),
          ("e2e p999", raw["e2e_p999"])], True),
        ("throughput", "Accepted vs completed throughput, and rejections", "events/s",
         [("accepted", raw["accepted"]), ("completed", raw["completed"]),
          ("rejected", raw["rejected"])], False),
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

    gc_path = os.path.join(a.imgdir, f"{a.label}-gc.png")
    if plot_gc_vs_throughput(gc_path, f"[{a.label}] GC pause vs completion rate",
                             raw["gc_pause_rate"], raw["completed"]):
        print(f"wrote {gc_path}")

    if a.no_validate:
        print("validation skipped (--no-validate)")
        return

    problems, warnings = validate(raw, a.label)
    for w in warnings:
        print(f"WARN  {w}")
    for pr in problems:
        print(f"FAIL  {pr}")
    if problems:
        print(f"\nVALIDATION FAILED: {len(problems)} problem(s). "
              f"series.json was still written to {series_path}.")
        sys.exit(1)
    print(f"validation OK ({len(warnings)} warning(s))")


if __name__ == "__main__":
    main()
