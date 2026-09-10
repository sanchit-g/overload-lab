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
    # True COMPLETION rate, as distinct from the acceptance rate above. The unbounded
    # queue accepts everything offered, so "accepted" tracks offered load right up until
    # the JVM dies while only a fraction is actually being written. The gap between these
    # two series is the leak, and it is the headline finding -- so it must be a graph,
    # not a paragraph.
    "completed":     'sum(rate(overload_event_e2e_seconds_count[10s]))',
    "rejected":      'sum by (reason) (rate(overload_events_rejected_total[10s]))',
    "queue_depth":   'overload_queue_depth',
    # Recorded for provenance, not plotted: the sentinel -1 means "unbounded queue",
    # which makes each run self-describing without cross-referencing its stage env file.
    # Deliberately absent from the charts -- -1 cannot sit on a log-scaled axis.
    "queue_capacity":'overload_queue_capacity',
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

    print(f"wrote {os.path.join(a.outdir, 'series.json')}")


if __name__ == "__main__":
    main()
