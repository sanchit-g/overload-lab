#!/usr/bin/env python3
"""Assert that a run actually offered the load it claims to have offered.

Usage: validate_run.py <run-dir> <target-events-per-second>

k6's summary reports a run-AVERAGE iteration rate, which is distorted on runs where
the gateway dies: the collapse and gracefulStop drag the average well below the rate
that was genuinely sustained. So the gate here is the p90 of the server-side accepted
rate, which answers "was the target ever sustained?" while tolerating the death tail.
dropped_iterations is reported as diagnostic context: non-zero is EXPECTED when the
service dies mid-run, and only matters if the sustained rate also fell short.
"""
import json
import sys

TOLERANCE = 0.97  # rate() smoothing costs a little at window edges


def pct(values, q):
    if not values:
        return float("nan")
    s = sorted(values)
    return s[min(len(s) - 1, int(q * len(s)))]


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    outdir, target = sys.argv[1], float(sys.argv[2])

    series = json.load(open(f"{outdir}/series.json"))["data"]
    acc = series.get("accepted") or []
    if not acc:
        print("FAIL  no accepted series: cannot verify the offered rate")
        sys.exit(1)
    vals = [float(v) for _, v in acc[0]["values"] if v not in ("NaN", "+Inf")]

    p90 = pct(vals, 0.90)
    sustained = sum(1 for v in vals if v >= 0.95 * target) / len(vals)

    try:
        m = json.load(open(f"{outdir}/k6-summary.json"))["metrics"]
        dropped = m.get("dropped_iterations", {}).get("values", {}).get("count", 0)
        iters = m.get("iterations", {}).get("values", {}).get("rate", float("nan"))
    except FileNotFoundError:
        dropped, iters = None, float("nan")

    print(f"  target offered      : {target:.0f} events/s")
    print(f"  accepted p90        : {p90:.0f} events/s  ({p90/target*100:.1f}% of target)")
    print(f"  samples within 5%   : {sustained*100:.0f}% of the window")
    print(f"  k6 iterations (avg) : {iters:.2f}/s   <- run-average, distorted by any death tail")
    print(f"  k6 dropped_iters    : {dropped if dropped is not None else 'n/a'}")

    if p90 < TOLERANCE * target:
        print(f"\nFAIL  the load generator never sustained the target rate "
              f"(p90 {p90:.0f} < {TOLERANCE*target:.0f}). Every 'offered' number from this "
              f"run is wrong; raise maxVUs or reduce the rate.")
        sys.exit(1)

    if dropped:
        print(f"\n  note: {dropped:.0f} dropped iterations, but the target was sustained "
              f"(p90 {p90/target*100:.1f}%) -- consistent with the service dying mid-run "
              f"rather than the generator falling behind.")
    print("\nvalidation OK: the offered rate was genuinely achieved")


if __name__ == "__main__":
    main()
