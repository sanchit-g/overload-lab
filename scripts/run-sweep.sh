#!/usr/bin/env bash
# Sweep the HikariCP pool size, holding everything else constant, to test whether
# capacity scales as c / hold_time and whether the latency knee sharpens with c.
set -euo pipefail

C="${1:?usage: run-sweep.sh <hikari-pool-size>}"
HOLD="${HOLD:-90s}"
RHOS="${RHOS:-0.5,0.7,0.85,0.92,0.97,1.05}"
HOLD_TIME_S="${HOLD_TIME_S:-0.0265}"   # measured: 20 connections / 754 ev/s
BATCH="${BATCH:-20}"
WARMUP="${WARMUP:-20}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BASE_RPS=$(python3 -c "print(round($C/$HOLD_TIME_S/$BATCH, 1))")
RUN_ID="sweep-c${C}"
OUTDIR="results/${RUN_ID}"
mkdir -p "$OUTDIR" docs/img

echo "==> [${RUN_ID}] predicted capacity $(python3 -c "print(round($C/$HOLD_TIME_S))") ev/s = ${BASE_RPS} rps"

echo "==> truncating events table (run independence)"
docker compose -f compose/app.yml exec -T postgres \
  psql -U overload -d overload -c 'TRUNCATE events RESTART IDENTITY;' > /dev/null

# HTTP pool is raised to the worker count for every sweep run. At its default of 32 it
# would bind before Hikari at c=40, and that point would silently measure the HTTP pool
# instead -- the exact confound Task 9 existed to remove.
echo "==> recreating gateway with hikari=${C}, http pool=64"
OVERLOAD_HIKARI_MAX="$C" OVERLOAD_HTTP_POOL=64 \
  docker compose -f compose/app.yml up -d --force-recreate --no-deps gateway

echo "==> waiting for health"
for i in $(seq 1 90); do
  curl -sf localhost:8080/actuator/health | grep -q UP && break
  sleep 2
  [ "$i" = 90 ] && { echo "gateway never became healthy"; exit 1; }
done

# Assert the independent variable actually changed. A pool size that silently stayed at
# its default would produce four near-identical curves and a confident wrong conclusion.
echo "==> preflight: asserting the pool sizes actually took effect"
curl -sf localhost:8080/actuator/overload > "$OUTDIR/preflight.json"
python3 - "$OUTDIR/preflight.json" "$C" <<'PY'
import json, sys
t = json.load(open(sys.argv[1]))["tunables"]
want_c, want_http = int(sys.argv[2]), 64
bad = {}
if t.get("hikariMaxPoolSize") != want_c: bad["hikariMaxPoolSize"] = (want_c, t.get("hikariMaxPoolSize"))
if t.get("httpPoolSize") != want_http:   bad["httpPoolSize"]      = (want_http, t.get("httpPoolSize"))
if bad:
    print("PREFLIGHT FAILED (expected, actual):", bad); sys.exit(1)
print(f"preflight OK: hikari={t['hikariMaxPoolSize']} http={t['httpPoolSize']} workers={t['workers']}")
PY

echo "==> resetting downstream knobs to healthy"
curl -sf -X POST localhost:9090/control -H 'Content-Type: application/json' \
  -d '{"latencyMs":25,"jitterMs":10,"failureRate":0,"mode":"NORMAL"}' > /dev/null

echo "==> warmup ${WARMUP}s (discarded)"
if ! k6 run --quiet -e BASE_RPS="$BASE_RPS" -e RHOS=0.5 -e HOLD="${WARMUP}s" \
     -e BATCH="$BATCH" -e SUMMARY_OUT=/dev/null k6/steps.js > "$OUTDIR/warmup.log" 2>&1; then
  echo "WARNING: warmup failed; the measured run starts from a cold JVM"
  tail -5 "$OUTDIR/warmup.log" | sed 's/^/    /'
fi

START=$(date +%s)
echo "==> stepping through rho = ${RHOS} at ${HOLD} each"
set +e
k6 run --quiet -e BASE_RPS="$BASE_RPS" -e RHOS="$RHOS" -e HOLD="$HOLD" -e BATCH="$BATCH" \
  -e SUMMARY_OUT="$OUTDIR/k6-summary.json" k6/steps.js
K6_RC=$?
set -e
END=$(date +%s)

echo "{\"c\": $C, \"base_rps\": $BASE_RPS, \"hold\": \"$HOLD\", \"rhos\": \"$RHOS\", \"start\": $START, \"end\": $END}" \
  > "$OUTDIR/sweep-meta.json"
STATE=$(docker inspect -f '{{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' overload-lab-gateway-1 2>/dev/null || echo gone)
echo "$STATE" > "$OUTDIR/container-state.txt"
echo "k6 exit=${K6_RC}" >> "$OUTDIR/container-state.txt"
echo "==> gateway container state: ${STATE}"

echo "==> capturing"
set +e
docker run --rm --network overload-lab -v "$ROOT/results:/out" -v "$ROOT/docs/img:/img" \
  overload-lab/capture:dev --prom http://prometheus:9090 \
  --start "$START" --end "$END" --step 1 --outdir "/out/${RUN_ID}" --imgdir /img --label "$RUN_ID"
CAP_RC=$?
set -e
[ "$CAP_RC" != 0 ] && { echo "==> CAPTURE VALIDATION FAILED for ${RUN_ID}"; exit 1; }
echo "==> done: $OUTDIR"
