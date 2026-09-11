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

# The stage file defines the stage, and deliberately WINS over anything the caller
# exported: a stray environment variable must not be able to silently alter a published
# measurement. Consequence: you cannot fake a preflight mismatch from the outside, which
# is what PREFLIGHT_SELFTEST below exists for.
# shellcheck disable=SC1090
set -a; source "stages/${STAGE}.env"; set +a

# RUN_SUFFIX distinguishes repetitions of the same condition, so n>1 designs do not
# overwrite themselves.
RUN_ID="${STAGE_NAME}-${PROFILE}-rate${RATE}${RUN_SUFFIX:+-${RUN_SUFFIX}}"
OUTDIR="results/${RUN_ID}"
mkdir -p "$OUTDIR" docs/img

# Opt-in, because a growing events table is a genuine confound between back-to-back runs
# (index maintenance slows inserts) AND a legitimate experiment in its own right. Runs that
# want independence set TRUNCATE_EVENTS=1; the growing-table variant deliberately does not.
if [ "${TRUNCATE_EVENTS:-0}" = "1" ]; then
  echo "==> truncating events table (run independence)"
  docker compose -f compose/app.yml exec -T postgres \
    psql -U overload -d overload -c 'TRUNCATE events RESTART IDENTITY;' > /dev/null
fi

echo "==> [${RUN_ID}] recreating gateway"
docker compose -f compose/app.yml up -d --force-recreate --no-deps gateway

echo "==> waiting for health"
for i in $(seq 1 90); do
  if curl -sf localhost:8080/actuator/health | grep -q UP; then break; fi
  sleep 2
  if [ "$i" = 90 ]; then echo "gateway never became healthy"; exit 1; fi
done

# Exercise the guard on demand. The gateway is already running on this stage's real
# config; corrupting only the EXPECTATION here proves the assertion aborts the run on a
# mismatch, without having to misconfigure the service. Tests the assertion path, which
# is the part that has to work -- a genuine drift (stale image, container that ignored
# new env, compose that failed to recreate) is caught by the same comparison.
if [ "${PREFLIGHT_SELFTEST:-}" = "1" ]; then
  echo "==> PREFLIGHT SELF-TEST: expecting boundedQueue=true against a stage that runs it off"
  export OVERLOAD_QUEUE_BOUNDED=true
fi

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
# Do NOT swallow failures here. A broken generator during warmup used to pass silently,
# and the measured run would then start from a cold JVM -- whose very first request takes
# ~112ms against a 25ms downstream, poisoning every percentile.
if ! k6 run --quiet -e RATE="$RATE" -e DURATION="${WARMUP}s" -e BATCH="$BATCH" \
     -e SUMMARY_OUT=/dev/null k6/steady.js > "$OUTDIR/warmup.log" 2>&1; then
  echo "WARNING: warmup k6 run failed. The measured run will start from a cold JVM."
  tail -5 "$OUTDIR/warmup.log" | sed 's/^/    /'
fi

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
set +e
docker run --rm --network overload-lab \
  -v "$ROOT/results:/out" -v "$ROOT/docs/img:/img" \
  overload-lab/capture:dev \
  --prom http://prometheus:9090 --start "$START" --end "$END" --step 1 \
  --outdir "/out/${RUN_ID}" --imgdir /img --label "$RUN_ID"
CAP_RC=$?
set -e

# Assert the generator actually offered what this run claims to have offered. Without
# this, a load generator that silently fell behind would make every "offered" figure in
# RESULTS.md wrong, with nothing anywhere to flag it.
echo "==> validating offered rate"
set +e
python3 tools/validate_run.py "$OUTDIR" "$(( RATE * BATCH ))"
VAL_RC=$?
set -e

if [ "$CAP_RC" != 0 ] || [ "$VAL_RC" != 0 ]; then
  echo "==> RUN FINISHED WITH VALIDATION FAILURES (capture=${CAP_RC} offered=${VAL_RC})"
  echo "    Artifacts are in $OUTDIR. Do not trust these numbers until the cause is understood."
  exit 1
fi

echo "==> done: $OUTDIR"
