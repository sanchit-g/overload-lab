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
