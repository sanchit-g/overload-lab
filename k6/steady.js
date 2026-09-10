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
