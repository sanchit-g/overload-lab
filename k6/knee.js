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
