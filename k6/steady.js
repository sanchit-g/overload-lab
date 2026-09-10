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
  const out = __ENV.SUMMARY_OUT || 'summary.json';
  return { [out]: JSON.stringify(data, null, 2), stdout: '' };
}
