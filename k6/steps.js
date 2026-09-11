// Stepped load profile: jump to a rate, HOLD it flat, jump to the next.
//
// knee.js ramps *toward* each target, so every stage is an average of the ramp rather
// than a measurement at a known rate -- which is why its formal knee detector had to be
// overridden by judgement. Here each hold is a clean constant-rate measurement at a known
// utilisation, with ~90 samples at the 1s scrape interval.
//
// Rates are expressed as fractions of a PREDICTED capacity (BASE_RPS) so the same profile
// works across Hikari pool sizes whose capacities differ by 8x.
import http from 'k6/http';
import { Counter } from 'k6/metrics';

const BASE = Number(__ENV.BASE_RPS || 37.7);
const HOLD = __ENV.HOLD || '90s';
const RHOS = (__ENV.RHOS || '0.5,0.7,0.85,0.92,0.97,1.05').split(',').map(Number);
const BATCH = Number(__ENV.BATCH || 20);
const PAYLOAD_BYTES = Number(__ENV.PAYLOAD_BYTES || 1024);
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

const accepted = new Counter('batches_accepted');
const shed = new Counter('batches_shed');
const failed = new Counter('batches_failed');
const PAD = 'x'.repeat(PAYLOAD_BYTES);

// timeUnit of 10s gives 0.1 rps resolution. At c=5 the steps sit near 4.7 rps, where
// integer-per-second rates would distort the utilisation being targeted.
const per10s = (rho) => Math.round(BASE * rho * 10);

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(99)', 'p(99.9)', 'max'],
  scenarios: {
    steps: {
      executor: 'ramping-arrival-rate',
      startRate: per10s(RHOS[0]),
      timeUnit: '10s',
      preAllocatedVUs: 200,
      maxVUs: 3000,
      // 1s jump then a flat hold, so each plateau is measured at a constant rate.
      stages: RHOS.flatMap((r) => [
        { target: per10s(r), duration: '1s' },
        { target: per10s(r), duration: HOLD },
      ]),
      gracefulStop: '20s',
    },
  },
};

export default function () {
  const batchId = `s-${__VU}-${__ITER}`;
  const events = [];
  for (let i = 0; i < BATCH; i++) events.push({ eventId: `${batchId}-${i}`, payload: PAD });
  const res = http.post(`${BASE_URL}/events`, JSON.stringify({ batchId, events }), {
    headers: { 'Content-Type': 'application/json' },
    timeout: '120s',
  });
  if (res.status === 202) accepted.add(1);
  else if (res.status === 429) shed.add(1);
  else failed.add(1);
}

export function handleSummary(data) {
  for (const k of ['batches_accepted', 'batches_shed', 'batches_failed']) {
    if (!data.metrics[k]) {
      data.metrics[k] = { type: 'counter', contains: 'default', values: { count: 0, rate: 0 } };
    }
  }
  // Record the schedule so analysis can locate step boundaries without re-deriving them.
  data.schedule = { base_rps: BASE, hold: HOLD, rhos: RHOS, batch: BATCH };
  return { [__ENV.SUMMARY_OUT || 'steps-summary.json']: JSON.stringify(data, null, 2), stdout: '' };
}
