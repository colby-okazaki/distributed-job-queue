/**
 * Load test against a running stack (docker compose up).
 *
 *   npm run bench -- --jobs 2000 --ms 20 --fail-rate 0.2
 *
 * Enqueues N sleep jobs through the API, waits for all of them to reach a
 * terminal state, then reads Postgres + Redis to report throughput, latency,
 * retry success rate and how many duplicate deliveries idempotency caught.
 */
import { parseArgs } from 'node:util';
import pg from 'pg';

const { values } = parseArgs({
  options: {
    jobs: { type: 'string', default: '1000' },
    ms: { type: 'string', default: '20' },
    'fail-rate': { type: 'string', default: '0.2' },
    concurrency: { type: 'string', default: '50' },
    api: { type: 'string', default: process.env.API_URL ?? 'http://localhost:3000' },
    db: { type: 'string', default: process.env.DATABASE_URL ?? 'postgres://jobs:jobs@localhost:5432/jobs' },
  },
});

const N = Number(values.jobs);
const api = values.api!;
const pool = new pg.Pool({ connectionString: values.db });

async function counters(): Promise<Record<string, number>> {
  const res = await fetch(`${api}/stats`);
  return ((await res.json()) as any).queue.counters;
}

function pct(sorted: number[], p: number): number {
  if (!sorted.length) return 0;
  return sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)];
}

const before = await counters();
const ids: string[] = [];
const t0 = performance.now(); // monotonic: immune to wall-clock jumps

// Enqueue with bounded client concurrency. On 429 the client waits as told
// by Retry-After and tries again, like a well-behaved producer should.
let next = 0;
let throttled = 0;
await Promise.all(
  Array.from({ length: Number(values.concurrency) }, async () => {
    while (next < N) {
      next++;
      for (;;) {
        const res = await fetch(`${api}/jobs`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'x-client-id': 'bench' },
          body: JSON.stringify({ type: 'sleep', payload: { ms: Number(values.ms), failRate: Number(values['fail-rate']) } }),
        });
        if (res.status === 429) {
          throttled++;
          const { retryAfterMs } = (await res.json()) as { retryAfterMs: number };
          await new Promise((r) => setTimeout(r, retryAfterMs));
          continue;
        }
        if (res.status !== 202) throw new Error(`enqueue failed: ${res.status} ${await res.text()}`);
        ids.push(((await res.json()) as any).id);
        break;
      }
    }
  }),
);
const enqueueSec = (performance.now() - t0) / 1000;
console.log(
  `enqueued ${N} jobs in ${enqueueSec.toFixed(2)}s (${(N / enqueueSec).toFixed(0)} jobs/s accepted, ${throttled} requests throttled)`,
);
if (throttled) console.log('  note: rate limiting slowed enqueueing; set RATE_LIMIT_PER_SEC=0 on the API to measure raw worker throughput');

for (;;) {
  const { rows } = await pool.query<{ n: string }>(
    "SELECT count(*) AS n FROM jobs WHERE id = ANY($1) AND status IN ('succeeded','failed')",
    [ids],
  );
  const done = Number(rows[0].n);
  process.stdout.write(`\r  ${done}/${N} finished`);
  if (done === N) break;
  await new Promise((r) => setTimeout(r, 250));
}
const wallSec = (performance.now() - t0) / 1000;
process.stdout.write('\n');

const { rows } = await pool.query<{ status: string; attempts: number; latency_ms: number; duration_ms: number | null }>(
  `SELECT status, attempts,
          EXTRACT(EPOCH FROM (finished_at - created_at)) * 1000 AS latency_ms,
          duration_ms
     FROM jobs WHERE id = ANY($1)`,
  [ids],
);
const after = await counters();
const delta = (k: string) => (after[k] ?? 0) - (before[k] ?? 0);

const succeeded = rows.filter((r) => r.status === 'succeeded');
const retried = rows.filter((r) => r.attempts > 1 || r.status === 'failed');
const retriedOk = retried.filter((r) => r.status === 'succeeded');
const e2e = succeeded.map((r) => Number(r.latency_ms)).sort((a, b) => a - b);
const firstTry = succeeded.filter((r) => r.attempts === 1).map((r) => Number(r.latency_ms)).sort((a, b) => a - b);
const proc = succeeded.map((r) => r.duration_ms ?? 0).sort((a, b) => a - b);

console.table({
  'jobs': N,
  'wall time (s)': +wallSec.toFixed(1),
  'throughput (jobs/s, end to end)': +(N / wallSec).toFixed(1),
  'succeeded': succeeded.length,
  'dead-lettered': rows.length - succeeded.length,
  'jobs that needed a retry': retried.length,
  'retry success rate': retried.length ? `${((100 * retriedOk.length) / retried.length).toFixed(1)}%` : 'n/a',
  'p50 latency, first-try jobs (ms)': Math.round(pct(firstTry, 50)),
  'p95 latency, first-try jobs (ms)': Math.round(pct(firstTry, 95)),
  'p95 latency, all jobs incl. backoff (ms)': Math.round(pct(e2e, 95)),
  'p95 handler duration (ms)': pct(proc, 95),
  'redeliveries (reclaimed after crash)': delta('redeliveries'),
  'duplicates caught by idempotency': delta('duplicates_skipped'),
});

await pool.end();
