/**
 * Rate-limit fairness benchmark against a running stack.
 *
 *   npm run bench:ratelimit -- --seconds 10 --normal-clients 4 --normal-rate 20
 *
 * One "noisy" client sends POST /jobs as fast as it can. Several "normal"
 * clients send at a steady, modest rate. A working per-client limiter holds
 * the noisy client to the configured rate while the normal clients get every
 * request through, with no added latency.
 */
import { parseArgs } from 'node:util';

const { values } = parseArgs({
  options: {
    seconds: { type: 'string', default: '10' },
    'normal-clients': { type: 'string', default: '4' },
    'normal-rate': { type: 'string', default: '20' },
    'noisy-concurrency': { type: 'string', default: '25' },
    api: { type: 'string', default: process.env.API_URL ?? 'http://localhost:3000' },
  },
});

const api = values.api!;
const durationMs = Number(values.seconds) * 1000;
const normalRate = Number(values['normal-rate']);
const body = JSON.stringify({ type: 'sleep', payload: { ms: 1 } });

interface ClientStats {
  name: string;
  sent: number;
  accepted: number;
  rejected: number;
  latencies: number[];
}

let limitHeader = '?';

async function send(stats: ClientStats): Promise<void> {
  const t = performance.now();
  const res = await fetch(`${api}/jobs`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-client-id': stats.name },
    body,
  });
  await res.arrayBuffer();
  stats.sent++;
  limitHeader = res.headers.get('ratelimit-limit') ?? limitHeader;
  if (res.status === 202) {
    stats.accepted++;
    stats.latencies.push(performance.now() - t);
  } else if (res.status === 429) {
    stats.rejected++;
  } else {
    throw new Error(`unexpected status ${res.status}`);
  }
}

const run = `${Date.now()}`; // fresh client IDs each run, so buckets start full
const noisy: ClientStats = { name: `noisy-${run}`, sent: 0, accepted: 0, rejected: 0, latencies: [] };
const normals: ClientStats[] = Array.from({ length: Number(values['normal-clients']) }, (_, i) => ({
  name: `normal-${i + 1}-${run}`,
  sent: 0,
  accepted: 0,
  rejected: 0,
  latencies: [],
}));

const start = performance.now();
const until = start + durationMs;

// Noisy client: N parallel loops, no pacing at all.
const noisyLoops = Array.from({ length: Number(values['noisy-concurrency']) }, async () => {
  while (performance.now() < until) await send(noisy);
});

// Normal clients: one request every 1/rate seconds, on a fixed schedule.
const normalLoops = normals.map(async (c) => {
  const gap = 1000 / normalRate;
  let nextAt = performance.now();
  const inflight: Promise<void>[] = [];
  while (nextAt < until) {
    const wait = nextAt - performance.now();
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    inflight.push(send(c));
    nextAt += gap;
  }
  await Promise.all(inflight);
});

await Promise.all([...noisyLoops, ...normalLoops]);
const secs = (performance.now() - start) / 1000;

const p95 = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  return s.length ? s[Math.min(s.length - 1, Math.ceil(0.95 * s.length) - 1)] : 0;
};
const row = (c: ClientStats) => ({
  sent: c.sent,
  accepted: c.accepted,
  rejected: c.rejected,
  'accepted/s': +(c.accepted / secs).toFixed(1),
  'p95 accepted latency (ms)': +p95(c.latencies).toFixed(1),
});

console.log(`ran ${secs.toFixed(1)}s against ${api} (bucket size ${limitHeader})`);
console.table({
  'noisy client': row(noisy),
  ...Object.fromEntries(normals.map((c, i) => [`normal client ${i + 1}`, row(c)])),
});

const normalSent = normals.reduce((n, c) => n + c.sent, 0);
const normalAccepted = normals.reduce((n, c) => n + c.accepted, 0);
console.log(
  `noisy client: ${noisy.sent} requests, ${((100 * noisy.rejected) / Math.max(1, noisy.sent)).toFixed(1)}% rejected, ` +
    `held to ${(noisy.accepted / secs).toFixed(1)} accepted/s`,
);
console.log(`normal clients: ${normalAccepted}/${normalSent} accepted (${((100 * normalAccepted) / Math.max(1, normalSent)).toFixed(1)}%)`);
