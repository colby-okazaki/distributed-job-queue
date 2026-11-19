import { afterEach, describe, expect, it } from 'vitest';
import { registry } from '../src/jobs/handlers.js';
import { RateLimiter } from '../src/rateLimit.js';
import { createRedis } from '../src/redis.js';
import { Harness, testConfig } from './helpers.js';

let h: Harness | undefined;
afterEach(async () => {
  await h?.teardown();
  h = undefined;
});

const job = { type: 'sleep', payload: { ms: 1 } };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function harness(rateLimitPerSec: number, rateLimitBurst: number) {
  h = await new Harness(testConfig({ rateLimitPerSec, rateLimitBurst }), registry).setup();
  return h;
}

describe('rate limiting POST /jobs', () => {
  it('allows a burst, then rejects with 429 and Retry-After', async () => {
    const h = await harness(1, 5);
    const client = { 'x-client-id': 'burst-test' };
    const remaining: string[] = [];
    for (let i = 0; i < 5; i++) {
      const res = await fetch(`${h.baseUrl}/jobs`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...client },
        body: JSON.stringify(job),
      });
      expect(res.status).toBe(202);
      remaining.push(res.headers.get('ratelimit-remaining')!);
    }
    expect(remaining).toEqual(['4', '3', '2', '1', '0']);

    const res = await fetch(`${h.baseUrl}/jobs`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...client },
      body: JSON.stringify(job),
    });
    expect(res.status).toBe(429);
    expect(Number(res.headers.get('retry-after'))).toBeGreaterThanOrEqual(1);
    expect((await res.json()).retryAfterMs).toBeGreaterThan(0);
    expect((await h.queue.stats()).counters.rate_limited).toBe(1);
  });

  it('refills tokens over time', async () => {
    const h = await harness(20, 2); // one token every 50 ms
    const client = { 'x-client-id': 'refill-test' };
    expect((await h.post('/jobs', job, client)).status).toBe(202);
    expect((await h.post('/jobs', job, client)).status).toBe(202);
    expect((await h.post('/jobs', job, client)).status).toBe(429);
    await sleep(120);
    expect((await h.post('/jobs', job, client)).status).toBe(202);
  });

  it('limits each client independently', async () => {
    const h = await harness(1, 3);
    for (let i = 0; i < 3; i++) await h.post('/jobs', job, { 'x-client-id': 'noisy' });
    expect((await h.post('/jobs', job, { 'x-client-id': 'noisy' })).status).toBe(429);
    expect((await h.post('/jobs', job, { 'x-client-id': 'quiet' })).status).toBe(202);
  });

  it('does not rate-limit reads', async () => {
    const h = await harness(1, 1);
    await h.post('/jobs', job, { 'x-client-id': 'reader' });
    for (let i = 0; i < 5; i++) expect((await fetch(`${h.baseUrl}/stats`)).status).toBe(200);
  });
});

describe('token bucket', () => {
  it('enforces one shared limit across multiple API instances', async () => {
    const cfg = testConfig();
    // Two limiters on separate connections stand in for two API processes.
    const redisA = createRedis(cfg.redisUrl);
    const redisB = createRedis(cfg.redisUrl);
    try {
      const a = new RateLimiter(redisA, cfg.keyPrefix, 0.01, 20);
      const b = new RateLimiter(redisB, cfg.keyPrefix, 0.01, 20);
      const results = await Promise.all(
        Array.from({ length: 60 }, (_, i) => (i % 2 ? a : b).take('shared-client')),
      );
      expect(results.filter((r) => r.allowed)).toHaveLength(20);
    } finally {
      const keys = await redisA.keys(`${cfg.keyPrefix}:*`);
      if (keys.length) await redisA.del(...keys);
      redisA.disconnect();
      redisB.disconnect();
    }
  });

  it('holds a client to the sustained rate under constant pressure', async () => {
    const cfg = testConfig();
    const redis = createRedis(cfg.redisUrl);
    try {
      const limiter = new RateLimiter(redis, cfg.keyPrefix, 50, 1);
      let allowed = 0;
      const started = performance.now();
      while (performance.now() - started < 1_000) {
        const results = await Promise.all(Array.from({ length: 10 }, () => limiter.take('hammer')));
        allowed += results.filter((r) => r.allowed).length;
      }
      const elapsedMs = performance.now() - started;
      const expected = 1 + (elapsedMs * 50) / 1000; // the burst, plus refill
      expect(allowed).toBeGreaterThanOrEqual(Math.floor(expected * 0.85));
      expect(allowed).toBeLessThanOrEqual(Math.ceil(expected * 1.1));
    } finally {
      await redis.del(`${cfg.keyPrefix}:rl:hammer`);
      redis.disconnect();
    }
  });

  it('expires idle buckets so Redis memory stays bounded', async () => {
    const cfg = testConfig();
    const redis = createRedis(cfg.redisUrl);
    try {
      await new RateLimiter(redis, cfg.keyPrefix, 100, 10).take('idle');
      const ttl = await redis.pttl(`${cfg.keyPrefix}:rl:idle`);
      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(1_100); // full refill (100 ms) + 1 s slack
    } finally {
      await redis.del(`${cfg.keyPrefix}:rl:idle`);
      redis.disconnect();
    }
  });
});
