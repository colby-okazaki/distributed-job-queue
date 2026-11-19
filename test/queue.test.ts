import { afterEach, describe, expect, it, vi } from 'vitest';
import { registry } from '../src/jobs/handlers.js';
import { SimulatedCrash } from '../src/worker.js';
import { countingJob, Harness, testConfig } from './helpers.js';

let h: Harness | undefined;
afterEach(async () => {
  await h?.teardown();
  h = undefined;
});

async function harness(extraRegistry = {}, cfg = {}) {
  h = await new Harness(testConfig(cfg), { ...registry, ...extraRegistry }).setup();
  return h;
}

describe('core loop', () => {
  it('runs a job end to end and records its history', async () => {
    const h = await harness();
    await h.startWorker('w1');
    const id = await h.enqueue('sleep', { ms: 10 });
    const job = await h.waitFor(id);
    expect(job.status).toBe('succeeded');
    expect(job.attempts).toBe(1);
    expect(job.result).toEqual({ slept: 10 });
    expect(job.events.map((e) => e.status)).toEqual(['pending', 'processing', 'succeeded']);
  });

  it('rejects unknown types and invalid payloads at the API', async () => {
    const h = await harness();
    expect((await h.post('/jobs', { type: 'nope', payload: {} })).status).toBe(400);
    expect((await h.post('/jobs', { type: 'sleep', payload: { ms: -5 } })).status).toBe(400);
  });

  it('dedupes producer retries with an Idempotency-Key header', async () => {
    const h = await harness();
    const key = { 'idempotency-key': `order-${Date.now()}` };
    const a = await h.post('/jobs', { type: 'sleep', payload: { ms: 1 } }, key);
    const b = await h.post('/jobs', { type: 'sleep', payload: { ms: 1 } }, key);
    expect(a.status).toBe(202);
    expect(b.status).toBe(200);
    expect(b.body).toMatchObject({ id: a.body.id, deduplicated: true });
  });

  it('runs scheduled (delayed) jobs only once they are due', async () => {
    const h = await harness();
    await h.startWorker('w1');
    const res = await h.post('/jobs', { type: 'sleep', payload: { ms: 1 }, delayMs: 300 });
    const started = Date.now();
    expect((await h.repo.get(res.body.id))!.status).toBe('scheduled');
    await h.waitFor(res.body.id);
    expect(Date.now() - started).toBeGreaterThanOrEqual(250);
  });
});

describe('retries and dead-letter queue', () => {
  it('retries with backoff until the job succeeds', async () => {
    const h = await harness();
    await h.startWorker('w1');
    const id = await h.enqueue('sleep', { ms: 1, failTimes: 2 });
    const job = await h.waitFor(id);
    expect(job.status).toBe('succeeded');
    expect(job.attempts).toBe(3);
    expect(job.events.map((e) => e.status)).toEqual([
      'pending', 'processing', 'retrying', 'processing', 'retrying', 'processing', 'succeeded',
    ]);
    expect((await h.queue.stats()).counters.succeeded_after_retry).toBe(1);
  });

  it('schedules retries on the Redis clock, so worker clock skew does not delay them', async () => {
    const h = await harness();
    await h.startWorker('w1');
    const id = await h.enqueue('sleep', { ms: 1, failTimes: 1 });
    // This process's clock jumps an hour into the past right after the first
    // attempt. If retries were scored with Date.now(), promotion would stall.
    const realNow = Date.now.bind(Date);
    const spy = vi.spyOn(Date, 'now').mockImplementation(() => realNow() - 3_600_000);
    try {
      const deadline = realNow() + 3_000;
      let job = await h.repo.get(id);
      while (job!.status !== 'succeeded' && realNow() < deadline) {
        await new Promise((r) => setTimeout(r, 20));
        job = await h.repo.get(id);
      }
      expect(job!.status).toBe('succeeded');
      expect(job!.attempts).toBe(2);
    } finally {
      spy.mockRestore();
    }
  });

  it('dead-letters after maxAttempts and can replay from the DLQ', async () => {
    const flaky = countingJob();
    const h = await harness({ flaky: flaky.def });
    await h.startWorker('w1');

    const id = await h.enqueue('flaky', { fail: 3 }, { maxAttempts: 3 });
    const failed = await h.waitFor(id);
    expect(failed.status).toBe('failed');
    expect(failed.last_error).toBe('fail #3');
    expect(flaky.runs.get(id)).toBe(3);

    const dlq = await (await fetch(`${h.baseUrl}/dlq`)).json();
    expect(dlq.map((d: any) => d.job.id)).toContain(id);

    // "Fix the bug", then replay: attempts start over, so it now passes.
    flaky.def.run = async () => ({ fixed: true });
    const replay = await h.post(`/dlq/${id}/replay`, {});
    expect(replay.status).toBe(202);
    const done = await h.waitFor(id, ['succeeded']);
    expect(done.result).toEqual({ fixed: true });
    expect(done.events.map((e) => e.status)).toContain('replayed');
    expect((await h.queue.stats()).deadLettered).toBe(0);
  });

  it('sends non-retryable errors straight to the DLQ', async () => {
    const h = await harness();
    await h.startWorker('w1');
    // Bypass API validation to simulate a malformed message that slipped through.
    await h.queue.enqueue({ id: crypto.randomUUID(), type: 'sleep', payload: { ms: 'oops' }, attempt: 1, maxAttempts: 5, enqueuedAt: Date.now() });
    await expect.poll(async () => (await h.queue.stats()).deadLettered, { timeout: 5_000 }).toBe(1);
    expect((await h.queue.stats()).counters.retries ?? 0).toBe(0);
  });
});

describe('idempotency under duplicate delivery', () => {
  it('skips a redelivered message for a job that already completed', async () => {
    const counter = countingJob();
    const h = await harness({ count: counter.def });
    await h.startWorker('w1');
    const id = await h.enqueue('count', {});
    await h.waitFor(id);

    // Simulate the broker delivering the same job again.
    await h.queue.enqueue({ id, type: 'count', payload: {}, attempt: 1, maxAttempts: 5, enqueuedAt: Date.now() });
    await new Promise((r) => setTimeout(r, 300));

    expect(counter.runs.get(id)).toBe(1);
    expect((await h.queue.stats()).counters.duplicates_skipped).toBe(1);
  });

  it('a crash after completion but before ACK does not re-run the job', async () => {
    const counter = countingJob();
    const h = await harness({ count: counter.def });
    // This worker "dies" right after marking the job done, before acking.
    const doomed = await h.startWorker('doomed', {
      config: { chaosCrashRate: 1, concurrency: 1 },
      crash: () => {
        throw new SimulatedCrash();
      },
    });
    const id = await h.enqueue('count', {});
    await h.waitFor(id, ['succeeded']);
    await doomed.stop();
    expect((await h.queue.stats()).inFlight).toBe(1); // never acked

    // A healthy worker reclaims the orphaned message once it has been idle for
    // claimIdleMs (either in its startup pass or in this explicit pass).
    await new Promise((r) => setTimeout(r, 150));
    const healthy = await h.startWorker('healthy');
    await new Promise((r) => setTimeout(r, 100));
    await healthy.reclaimOrphans();

    expect(counter.runs.get(id)).toBe(1);
    const stats = await h.queue.stats();
    expect(stats.inFlight).toBe(0);
    expect(stats.counters.duplicates_skipped).toBe(1);
    expect(stats.counters.redeliveries).toBe(1);
  });

  it('never reclaims a slow job while its worker is alive and heartbeating', async () => {
    const h = await harness();
    await h.startWorker('slow', { config: { concurrency: 1 } });
    const id = await h.enqueue('sleep', { ms: 600 }); // 6x claimIdleMs, 10x lockTtlMs
    await h.waitFor(id, ['processing']);
    const rescuer = await h.startWorker('rescuer', { config: { concurrency: 1 } });
    for (let i = 0; i < 4; i++) {
      await new Promise((r) => setTimeout(r, 120));
      await rescuer.reclaimOrphans();
    }
    const job = await h.waitFor(id);
    expect(job.status).toBe('succeeded');
    expect(job.events.filter((e) => e.status === 'processing')).toHaveLength(1);
    expect((await h.queue.stats()).counters.redeliveries ?? 0).toBe(0);
  });

  it('reclaims and runs a message orphaned by a dead consumer', async () => {
    const counter = countingJob();
    const h = await harness({ count: counter.def });
    const id = await h.enqueue('count', {});
    // A consumer reads the message and then disappears without acking.
    const ghost = h.redis.duplicate();
    expect(await h.queue.read(ghost, 'ghost', 1, 10)).toHaveLength(1);
    ghost.disconnect();

    const w = await h.startWorker('rescuer', { config: { concurrency: 1 } });
    await new Promise((r) => setTimeout(r, 150));
    await w.reclaimOrphans();
    expect((await h.waitFor(id)).status).toBe('succeeded');
    expect(counter.runs.get(id)).toBe(1);
  });

  it('dead-letters a poison message that keeps getting redelivered', async () => {
    const h = await harness({}, { maxDeliveries: 2 });
    const id = await h.enqueue('sleep', { ms: 1 });
    // Three consumers in a row read it and "crash".
    for (const name of ['c1', 'c2']) {
      const c = h.redis.duplicate();
      if (name === 'c1') await h.queue.read(c, name, 1, 10);
      else {
        await new Promise((r) => setTimeout(r, 120));
        await h.queue.reclaim(name, 100);
      }
      c.disconnect();
    }
    const w = await h.startWorker('w', { config: { concurrency: 1 } });
    await new Promise((r) => setTimeout(r, 120));
    await w.reclaimOrphans();
    const job = await h.waitFor(id, ['failed']);
    expect(job.last_error).toMatch(/exceeded 2 deliveries/);
  });

  it('multiple workers never run the same job twice', async () => {
    const counter = countingJob();
    const h = await harness({ count: counter.def });
    await Promise.all(['a', 'b', 'c'].map((n) => h.startWorker(n, { config: { concurrency: 3 } })));
    const ids: string[] = [];
    for (let i = 0; i < 6; i++) ids.push(...(await Promise.all(Array.from({ length: 25 }, () => h.enqueue('count', {})))));
    await Promise.all(ids.map((id) => h.waitFor(id, ['succeeded'], 15_000)));
    expect([...counter.runs.values()].every((n) => n === 1)).toBe(true);
    expect(counter.runs.size).toBe(150);
  });
});
