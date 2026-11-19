import { randomUUID } from 'node:crypto';
import type { AddressInfo } from 'node:net';
import type { Redis } from 'ioredis';
import { createRedis } from '../src/redis.js';
import { createApp } from '../src/api.js';
import { loadConfig, type Config } from '../src/config.js';
import { JobRepo } from '../src/db.js';
import type { JobDefinition } from '../src/jobs/handlers.js';
import { Queue } from '../src/queue.js';
import type { JobRow } from '../src/types.js';
import { Worker, type WorkerDeps } from '../src/worker.js';

/** Fast timings so retry/backoff/reclaim tests finish in milliseconds. */
export function testConfig(overrides: Partial<Config> = {}): Config {
  return loadConfig({
    keyPrefix: `test:${randomUUID()}`,
    concurrency: 2,
    blockMs: 50,
    backoffBaseMs: 20,
    backoffMaxMs: 200,
    jobTimeoutMs: 2_000,
    lockTtlMs: 60,
    claimIdleMs: 100,
    claimIntervalMs: 60_000, // tests trigger reclaim explicitly
    promoteIntervalMs: 20,
    ...overrides,
  });
}

export class Harness {
  readonly redis: Redis;
  readonly repo: JobRepo;
  readonly queue: Queue;
  readonly workers: Worker[] = [];
  private server?: ReturnType<ReturnType<typeof createApp>['listen']>;
  baseUrl = '';

  constructor(
    readonly config: Config,
    readonly registry: Record<string, JobDefinition>,
  ) {
    this.redis = createRedis(config.redisUrl);
    this.repo = JobRepo.connect(config.databaseUrl);
    this.queue = new Queue(this.redis, config.keyPrefix, config.consumerGroup);
  }

  async setup(): Promise<this> {
    await this.repo.migrate();
    await this.queue.init();
    const app = createApp({ config: this.config, queue: this.queue, repo: this.repo, registry: this.registry });
    await new Promise<void>((resolve) => {
      this.server = app.listen(0, () => resolve());
    });
    this.baseUrl = `http://127.0.0.1:${(this.server!.address() as AddressInfo).port}`;
    return this;
  }

  async startWorker(name: string, extra: Partial<WorkerDeps> & { config?: Partial<Config> } = {}): Promise<Worker> {
    const w = new Worker({
      queue: this.queue,
      repo: this.repo,
      registry: this.registry,
      ...extra,
      config: { ...this.config, workerId: name, ...extra.config },
    });
    await w.start();
    this.workers.push(w);
    return w;
  }

  async post(path: string, body: unknown, headers: Record<string, string> = {}) {
    const res = await fetch(this.baseUrl + path, {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...headers },
      body: JSON.stringify(body),
    });
    return { status: res.status, body: (await res.json()) as any };
  }

  async enqueue(type: string, payload: unknown, opts: { maxAttempts?: number } = {}): Promise<string> {
    const res = await this.post('/jobs', { type, payload, ...opts });
    if (res.status !== 202) throw new Error(`enqueue failed: ${res.status} ${JSON.stringify(res.body)}`);
    return res.body.id;
  }

  async waitFor(id: string, statuses: string[] = ['succeeded', 'failed'], timeoutMs = 5_000): Promise<JobRow & { events: any[] }> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const job = await this.repo.get(id);
      if (job && statuses.includes(job.status)) return job;
      if (Date.now() > deadline) throw new Error(`job ${id} stuck in ${job?.status}`);
      await new Promise((r) => setTimeout(r, 15));
    }
  }

  async teardown(): Promise<void> {
    await Promise.all(this.workers.map((w) => w.stop()));
    await new Promise((r) => (this.server ? this.server.close(r) : r(null)));
    const keys = await this.redis.keys(`${this.config.keyPrefix}:*`);
    if (keys.length) await this.redis.del(...keys);
    await this.redis.quit();
    await this.repo.close();
  }
}

/** A registry entry that counts how many times each job ID actually executed. */
export function countingJob(failFirst = 0) {
  const runs = new Map<string, number>();
  const def: JobDefinition<{ fail?: number }> = {
    schema: { safeParse: (v: unknown) => ({ success: true, data: v ?? {} }) } as any,
    async run(p, ctx) {
      runs.set(ctx.jobId, (runs.get(ctx.jobId) ?? 0) + 1);
      if (ctx.attempt <= (p.fail ?? failFirst)) throw new Error(`fail #${ctx.attempt}`);
      return { ok: true };
    },
  };
  return { def, runs };
}
