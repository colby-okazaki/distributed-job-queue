import { randomUUID } from 'node:crypto';
import express, { type NextFunction, type Request, type Response } from 'express';
import { z } from 'zod';
import type { Config } from './config.js';
import type { JobRepo } from './db.js';
import type { JobDefinition } from './jobs/handlers.js';
import { logger } from './logger.js';
import type { Queue } from './queue.js';
import { RateLimiter, rateLimitMiddleware } from './rateLimit.js';
import type { JobEnvelope } from './types.js';

export interface ApiDeps {
  config: Config;
  queue: Queue;
  repo: JobRepo;
  registry: Record<string, JobDefinition>;
}

const EnqueueBody = z.object({
  type: z.string().min(1),
  payload: z.unknown().default({}),
  maxAttempts: z.number().int().min(1).max(25).optional(),
  /** Run no earlier than this many ms from now (scheduled jobs). */
  delayMs: z.number().int().min(0).max(30 * 24 * 3600 * 1000).optional(),
});

const UUID = z.string().uuid();

export function createApp({ config, queue, repo, registry }: ApiDeps) {
  const app = express();
  app.use(express.json({ limit: '256kb' }));

  app.get('/health', async (_req, res) => {
    await Promise.all([queue.redis.ping(), repo.pool.query('SELECT 1')]);
    res.json({ ok: true });
  });

  const limitJobs =
    config.rateLimitPerSec > 0
      ? rateLimitMiddleware(new RateLimiter(queue.redis, config.keyPrefix, config.rateLimitPerSec, config.rateLimitBurst), () =>
          queue.incr('rate_limited'),
        )
      : (_req: Request, _res: Response, next: NextFunction) => next();

  app.post('/jobs', limitJobs, async (req, res) => {
    const body = EnqueueBody.safeParse(req.body);
    if (!body.success) return res.status(400).json({ error: 'invalid body', issues: body.error.issues });
    const { type, delayMs } = body.data;

    const def = registry[type];
    if (!def) return res.status(400).json({ error: `unknown job type "${type}"`, known: Object.keys(registry) });
    // Validate at the edge so bad input fails fast with a 400 instead of
    // burning through retries and landing in the DLQ.
    const payload = def.schema.safeParse(body.data.payload);
    if (!payload.success) return res.status(400).json({ error: 'invalid payload', issues: payload.error.issues });

    const idempotencyKey = req.header('idempotency-key') || undefined;
    if (idempotencyKey && idempotencyKey.length > 255) return res.status(400).json({ error: 'Idempotency-Key too long' });

    const maxAttempts = body.data.maxAttempts ?? config.defaultMaxAttempts;
    const runAt = delayMs ? new Date(Date.now() + delayMs) : undefined;
    const { job, created } = await repo.create({ id: randomUUID(), type, payload: payload.data, maxAttempts, idempotencyKey, runAt });
    if (!created) return res.status(200).json({ id: job.id, status: job.status, deduplicated: true });

    const envelope: JobEnvelope = { id: job.id, type, payload: payload.data, attempt: 1, maxAttempts, enqueuedAt: Date.now() };
    try {
      // Postgres row first so a worker that picks the job up instantly can find it.
      await queue.enqueue(envelope, delayMs ?? 0);
    } catch (err) {
      // The dual-write gap: row exists, message doesn't. Mark it failed so it
      // isn't silently stuck in "pending". (A transactional outbox would close this fully.)
      logger.error({ err, jobId: job.id }, 'enqueue to redis failed');
      await repo.transition(job.id, 'failed', 0, { error: 'failed to enqueue' }).catch(() => {});
      return res.status(503).json({ error: 'queue unavailable', id: job.id });
    }
    res.status(202).location(`/jobs/${job.id}`).json({ id: job.id, status: job.status });
  });

  app.get('/jobs', async (req, res) => {
    const q = z
      .object({ status: z.string().optional(), type: z.string().optional(), limit: z.coerce.number().int().min(1).max(500).optional() })
      .parse(req.query);
    res.json(await repo.list(q));
  });

  app.get('/jobs/:id', async (req, res) => {
    const id = UUID.safeParse(req.params.id);
    if (!id.success) return res.status(400).json({ error: 'invalid job id' });
    const job = await repo.get(id.data);
    if (!job) return res.status(404).json({ error: 'not found' });
    res.json(job);
  });

  app.get('/stats', async (_req, res) => {
    const [queueStats, byStatus] = await Promise.all([queue.stats(), repo.countByStatus()]);
    res.json({ queue: queueStats, jobs: byStatus });
  });

  app.get('/dlq', async (req, res) => {
    const limit = z.coerce.number().int().min(1).max(500).default(50).parse(req.query.limit);
    res.json(await queue.listDlq(limit));
  });

  app.post('/dlq/:id/replay', async (req, res) => {
    const id = UUID.safeParse(req.params.id);
    if (!id.success) return res.status(400).json({ error: 'invalid job id' });
    const replayed = await queue.replayDlq(id.data);
    if (!replayed) return res.status(404).json({ error: 'job not in dead-letter queue' });
    await repo.transition(id.data, 'pending', 0, {}, 'replayed');
    await queue.incr('replayed');
    res.status(202).json({ id: id.data, status: 'pending', replayed: true });
  });

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof z.ZodError) return res.status(400).json({ error: 'invalid request', issues: err.issues });
    logger.error({ err }, 'unhandled api error');
    res.status(500).json({ error: 'internal error' });
  });

  return app;
}
