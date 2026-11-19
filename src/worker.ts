import type { Redis } from 'ioredis';
import { backoffDelay } from './backoff.js';
import type { Config } from './config.js';
import type { JobRepo } from './db.js';
import { NonRetryableError, type JobDefinition } from './jobs/handlers.js';
import { logger } from './logger.js';
import type { Queue, StreamMessage } from './queue.js';
import type { JobEnvelope } from './types.js';

/** Thrown by the crash hook in tests to simulate the process dying mid-job. */
export class SimulatedCrash extends Error {}

export interface WorkerDeps {
  config: Config;
  queue: Queue;
  repo: JobRepo;
  registry: Record<string, JobDefinition>;
  /** Called at the "crash point" when chaos fires. Default: kill the process. */
  crash?: () => never;
}

type Outcome = 'succeeded' | 'retrying' | 'failed' | 'duplicate' | 'locked' | 'crashed';

export class Worker {
  private running = false;
  private loops: Promise<void>[] = [];
  private conns: Redis[] = [];
  private timers: NodeJS.Timeout[] = [];
  private maintenanceBusy = false;
  readonly id: string;

  constructor(private deps: WorkerDeps) {
    this.id = deps.config.workerId;
  }

  async start(): Promise<void> {
    const { config, queue } = this.deps;
    await queue.init();
    this.running = true;

    for (let i = 0; i < config.concurrency; i++) {
      // Each loop BLOCKs on XREADGROUP, so each needs its own connection.
      const conn = queue.redis.duplicate();
      this.conns.push(conn);
      this.loops.push(this.consumeLoop(conn, `${this.id}#${i}`));
    }

    // Recover anything a crashed predecessor left behind before waiting for the timer.
    void this.reclaimOrphans();
    this.timers.push(setInterval(() => void this.promote(), config.promoteIntervalMs));
    this.timers.push(setInterval(() => void this.reclaimOrphans(), config.claimIntervalMs));
    logger.info({ worker: this.id, concurrency: config.concurrency }, 'worker started');
  }

  /** Graceful shutdown: stop reading, let in-flight jobs finish, then close connections. */
  async stop(): Promise<void> {
    this.running = false;
    this.timers.forEach(clearInterval);
    await Promise.all(this.loops);
    await Promise.all(this.conns.map((c) => c.quit().catch(() => c.disconnect())));
    logger.info({ worker: this.id }, 'worker stopped');
  }

  private async consumeLoop(conn: Redis, consumer: string): Promise<void> {
    const { queue, config } = this.deps;
    while (this.running) {
      try {
        const msgs = await queue.read(conn, consumer, 1, config.blockMs);
        for (const m of msgs) await this.process(m, consumer);
      } catch (err) {
        if (!this.running) break;
        logger.error({ err, consumer }, 'consume loop error');
        await new Promise((r) => setTimeout(r, 500));
      }
    }
  }

  private async promote(): Promise<void> {
    try {
      const n = await this.deps.queue.promoteDue();
      if (n) logger.debug({ n }, 'promoted delayed jobs');
    } catch (err) {
      logger.error({ err }, 'promote failed');
    }
  }

  /**
   * Crash recovery. A worker that dies between XREADGROUP and XACK leaves the
   * message in the group's pending list forever. Every worker periodically
   * claims messages that have been pending longer than claimIdleMs and runs
   * them again.
   */
  async reclaimOrphans(): Promise<number> {
    if (this.maintenanceBusy || !this.running) return 0;
    this.maintenanceBusy = true;
    const { queue, config } = this.deps;
    const consumer = `${this.id}#reclaim`;
    try {
      const msgs = await queue.reclaim(consumer, config.claimIdleMs);
      for (const m of msgs) {
        await queue.incr('redeliveries');
        const deliveries = await queue.deliveryCount(m.msgId);
        if (deliveries > config.maxDeliveries) {
          // Poison pill: something about this job kills workers. Stop the loop.
          const job = this.decode(m);
          const lastError = `exceeded ${config.maxDeliveries} deliveries without an ack (worker crash loop?)`;
          await queue.deadLetter(m.msgId, job ? { ...job, lastError, failedAt: Date.now() } : { raw: m.fields, lastError, failedAt: Date.now() });
          if (job) await this.deps.repo.transition(job.id, 'failed', job.attempt, { worker: this.id, error: lastError });
          await queue.incr('failed');
          continue;
        }
        logger.warn({ msgId: m.msgId, deliveries }, 'reclaimed orphaned message');
        await this.process(m, consumer);
      }
      return msgs.length;
    } catch (err) {
      logger.error({ err }, 'reclaim failed');
      return 0;
    } finally {
      this.maintenanceBusy = false;
    }
  }

  private decode(m: StreamMessage): JobEnvelope | null {
    try {
      const job = JSON.parse(m.fields.job ?? '') as JobEnvelope;
      return job && typeof job.id === 'string' && typeof job.type === 'string' ? job : null;
    } catch {
      return null;
    }
  }

  /** Process one delivery. Public so tests can drive it directly. */
  async process(m: StreamMessage, consumer: string): Promise<Outcome> {
    const { queue, repo, config, registry } = this.deps;
    const job = this.decode(m);
    if (!job) {
      await queue.deadLetter(m.msgId, { raw: m.fields, lastError: 'undecodable message', failedAt: Date.now() });
      await queue.incr('failed');
      return 'failed';
    }
    const log = logger.child({ jobId: job.id, type: job.type, attempt: job.attempt, consumer });

    // 1. Idempotency check. At-least-once delivery means we WILL see some jobs
    //    twice (reclaim after a crash, a retry that raced an ack, a manual
    //    replay). Redis is the fast path; Postgres covers markers past their TTL.
    if ((await queue.isDone(job.id)) || (await repo.isSucceeded(job.id))) {
      await queue.ack(m.msgId);
      await queue.incr('duplicates_skipped');
      log.warn('duplicate delivery of completed job, skipping');
      return 'duplicate';
    }

    // 2. Lease. If another live worker holds it, it's still running this job:
    //    leave the message pending and let that worker ack it.
    if (!(await queue.acquireLock(job.id, consumer, config.lockTtlMs))) {
      await queue.incr('lock_contention');
      log.warn('job is locked by another worker, skipping');
      return 'locked';
    }

    let crashed = false;
    // Keep the lease (and the message's idle clock) fresh while the job runs.
    // If this process dies, heartbeats stop and the lease expires within lockTtlMs.
    const hb = setInterval(() => {
      queue
        .heartbeat(job.id, consumer, config.lockTtlMs, m.msgId)
        .then((ok) => ok || log.error('lost lease while running; another worker may pick this job up'))
        .catch((err) => log.error({ err }, 'heartbeat failed'));
    }, Math.max(10, Math.floor(config.lockTtlMs / 3)));
    try {
      // Re-check under the lease: another worker may have finished the job
      // and released the lock between our step-1 check and our SET NX.
      if (await queue.isDone(job.id)) {
        await queue.ack(m.msgId);
        await queue.incr('duplicates_skipped');
        return 'duplicate';
      }
      await repo.transition(job.id, 'processing', job.attempt, { worker: consumer });
      const started = performance.now();
      let result: unknown;
      try {
        result = await this.runHandler(job, registry[job.type]);
      } catch (err) {
        return await this.onFailure(m.msgId, job, err as Error, consumer);
      }
      const durationMs = Math.round(performance.now() - started);

      // 3. Record success BEFORE acking. If we die after this line, the
      //    redelivered message hits the check in step 1 instead of re-running.
      await queue.markDone(job.id, config.doneTtlSec);
      await repo.transition(job.id, 'succeeded', job.attempt, { worker: consumer, result: result ?? null, durationMs });
      await queue.incr('succeeded');
      if (job.attempt > 1) await queue.incr('succeeded_after_retry');

      if (config.chaosCrashRate > 0 && Math.random() < config.chaosCrashRate) {
        log.error('CHAOS: crashing after marking done, before ack');
        crashed = true;
        (this.deps.crash ?? (() => process.exit(1)))();
      }

      await queue.ack(m.msgId);
      log.info({ durationMs }, 'job succeeded');
      return 'succeeded';
    } catch (err) {
      if (err instanceof SimulatedCrash) return 'crashed';
      throw err;
    } finally {
      clearInterval(hb);
      // A real crash never reaches here, and the lease simply expires.
      if (!crashed) await queue.releaseLock(job.id, consumer);
    }
  }

  private async runHandler(job: JobEnvelope, def: JobDefinition | undefined): Promise<unknown> {
    if (!def) throw new NonRetryableError(`unknown job type "${job.type}"`);
    const parsed = def.schema.safeParse(job.payload);
    if (!parsed.success) throw new NonRetryableError(`invalid payload: ${parsed.error.issues.map((i) => i.message).join('; ')}`);

    const ac = new AbortController();
    const timeoutMs = this.deps.config.jobTimeoutMs;
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        const err = new Error(`job timed out after ${timeoutMs}ms`);
        ac.abort(err);
        reject(err);
      }, timeoutMs);
    });
    try {
      return await Promise.race([def.run(parsed.data, { jobId: job.id, attempt: job.attempt, signal: ac.signal }), timeout]);
    } finally {
      clearTimeout(timer);
    }
  }

  private async onFailure(msgId: string, job: JobEnvelope, err: Error, consumer: string): Promise<Outcome> {
    const { queue, repo, config } = this.deps;
    const error = err.message || String(err);
    const retryable = !(err instanceof NonRetryableError);

    if (retryable && job.attempt < job.maxAttempts) {
      const delay = backoffDelay(job.attempt, config.backoffBaseMs, config.backoffMaxMs);
      // Postgres first: if the Redis step fails, the message stays pending and
      // is reclaimed later, which only means the history gets an extra row.
      // (run_at here is informational; the authoritative time is Redis's clock.)
      await repo.transition(job.id, 'retrying', job.attempt, { worker: consumer, error, runAt: new Date(Date.now() + delay) });
      await queue.scheduleRetry(msgId, { ...job, attempt: job.attempt + 1, lastError: error }, delay);
      await queue.incr('retries');
      logger.warn({ jobId: job.id, attempt: job.attempt, delay, error }, 'job failed, retrying with backoff');
      return 'retrying';
    }

    await repo.transition(job.id, 'failed', job.attempt, { worker: consumer, error });
    await queue.deadLetter(msgId, { ...job, lastError: error, failedAt: Date.now() });
    await queue.incr('failed');
    logger.error({ jobId: job.id, attempt: job.attempt, retryable, error }, 'job moved to dead-letter queue');
    return 'failed';
  }
}
