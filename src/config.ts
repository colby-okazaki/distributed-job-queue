import os from 'node:os';

export interface Config {
  redisUrl: string;
  databaseUrl: string;
  port: number;
  /** Prefix for every Redis key, so tests / environments can share one Redis. */
  keyPrefix: string;
  consumerGroup: string;
  /** Unique name of this worker process inside the consumer group. */
  workerId: string;
  /** Parallel consumer loops per worker process. */
  concurrency: number;
  blockMs: number;

  defaultMaxAttempts: number;
  backoffBaseMs: number;
  backoffMaxMs: number;
  jobTimeoutMs: number;

  /** Lease held while a job runs, renewed by heartbeat every lockTtlMs/3. */
  lockTtlMs: number;
  /**
   * A message pending this long without a heartbeat is presumed orphaned and
   * reclaimed. Must exceed lockTtlMs so a dead worker's lease has expired by then.
   */
  claimIdleMs: number;
  claimIntervalMs: number;
  promoteIntervalMs: number;
  /** Delivered more than this many times without an ack => poison pill => DLQ. */
  maxDeliveries: number;
  /** How long the "already processed" marker lives in Redis. Postgres is the durable fallback. */
  doneTtlSec: number;

  /** Probability that a worker crashes after marking a job done but before ACKing it (demo only). */
  chaosCrashRate: number;
}

function num(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const n = Number(raw);
  if (Number.isNaN(n)) throw new Error(`Env var ${name} must be a number, got "${raw}"`);
  return n;
}

export function loadConfig(overrides: Partial<Config> = {}): Config {
  const cfg: Config = {
    redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
    databaseUrl: process.env.DATABASE_URL ?? 'postgres://jobs:jobs@localhost:5432/jobs',
    port: num('PORT', 3000),
    keyPrefix: process.env.KEY_PREFIX ?? 'jq',
    consumerGroup: process.env.CONSUMER_GROUP ?? 'workers',
    workerId: process.env.WORKER_ID ?? `${os.hostname()}-${process.pid}`,
    concurrency: num('WORKER_CONCURRENCY', 4),
    blockMs: num('BLOCK_MS', 1000),

    defaultMaxAttempts: num('DEFAULT_MAX_ATTEMPTS', 5),
    backoffBaseMs: num('BACKOFF_BASE_MS', 1000),
    backoffMaxMs: num('BACKOFF_MAX_MS', 60_000),
    jobTimeoutMs: num('JOB_TIMEOUT_MS', 30_000),

    lockTtlMs: num('LOCK_TTL_MS', 15_000),
    claimIdleMs: num('CLAIM_IDLE_MS', 30_000),
    claimIntervalMs: num('CLAIM_INTERVAL_MS', 5_000),
    promoteIntervalMs: num('PROMOTE_INTERVAL_MS', 250),
    maxDeliveries: num('MAX_DELIVERIES', 10),
    doneTtlSec: num('DONE_TTL_SEC', 86_400),

    chaosCrashRate: num('CHAOS_CRASH_RATE', 0),
    ...overrides,
  };
  if (cfg.claimIdleMs <= cfg.lockTtlMs) {
    throw new Error(
      'CLAIM_IDLE_MS must be greater than LOCK_TTL_MS, or orphaned jobs get reclaimed while the dead worker still holds the lease',
    );
  }
  return cfg;
}
