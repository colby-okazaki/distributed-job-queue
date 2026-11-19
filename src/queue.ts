import type { Redis } from 'ioredis';
import type { JobEnvelope } from './types.js';

export interface Keys {
  stream: string;
  delayed: string;
  dlq: string;
  metrics: string;
  done(id: string): string;
  lock(id: string): string;
}

export function makeKeys(prefix: string): Keys {
  return {
    stream: `${prefix}:stream`, // ready jobs (Redis Stream + consumer group)
    delayed: `${prefix}:delayed`, // retries / scheduled jobs (ZSET scored by run-at ms)
    dlq: `${prefix}:dlq`, // dead-lettered jobs (Stream)
    metrics: `${prefix}:metrics`, // counters (HASH)
    done: (id) => `${prefix}:done:${id}`, // idempotency marker
    lock: (id) => `${prefix}:lock:${id}`, // execution lease
  };
}

export interface StreamMessage {
  msgId: string;
  fields: Record<string, string>;
}

export interface QueueStats {
  /** Messages in the stream not yet delivered to any consumer. */
  ready: number;
  /** Delivered to a consumer but not yet ACKed (in flight, or orphaned by a crash). */
  inFlight: number;
  /** Waiting in the delayed set (backoff or scheduled). */
  delayed: number;
  deadLettered: number;
  counters: Record<string, number>;
}

/**
 * All scheduling uses the Redis server clock, never the caller's Date.now().
 * With several API/worker hosts, their clocks drift; if the process that
 * scores a retry and the one that promotes it disagree by minutes, the
 * retry waits minutes longer (or fires early). One clock removes that.
 */
const REDIS_NOW_LUA = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
`;

/**
 * Moves due jobs from the delayed ZSET into the stream. Runs as one atomic
 * Lua script so two workers promoting at the same moment can't both XADD
 * the same job.
 */
const PROMOTE_LUA = `${REDIS_NOW_LUA}
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
for _, member in ipairs(due) do
  redis.call('ZREM', KEYS[1], member)
  redis.call('XADD', KEYS[2], '*', 'job', member)
end
return #due
`;

/**
 * Park a job in the delayed set at (redis now + delay). If a stream message ID
 * is given, ACK + delete it in the same atomic step (used for retries).
 * KEYS: delayed, stream. ARGV: delayMs, member, group, msgId|''.
 */
const SCHEDULE_LUA = `${REDIS_NOW_LUA}
local runAt = now + tonumber(ARGV[1])
redis.call('ZADD', KEYS[1], runAt, ARGV[2])
if ARGV[4] ~= '' then
  redis.call('XACK', KEYS[2], ARGV[3], ARGV[4])
  redis.call('XDEL', KEYS[2], ARGV[4])
end
return runAt
`;

/**
 * Heartbeat for a running job. If we still own the lease: extend it, and
 * reset the stream message's idle time (XCLAIM ... JUSTID doesn't bump the
 * delivery counter) so XAUTOCLAIM never treats a live-but-slow job as orphaned.
 * KEYS: lock, stream. ARGV: owner, ttlMs, group, msgId.
 */
const HEARTBEAT_LUA = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
redis.call('XCLAIM', KEYS[2], ARGV[3], ARGV[1], 0, ARGV[4], 'JUSTID')
return 1
`;

/** Release a lock only if we still own it (it may have expired and been taken by someone else). */
const RELEASE_LOCK_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

type ExecResult = [Error | null, unknown][] | null;

function check(res: ExecResult): void {
  if (!res) throw new Error('Redis transaction aborted');
  for (const [err] of res) if (err) throw err;
}

function toFields(raw: unknown): Record<string, string> {
  const arr = (raw ?? []) as string[];
  const out: Record<string, string> = {};
  for (let i = 0; i + 1 < arr.length; i += 2) out[arr[i]] = arr[i + 1];
  return out;
}

export function parseEntries(raw: unknown): StreamMessage[] {
  return ((raw ?? []) as [string, unknown][])
    .filter(([, f]) => f !== null)
    .map(([msgId, f]) => ({ msgId, fields: toFields(f) }));
}

export class Queue {
  readonly keys: Keys;

  constructor(
    readonly redis: Redis,
    prefix: string,
    readonly group: string,
  ) {
    this.keys = makeKeys(prefix);
  }

  async init(): Promise<void> {
    try {
      await this.redis.call('XGROUP', 'CREATE', this.keys.stream, this.group, '0', 'MKSTREAM');
    } catch (err) {
      if (!String((err as Error).message).includes('BUSYGROUP')) throw err;
    }
  }

  // ---------- producing ----------

  async enqueue(job: JobEnvelope, delayMs = 0): Promise<void> {
    const body = JSON.stringify(job);
    if (delayMs > 0) {
      await this.redis.eval(SCHEDULE_LUA, 2, this.keys.delayed, this.keys.stream, delayMs, body, this.group, '');
    } else {
      await this.redis.call('XADD', this.keys.stream, '*', 'job', body);
    }
  }

  async promoteDue(limit = 100): Promise<number> {
    return (await this.redis.eval(PROMOTE_LUA, 2, this.keys.delayed, this.keys.stream, limit)) as number;
  }

  // ---------- consuming ----------

  /** Blocking read of new messages. Use a dedicated connection per caller: BLOCK ties it up. */
  async read(conn: Redis, consumer: string, count: number, blockMs: number): Promise<StreamMessage[]> {
    const res = (await conn.call(
      'XREADGROUP', 'GROUP', this.group, consumer,
      'COUNT', count, 'BLOCK', blockMs,
      'STREAMS', this.keys.stream, '>',
    )) as [string, unknown][] | null;
    if (!res) return [];
    return parseEntries(res[0][1]);
  }

  /**
   * Take ownership of messages another consumer received but never ACKed for
   * at least minIdleMs (it probably crashed). This is the main source of
   * duplicate delivery, and the reason workers must be idempotent.
   */
  async reclaim(consumer: string, minIdleMs: number, count = 50): Promise<StreamMessage[]> {
    const out: StreamMessage[] = [];
    let cursor = '0-0';
    do {
      const res = (await this.redis.call(
        'XAUTOCLAIM', this.keys.stream, this.group, consumer, minIdleMs, cursor, 'COUNT', count,
      )) as [string, unknown, unknown];
      cursor = res[0];
      out.push(...parseEntries(res[1]));
    } while (cursor !== '0-0' && out.length < count * 10);
    return out;
  }

  async deliveryCount(msgId: string): Promise<number> {
    const res = (await this.redis.call('XPENDING', this.keys.stream, this.group, msgId, msgId, 1)) as
      | [string, string, number, number][]
      | null;
    return res?.[0]?.[3] ?? 0;
  }

  /** ACK and delete, so XLEN of the stream is the real backlog. */
  async ack(msgId: string): Promise<void> {
    check(await this.redis.multi().xack(this.keys.stream, this.group, msgId).xdel(this.keys.stream, msgId).exec());
  }

  /** Atomically park the next attempt in the delayed set and ACK the failed delivery. Returns run-at (Redis clock). */
  async scheduleRetry(msgId: string, next: JobEnvelope, delayMs: number): Promise<number> {
    return (await this.redis.eval(
      SCHEDULE_LUA, 2, this.keys.delayed, this.keys.stream, delayMs, JSON.stringify(next), this.group, msgId,
    )) as number;
  }

  /** Atomically move a job to the DLQ and ACK it. msgId is null for undecodable messages handled elsewhere. */
  async deadLetter(msgId: string | null, job: JobEnvelope | { raw: unknown; lastError: string; failedAt: number }): Promise<void> {
    const tx = this.redis.multi().xadd(this.keys.dlq, '*', 'job', JSON.stringify(job));
    if (msgId) tx.xack(this.keys.stream, this.group, msgId).xdel(this.keys.stream, msgId);
    check(await tx.exec());
  }

  // ---------- idempotency ----------

  async isDone(jobId: string): Promise<boolean> {
    return (await this.redis.exists(this.keys.done(jobId))) === 1;
  }

  async markDone(jobId: string, ttlSec: number): Promise<void> {
    await this.redis.set(this.keys.done(jobId), '1', 'EX', ttlSec);
  }

  /**
   * Execution lease. Prevents two live workers running the same job at once
   * (e.g. a slow worker whose message got reclaimed by another). SET NX PX is
   * atomic; the TTL means a crashed holder can't block the job forever.
   */
  async acquireLock(jobId: string, owner: string, ttlMs: number): Promise<boolean> {
    return (await this.redis.set(this.keys.lock(jobId), owner, 'PX', ttlMs, 'NX')) === 'OK';
  }

  /** Returns false if the lease was lost (expired and taken by someone else). */
  async heartbeat(jobId: string, owner: string, ttlMs: number, msgId: string): Promise<boolean> {
    return (await this.redis.eval(HEARTBEAT_LUA, 2, this.keys.lock(jobId), this.keys.stream, owner, ttlMs, this.group, msgId)) === 1;
  }

  async releaseLock(jobId: string, owner: string): Promise<void> {
    await this.redis.eval(RELEASE_LOCK_LUA, 1, this.keys.lock(jobId), owner);
  }

  // ---------- metrics ----------

  async incr(field: string, by = 1): Promise<void> {
    await this.redis.hincrby(this.keys.metrics, field, by);
  }

  async stats(): Promise<QueueStats> {
    const [len, pending, delayed, dlq, counters] = await Promise.all([
      this.redis.xlen(this.keys.stream),
      this.redis.call('XPENDING', this.keys.stream, this.group) as Promise<[number, ...unknown[]]>,
      this.redis.zcard(this.keys.delayed),
      this.redis.xlen(this.keys.dlq),
      this.redis.hgetall(this.keys.metrics),
    ]);
    const inFlight = Number(pending[0] ?? 0);
    return {
      ready: Math.max(0, len - inFlight),
      inFlight,
      delayed,
      deadLettered: dlq,
      counters: Object.fromEntries(Object.entries(counters).map(([k, v]) => [k, Number(v)])),
    };
  }

  // ---------- dead-letter queue ----------

  async listDlq(limit = 50): Promise<{ entryId: string; job: unknown }[]> {
    const raw = await this.redis.call('XREVRANGE', this.keys.dlq, '+', '-', 'COUNT', limit);
    return parseEntries(raw).map((m) => ({ entryId: m.msgId, job: JSON.parse(m.fields.job) }));
  }

  /** Remove a job from the DLQ and put it back on the stream with a fresh attempt budget. */
  async replayDlq(jobId: string): Promise<JobEnvelope | null> {
    let start = '-';
    for (;;) {
      const entries = parseEntries(await this.redis.call('XRANGE', this.keys.dlq, start, '+', 'COUNT', 500));
      if (entries.length === 0) return null;
      for (const e of entries) {
        const job = JSON.parse(e.fields.job) as JobEnvelope;
        if (job.id !== jobId) continue;
        const replay: JobEnvelope = { ...job, attempt: 1, enqueuedAt: Date.now() };
        delete replay.failedAt;
        const res = await this.redis
          .multi()
          .xdel(this.keys.dlq, e.msgId)
          .xadd(this.keys.stream, '*', 'job', JSON.stringify(replay))
          .exec();
        check(res);
        return replay;
      }
      start = `(${entries[entries.length - 1].msgId}`;
    }
  }
}
