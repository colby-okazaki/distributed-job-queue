import { z } from 'zod';

export interface JobContext {
  jobId: string;
  attempt: number;
  signal: AbortSignal;
}

/** Throw this for failures that will never succeed on retry (bad input, 4xx). Goes straight to the DLQ. */
export class NonRetryableError extends Error {
  override name = 'NonRetryableError';
}

export interface JobDefinition<T = any> {
  schema: z.ZodType<T>;
  run(payload: T, ctx: JobContext): Promise<unknown>;
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);
    const t = setTimeout(resolve, ms);
    signal.addEventListener('abort', () => {
      clearTimeout(t);
      reject(signal.reason);
    }, { once: true });
  });
}

/** Synthetic job for exercising the plumbing: sleep, optionally fail. */
const sleepJob: JobDefinition<{ ms: number; failRate: number; failTimes: number }> = {
  schema: z.object({
    ms: z.number().int().min(0).max(60_000).default(100),
    /** Random failure probability per attempt (transient-error simulation). */
    failRate: z.number().min(0).max(1).default(0),
    /** Deterministically fail the first N attempts. */
    failTimes: z.number().int().min(0).default(0),
  }),
  async run(p, ctx) {
    await sleep(p.ms, ctx.signal);
    if (ctx.attempt <= p.failTimes) throw new Error(`planned failure on attempt ${ctx.attempt}`);
    if (Math.random() < p.failRate) throw new Error('random transient failure');
    return { slept: p.ms };
  },
};

/**
 * Real job: deliver a notification to a webhook. Works for "scheduled
 * notifications" via the API's delayMs. The job ID goes out as the
 * Idempotency-Key header so the receiver can drop duplicates, which covers
 * the one gap worker-side idempotency can't: a crash after the HTTP call
 * succeeded but before we recorded it.
 */
const webhookJob: JobDefinition<{ url: string; event: string; data: unknown; headers: Record<string, string> }> = {
  schema: z.object({
    url: z.string().url(),
    event: z.string().min(1).max(200),
    data: z.unknown().default({}),
    headers: z.record(z.string(), z.string()).default({}),
  }),
  async run(p, ctx) {
    let res: Response;
    try {
      res = await fetch(p.url, {
        method: 'POST',
        signal: ctx.signal,
        headers: {
          'content-type': 'application/json',
          ...p.headers,
          'idempotency-key': ctx.jobId,
          'x-attempt': String(ctx.attempt),
        },
        body: JSON.stringify({ id: ctx.jobId, event: p.event, data: p.data }),
      });
    } catch (err) {
      // Network errors, DNS, connection refused, timeout: worth retrying.
      throw new Error(`webhook request failed: ${(err as Error).message}`);
    }
    if (res.ok) return { status: res.status };
    const msg = `webhook responded ${res.status}`;
    // 408 / 429 / 5xx are transient. Any other 4xx means the request itself is wrong.
    if (res.status >= 400 && res.status < 500 && res.status !== 408 && res.status !== 429) {
      throw new NonRetryableError(msg);
    }
    throw new Error(msg);
  },
};

export const registry: Record<string, JobDefinition> = {
  sleep: sleepJob,
  'webhook.deliver': webhookJob,
};
