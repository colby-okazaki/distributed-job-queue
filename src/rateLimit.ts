import type { NextFunction, Request, Response } from 'express';
import type { Redis } from 'ioredis';
import { logger } from './logger.js';

/**
 * Token bucket, evaluated atomically inside Redis.
 *
 * Each client has a bucket holding up to `burst` tokens that refills at
 * `rate` tokens per second. A request spends one token; with none left it's
 * rejected and told how long until the next token arrives.
 *
 * State lives in Redis (not process memory) so every API instance behind a
 * load balancer enforces the same limit, and time comes from Redis TIME so
 * instances with drifting clocks can't hand out extra tokens.
 *
 * KEYS[1] bucket hash. ARGV: rate (tokens/sec), burst, cost.
 * Returns { allowed (0|1), tokens left (floored), ms until enough tokens }.
 */
const TOKEN_BUCKET_LUA = `
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost  = tonumber(ARGV[3])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts     = tonumber(state[2])
if tokens == nil then
  tokens = burst
  ts = now
end

-- Refill for the time elapsed since the last request, capped at the burst size.
local elapsed = math.max(0, now - ts)
tokens = math.min(burst, tokens + elapsed * rate / 1000)

local allowed = 0
local wait = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  wait = math.ceil((cost - tokens) * 1000 / rate)
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
-- An idle bucket refills completely after burst/rate seconds; after that it's
-- indistinguishable from a missing key, so let Redis drop it.
redis.call('PEXPIRE', KEYS[1], math.ceil(burst * 1000 / rate) + 1000)

return { allowed, math.floor(tokens), wait }
`;

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  /** ms until a request would be allowed (0 if allowed now). */
  retryAfterMs: number;
}

export class RateLimiter {
  constructor(
    private redis: Redis,
    private prefix: string,
    readonly ratePerSec: number,
    readonly burst: number,
  ) {
    if (ratePerSec <= 0 || burst < 1) throw new Error('rate limiter needs ratePerSec > 0 and burst >= 1');
  }

  async take(clientId: string, cost = 1): Promise<RateLimitResult> {
    const [allowed, remaining, wait] = (await this.redis.eval(
      TOKEN_BUCKET_LUA, 1, `${this.prefix}:rl:${clientId}`, this.ratePerSec, this.burst, cost,
    )) as [number, number, number];
    return { allowed: allowed === 1, remaining, retryAfterMs: wait };
  }
}

const CLIENT_ID = /^[A-Za-z0-9._:-]{1,64}$/;

/**
 * Who a request counts against. X-Client-Id stands in for an API key here;
 * in production it would come from authentication, since a client could
 * otherwise rotate the header to dodge its limit. Falls back to the source IP.
 */
export function clientIdOf(req: Request): string {
  const header = req.header('x-client-id');
  if (header && CLIENT_ID.test(header)) return `id:${header}`;
  return `ip:${req.ip ?? 'unknown'}`;
}

export function rateLimitMiddleware(limiter: RateLimiter, onReject: () => Promise<void> = async () => {}) {
  return async (req: Request, res: Response, next: NextFunction) => {
    let result: RateLimitResult;
    try {
      result = await limiter.take(clientIdOf(req));
    } catch (err) {
      // Fail open: if Redis is unreachable the enqueue will fail anyway with a
      // clearer error, and a limiter outage shouldn't take the API down alone.
      logger.error({ err }, 'rate limiter unavailable, allowing request');
      return next();
    }

    // IETF draft RateLimit headers, so well-behaved clients can pace themselves.
    res.setHeader('RateLimit-Limit', String(limiter.burst));
    res.setHeader('RateLimit-Remaining', String(result.remaining));
    res.setHeader('RateLimit-Reset', String(Math.ceil(result.retryAfterMs / 1000)));
    if (result.allowed) return next();

    res.setHeader('Retry-After', String(Math.max(1, Math.ceil(result.retryAfterMs / 1000))));
    await onReject().catch(() => {});
    res.status(429).json({ error: 'rate limit exceeded', retryAfterMs: result.retryAfterMs });
  };
}
