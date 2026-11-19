import { Redis, type RedisOptions } from 'ioredis';

/**
 * Pin RESP2 so stream replies (XREADGROUP, XAUTOCLAIM, XRANGE) come back as
 * plain nested arrays, which is what the parsing in queue.ts expects.
 */
export function createRedis(url: string, opts: RedisOptions = {}): Redis {
  return new Redis(url, { protocol: 2, maxRetriesPerRequest: null, ...opts });
}
