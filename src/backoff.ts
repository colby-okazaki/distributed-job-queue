/**
 * Exponential backoff with "equal jitter".
 *
 * attempt is the 1-based number of the attempt that just failed, so the
 * un-jittered delays are base, 2*base, 4*base, ... capped at maxMs.
 * Half of the delay is fixed and half is random: retries keep the 1s/2s/4s
 * shape, but jobs that failed together (e.g. a downstream outage) don't all
 * come back at the same instant and knock the dependency over again.
 */
export function backoffDelay(
  attempt: number,
  baseMs: number,
  maxMs: number,
  rand: () => number = Math.random,
): number {
  const exp = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
  const half = exp / 2;
  return Math.round(half + rand() * half);
}
