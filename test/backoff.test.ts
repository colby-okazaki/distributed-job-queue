import { describe, expect, it } from 'vitest';
import { backoffDelay } from '../src/backoff.js';

describe('backoffDelay', () => {
  it('doubles per attempt (upper bound of the jitter window)', () => {
    const max = () => 1;
    expect([1, 2, 3, 4].map((a) => backoffDelay(a, 1000, 60_000, max))).toEqual([1000, 2000, 4000, 8000]);
  });

  it('never drops below half the exponential delay', () => {
    const min = () => 0;
    expect([1, 2, 3].map((a) => backoffDelay(a, 1000, 60_000, min))).toEqual([500, 1000, 2000]);
  });

  it('is capped at maxMs', () => {
    expect(backoffDelay(30, 1000, 60_000, () => 1)).toBe(60_000);
  });
});
