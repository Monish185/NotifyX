import { describe, it, expect } from 'vitest';
import { calculateRetryDelay } from '../src/backoff.js';

describe('calculateRetryDelay (Exponential Backoff with Jitter)', () => {
  it('calculates expected exponential delays with neutral jitter (random = 0.5)', () => {
    const neutralRandom = () => 0.5;

    // attempt 1: 5000 * 2^0 = 5000
    expect(calculateRetryDelay({ attempt: 1, random: neutralRandom })).toBe(5000);

    // attempt 2: 5000 * 2^1 = 10000
    expect(calculateRetryDelay({ attempt: 2, random: neutralRandom })).toBe(10000);

    // attempt 3: 5000 * 2^2 = 20000
    expect(calculateRetryDelay({ attempt: 3, random: neutralRandom })).toBe(20000);

    // attempt 4: 5000 * 2^3 = 40000
    expect(calculateRetryDelay({ attempt: 4, random: neutralRandom })).toBe(40000);

    // attempt 5: 5000 * 2^4 = 80000
    expect(calculateRetryDelay({ attempt: 5, random: neutralRandom })).toBe(80000);
  });

  it('bounds jitter strictly between (1 - jitterRatio) and (1 + jitterRatio)', () => {
    const minRandom = () => 0.0;
    const maxRandom = () => 1.0;

    // base 10000, jitterRatio 0.5 -> range is [5000, 15000]
    const minDelay = calculateRetryDelay({
      attempt: 2, // 5000 * 2^1 = 10000
      random: minRandom,
      jitterRatio: 0.5,
    });
    expect(minDelay).toBe(5000);

    const maxDelay = calculateRetryDelay({
      attempt: 2, // 10000
      random: maxRandom,
      jitterRatio: 0.5,
    });
    expect(maxDelay).toBe(15000);
  });

  it('clamps delay to maxDelayMs', () => {
    // attempt 20 without cap would be 5000 * 2^19 = 2,621,440,000 ms
    const cappedDelay = calculateRetryDelay({
      attempt: 20,
      baseDelayMs: 5000,
      maxDelayMs: 60000, // 1 minute cap
      random: () => 0.5,
    });

    expect(cappedDelay).toBe(60000);
  });

  it('protects against integer overflow with huge attempt numbers', () => {
    const hugeAttemptDelay = calculateRetryDelay({
      attempt: 1000000,
      baseDelayMs: 5000,
      maxDelayMs: 900000,
      random: () => 0.5,
    });

    expect(hugeAttemptDelay).toBe(900000);
    expect(Number.isFinite(hugeAttemptDelay)).toBe(true);
    expect(isNaN(hugeAttemptDelay)).toBe(false);
  });

  it('handles attempt <= 0 or invalid numbers safely', () => {
    const delayZero = calculateRetryDelay({
      attempt: 0,
      random: () => 0.5,
    });
    expect(delayZero).toBe(5000); // normalized to attempt 1

    const delayNegative = calculateRetryDelay({
      attempt: -5,
      random: () => 0.5,
    });
    expect(delayNegative).toBe(5000);
  });
});
