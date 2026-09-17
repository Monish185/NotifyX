import { describe, it, expect, beforeEach, afterAll } from 'vitest';
import { Redis } from 'ioredis';
import { TokenBucketLimiter } from '../src/token-bucket.js';
import { NotificationQuotaLimiter } from '../src/notification-quota.js';
import { disconnectRateLimitRedis } from '../src/redis-client.js';

describe('TokenBucketLimiter & NotificationQuotaLimiter', () => {
  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  const testRedis = new Redis(redisUrl, { maxRetriesPerRequest: 1 });
  const tokenLimiter = new TokenBucketLimiter(testRedis);
  const quotaLimiter = new NotificationQuotaLimiter(testRedis);

  const testTenant = `test_tenant_${Date.now()}`;

  beforeEach(async () => {
    await tokenLimiter.reset(testTenant);
    await quotaLimiter.reset(testTenant);
  });

  afterAll(async () => {
    await tokenLimiter.reset(testTenant);
    await quotaLimiter.reset(testTenant);
    testRedis.disconnect();
    await disconnectRateLimitRedis();
  });

  describe('TokenBucketLimiter', () => {
    it('allows requests up to burstCapacity', async () => {
      const settings = {
        requestsPerSecond: 5,
        burstCapacity: 3,
        notificationsPerMinute: 100,
        notificationsPerDay: 1000,
        enabled: true,
      };

      // 1st request
      const r1 = await tokenLimiter.consume(testTenant, settings);
      expect(r1.allowed).toBe(true);
      expect(r1.remaining).toBe(2);

      // 2nd request
      const r2 = await tokenLimiter.consume(testTenant, settings);
      expect(r2.allowed).toBe(true);
      expect(r2.remaining).toBe(1);

      // 3rd request
      const r3 = await tokenLimiter.consume(testTenant, settings);
      expect(r3.allowed).toBe(true);
      expect(r3.remaining).toBe(0);

      // 4th request (should be rejected)
      const r4 = await tokenLimiter.consume(testTenant, settings);
      expect(r4.allowed).toBe(false);
      expect(r4.retryAfterMs).toBeGreaterThan(0);
    });

    it('refills tokens over elapsed time', async () => {
      const settings = {
        requestsPerSecond: 10, // 1 token every 100ms
        burstCapacity: 2,
        notificationsPerMinute: 100,
        notificationsPerDay: 1000,
        enabled: true,
      };

      // Exhaust tokens
      await tokenLimiter.consume(testTenant, settings);
      await tokenLimiter.consume(testTenant, settings);
      const exhausted = await tokenLimiter.consume(testTenant, settings);
      expect(exhausted.allowed).toBe(false);

      // Wait 150ms for at least 1 token refill
      await new Promise((resolve) => setTimeout(resolve, 150));

      const refilled = await tokenLimiter.consume(testTenant, settings);
      expect(refilled.allowed).toBe(true);
    });

    it('handles concurrent requests atomically without overshoots', async () => {
      const settings = {
        requestsPerSecond: 10,
        burstCapacity: 10,
        notificationsPerMinute: 100,
        notificationsPerDay: 1000,
        enabled: true,
      };

      // Fire 20 concurrent requests
      const promises = Array.from({ length: 20 }, () =>
        tokenLimiter.consume(testTenant, settings)
      );
      const results = await Promise.all(promises);

      const allowedCount = results.filter((r) => r.allowed).length;
      const rejectedCount = results.filter((r) => !r.allowed).length;

      // Burst capacity is 10, so exactly 10 (or 11 if slight refill during execution) must be allowed
      expect(allowedCount).toBeGreaterThanOrEqual(10);
      expect(allowedCount).toBeLessThanOrEqual(11);
      expect(rejectedCount).toBeGreaterThanOrEqual(9);
    });
  });

  describe('NotificationQuotaLimiter', () => {
    it('enforces per-minute notification quota', async () => {
      const settings = {
        requestsPerSecond: 50,
        burstCapacity: 100,
        notificationsPerMinute: 3,
        notificationsPerDay: 1000,
        enabled: true,
      };

      const q1 = await quotaLimiter.consume(testTenant, settings);
      expect(q1.allowed).toBe(true);

      const q2 = await quotaLimiter.consume(testTenant, settings);
      expect(q2.allowed).toBe(true);

      const q3 = await quotaLimiter.consume(testTenant, settings);
      expect(q3.allowed).toBe(true);

      // 4th request exceeds minute limit of 3
      const q4 = await quotaLimiter.consume(testTenant, settings);
      expect(q4.allowed).toBe(false);
      expect(q4.quotaType).toBe('minute');
      expect(q4.retryAfterMs).toBeGreaterThan(0);
    });

    it('enforces per-day notification quota', async () => {
      const settings = {
        requestsPerSecond: 50,
        burstCapacity: 100,
        notificationsPerMinute: 100,
        notificationsPerDay: 2,
        enabled: true,
      };

      const q1 = await quotaLimiter.consume(testTenant, settings);
      expect(q1.allowed).toBe(true);

      const q2 = await quotaLimiter.consume(testTenant, settings);
      expect(q2.allowed).toBe(true);

      // 3rd request exceeds day limit of 2
      const q3 = await quotaLimiter.consume(testTenant, settings);
      expect(q3.allowed).toBe(false);
      expect(q3.quotaType).toBe('day');
    });
  });
});
