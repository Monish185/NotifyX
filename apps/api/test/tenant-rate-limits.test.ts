import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { type FastifyInstance } from 'fastify';
import { buildApp } from '../src/app.js';
import { prisma } from '@notifyx/database';
import { tokenBucketLimiter, notificationQuotaLimiter } from '@notifyx/rate-limit';

describe('Tenant Rate Limits & Quota Endpoints', () => {
  let app: FastifyInstance;
  let testTenantId: string;
  let testApiKey: string;
  let otherTenantId: string;
  let otherApiKey: string;

  beforeAll(async () => {
    app = await buildApp();
    await app.ready();

    // Create Test Tenant A
    const tenantA = await prisma.tenant.create({
      data: {
        name: `Tenant RateLimit A ${Date.now()}`,
        slug: `tenant-rl-a-${Date.now()}`,
      },
    });
    testTenantId = tenantA.id;

    const keyARes = await app.inject({
      method: 'POST',
      url: '/v1/api-keys',
      payload: { name: 'Key A', env: 'TEST', tenantId: testTenantId },
    });
    testApiKey = JSON.parse(keyARes.payload).key;

    // Create Test Tenant B
    const tenantB = await prisma.tenant.create({
      data: {
        name: `Tenant RateLimit B ${Date.now()}`,
        slug: `tenant-rl-b-${Date.now()}`,
      },
    });
    otherTenantId = tenantB.id;

    const keyBRes = await app.inject({
      method: 'POST',
      url: '/v1/api-keys',
      payload: { name: 'Key B', env: 'TEST', tenantId: otherTenantId },
    });
    otherApiKey = JSON.parse(keyBRes.payload).key;
  });

  afterAll(async () => {
    await tokenBucketLimiter.reset(testTenantId);
    await notificationQuotaLimiter.reset(testTenantId);
    await tokenBucketLimiter.reset(otherTenantId);
    await notificationQuotaLimiter.reset(otherTenantId);
    await app.close();
  });

  it('GET /v1/tenant/rate-limits returns default limits for a tenant without custom config', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/tenant/rate-limits',
      headers: { authorization: `Bearer ${testApiKey}` },
    });

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.payload);
    expect(body.tenantId).toBe(testTenantId);
    expect(body.requestsPerSecond).toBe(10);
    expect(body.burstCapacity).toBe(20);
    expect(body.notificationsPerMinute).toBe(100);
    expect(body.notificationsPerDay).toBe(10000);
    expect(body.isCustom).toBe(false);
  });

  it('PUT /v1/tenant/rate-limits updates configuration with tenant isolation', async () => {
    const updateRes = await app.inject({
      method: 'PUT',
      url: '/v1/tenant/rate-limits',
      headers: { authorization: `Bearer ${testApiKey}` },
      payload: {
        requestsPerSecond: 25,
        burstCapacity: 50,
        notificationsPerMinute: 200,
        notificationsPerDay: 20000,
      },
    });

    expect(updateRes.statusCode).toBe(200);
    const updated = JSON.parse(updateRes.payload);
    expect(updated.tenantId).toBe(testTenantId);
    expect(updated.requestsPerSecond).toBe(25);
    expect(updated.burstCapacity).toBe(50);
    expect(updated.isCustom).toBe(true);

    // Verify Tenant B still has default limits (isolation check)
    const checkB = await app.inject({
      method: 'GET',
      url: '/v1/tenant/rate-limits',
      headers: { authorization: `Bearer ${otherApiKey}` },
    });
    const bodyB = JSON.parse(checkB.payload);
    expect(bodyB.tenantId).toBe(otherTenantId);
    expect(bodyB.requestsPerSecond).toBe(10);
    expect(bodyB.burstCapacity).toBe(20);
    expect(bodyB.isCustom).toBe(false);
  });

  it('exposes rate limit headers (X-RateLimit-Limit, X-RateLimit-Remaining) on accepted requests', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/tenant/rate-limits',
      headers: { authorization: `Bearer ${testApiKey}` },
    });

    expect(res.statusCode).toBe(200);
    expect(res.headers['x-ratelimit-limit']).toBeDefined();
    expect(res.headers['x-ratelimit-remaining']).toBeDefined();
  });
});
