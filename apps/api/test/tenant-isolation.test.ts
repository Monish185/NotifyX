import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/app.js';
import { prisma } from '@notifyx/database';

describe('Multi-Tenant Isolation & Cross-Tenant Attack Tests', () => {
  let app: FastifyInstance;

  let tenantAId: string;
  let tenantAApiKey: string;
  let tenantAUserId: string;

  let tenantBId: string;
  let tenantBApiKey: string;
  let tenantBApiKeyId: string;
  let tenantBUserId: string;
  let tenantBNotifId: string;

  beforeAll(async () => {
    app = await buildApp();
    await app.ready();

    // 1. Provision Tenant A
    const tenantA = await prisma.tenant.create({
      data: { name: 'Tenant Alpha', slug: `alpha-${Date.now()}` },
    });
    tenantAId = tenantA.id;

    const keyARes = await app.inject({
      method: 'POST',
      url: '/v1/api-keys',
      payload: { name: 'Alpha Key', env: 'TEST', tenantId: tenantAId },
    });
    tenantAApiKey = JSON.parse(keyARes.payload).key;

    const userARes = await app.inject({
      method: 'POST',
      url: '/v1/users',
      headers: { authorization: `Bearer ${tenantAApiKey}` },
      payload: { externalId: 'user_alpha', email: 'alpha@example.com' },
    });
    tenantAUserId = JSON.parse(userARes.payload).id;

    // 2. Provision Tenant B
    const tenantB = await prisma.tenant.create({
      data: { name: 'Tenant Beta', slug: `beta-${Date.now()}` },
    });
    tenantBId = tenantB.id;

    const keyBRes = await app.inject({
      method: 'POST',
      url: '/v1/api-keys',
      payload: { name: 'Beta Key', env: 'TEST', tenantId: tenantBId },
    });
    const keyBData = JSON.parse(keyBRes.payload);
    tenantBApiKey = keyBData.key;
    tenantBApiKeyId = keyBData.id;

    const userBRes = await app.inject({
      method: 'POST',
      url: '/v1/users',
      headers: { authorization: `Bearer ${tenantBApiKey}` },
      payload: { externalId: 'user_beta', email: 'beta@example.com' },
    });
    tenantBUserId = JSON.parse(userBRes.payload).id;

    // Create a notification for Tenant B
    const notifBRes = await app.inject({
      method: 'POST',
      url: '/v1/notifications',
      headers: { authorization: `Bearer ${tenantBApiKey}` },
      payload: {
        userId: tenantBUserId,
        channels: ['EMAIL'],
        payload: { secret: 'tenant_b_secret_data' },
      },
    });
    tenantBNotifId = JSON.parse(notifBRes.payload).id;
  });

  afterAll(async () => {
    await app.close();
  });

  it("CRITICAL: Tenant A cannot retrieve Tenant B's user", async () => {
    const res = await app.inject({
      method: 'GET',
      url: `/v1/users/${tenantBUserId}`,
      headers: { authorization: `Bearer ${tenantAApiKey}` },
    });

    // Must return 404, never revealing the user belongs to another tenant
    expect(res.statusCode).toBe(404);
  });

  it("CRITICAL: Tenant A cannot create a notification targeting Tenant B's user", async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/notifications',
      headers: { authorization: `Bearer ${tenantAApiKey}` },
      payload: {
        userId: tenantBUserId, // Tenant B's user!
        channels: ['EMAIL'],
        payload: { attempt: 'cross-tenant exploit' },
      },
    });

    // Must fail because user does not exist in Tenant A scope
    expect(res.statusCode).toBe(404);
  });

  it("CRITICAL: Tenant A cannot retrieve Tenant B's notification", async () => {
    const res = await app.inject({
      method: 'GET',
      url: `/v1/notifications/${tenantBNotifId}`,
      headers: { authorization: `Bearer ${tenantAApiKey}` },
    });

    expect(res.statusCode).toBe(404);
  });

  it("CRITICAL: Tenant A cannot see Tenant B's notifications in list endpoint", async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/notifications',
      headers: { authorization: `Bearer ${tenantAApiKey}` },
    });

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.payload);
    const ids = body.data.map((n: any) => n.id);
    expect(ids).not.toContain(tenantBNotifId);
  });

  it("CRITICAL: Tenant A cannot list Tenant B's API keys", async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/api-keys',
      headers: { authorization: `Bearer ${tenantAApiKey}` },
    });

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.payload);
    const ids = body.map((k: any) => k.id);
    expect(ids).not.toContain(tenantBApiKeyId);
  });

  it("CRITICAL: Tenant A cannot revoke Tenant B's API key", async () => {
    const res = await app.inject({
      method: 'DELETE',
      url: `/v1/api-keys/${tenantBApiKeyId}`,
      headers: { authorization: `Bearer ${tenantAApiKey}` },
    });

    expect(res.statusCode).toBe(404);

    // Verify Tenant B's key is still active and unrevoked
    const checkB = await app.inject({
      method: 'GET',
      url: '/v1/api-keys',
      headers: { authorization: `Bearer ${tenantBApiKey}` },
    });
    expect(checkB.statusCode).toBe(200);
  });

  it('CRITICAL: Outbox events maintain strict tenant isolation and cannot leak across tenants', async () => {
    // 1. Verify OutboxEvent created for Tenant B belongs strictly to Tenant B
    const outboxB = await prisma.outboxEvent.findFirst({
      where: { aggregateId: tenantBNotifId },
    });

    expect(outboxB).not.toBeNull();
    expect(outboxB!.tenantId).toBe(tenantBId);

    // 2. Querying outbox events for Tenant A cannot find Tenant B's events
    const outboxAQuery = await prisma.outboxEvent.findMany({
      where: {
        tenantId: tenantAId,
        aggregateId: tenantBNotifId,
      },
    });
    expect(outboxAQuery).toHaveLength(0);

    // 3. Verify event envelope payload inside OutboxEvent preserves Tenant B context
    const payload = outboxB!.payload as any;
    expect(payload.tenantId).toBe(tenantBId);
    expect(payload.notificationId).toBe(tenantBNotifId);
    expect(payload.userId).toBe(tenantBUserId);
  });
});
