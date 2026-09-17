import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/app.js';
import { prisma } from '@notifyx/database';

describe('In-App Inbox REST Endpoints', () => {
  let app: FastifyInstance;
  let tenantId: string;
  let apiKey: string;
  let userId: string;

  let otherTenantId: string;
  let otherApiKey: string;
  let otherUserId: string;

  beforeAll(async () => {
    app = await buildApp();
    await app.ready();

    // 1. Create primary tenant
    const tenant = await prisma.tenant.create({
      data: { name: 'Inbox Test Tenant', slug: `inbox-tenant-${Date.now()}` },
    });
    tenantId = tenant.id;

    const keyRes = await app.inject({
      method: 'POST',
      url: '/v1/api-keys',
      payload: { name: 'Inbox Key', env: 'TEST', tenantId },
    });
    apiKey = JSON.parse(keyRes.payload).key;

    const userRes = await app.inject({
      method: 'POST',
      url: '/v1/users',
      headers: { authorization: `Bearer ${apiKey}` },
      payload: { externalId: 'inbox_usr_1', email: 'inbox@example.com' },
    });
    userId = JSON.parse(userRes.payload).id;

    // 2. Create secondary tenant for isolation test
    const otherTenant = await prisma.tenant.create({
      data: { name: 'Other Tenant', slug: `other-inbox-${Date.now()}` },
    });
    otherTenantId = otherTenant.id;

    const otherKeyRes = await app.inject({
      method: 'POST',
      url: '/v1/api-keys',
      payload: { name: 'Other Key', env: 'TEST', tenantId: otherTenantId },
    });
    otherApiKey = JSON.parse(otherKeyRes.payload).key;

    const otherUserRes = await app.inject({
      method: 'POST',
      url: '/v1/users',
      headers: { authorization: `Bearer ${otherApiKey}` },
      payload: { externalId: 'other_usr_1', email: 'other@example.com' },
    });
    otherUserId = JSON.parse(otherUserRes.payload).id;
  });

  afterAll(async () => {
    await app.close();
  });

  it('1. should list in-app notifications and return unread count for user', async () => {
    // Seed 2 in-app notifications for user
    const n1 = await prisma.inAppNotification.create({
      data: {
        tenantId,
        userId,
        title: 'Order Shipped',
        body: 'Your item is on the way.',
        data: { orderId: 101 },
      },
    });

    const n2 = await prisma.inAppNotification.create({
      data: {
        tenantId,
        userId,
        title: 'Promo Discount',
        body: 'Enjoy 20% off.',
        data: { code: 'SAVE20' },
      },
    });

    // Test GET /v1/in-app-notifications?userId=...
    const listRes = await app.inject({
      method: 'GET',
      url: `/v1/in-app-notifications?userId=${userId}`,
      headers: { authorization: `Bearer ${apiKey}` },
    });

    expect(listRes.statusCode).toBe(200);
    const listBody = JSON.parse(listRes.payload);
    expect(listBody.data).toHaveLength(2);
    expect(listBody.unreadCount).toBe(2);

    // Test GET /v1/in-app-notifications/unread-count?userId=...
    const countRes = await app.inject({
      method: 'GET',
      url: `/v1/in-app-notifications/unread-count?userId=${userId}`,
      headers: { authorization: `Bearer ${apiKey}` },
    });

    expect(countRes.statusCode).toBe(200);
    const countBody = JSON.parse(countRes.payload);
    expect(countBody.unreadCount).toBe(2);
  });

  it('2. should mark a single in-app notification as read', async () => {
    const notif = await prisma.inAppNotification.create({
      data: {
        tenantId,
        userId,
        title: 'Security Alert',
        body: 'New login detected.',
      },
    });

    expect(notif.readAt).toBeNull();

    const readRes = await app.inject({
      method: 'POST',
      url: `/v1/in-app-notifications/${notif.id}/read`,
      headers: { authorization: `Bearer ${apiKey}` },
    });

    expect(readRes.statusCode).toBe(200);
    const readBody = JSON.parse(readRes.payload);
    expect(readBody.id).toBe(notif.id);
    expect(readBody.readAt).not.toBeNull();

    // Verify in database
    const dbNotif = await prisma.inAppNotification.findUnique({
      where: { id: notif.id },
    });
    expect(dbNotif!.readAt).not.toBeNull();
  });

  it('3. should mark all unread notifications as read for a user', async () => {
    // Seed 2 unread notifications
    await prisma.inAppNotification.createMany({
      data: [
        { tenantId, userId, title: 'Msg 1', body: 'Body 1' },
        { tenantId, userId, title: 'Msg 2', body: 'Body 2' },
      ],
    });

    const readAllRes = await app.inject({
      method: 'POST',
      url: '/v1/in-app-notifications/read-all',
      headers: { authorization: `Bearer ${apiKey}` },
      payload: { userId },
    });

    expect(readAllRes.statusCode).toBe(200);
    const body = JSON.parse(readAllRes.payload);
    expect(body.updatedCount).toBeGreaterThanOrEqual(2);

    // Verify unread count is now 0
    const countRes = await app.inject({
      method: 'GET',
      url: `/v1/in-app-notifications/unread-count?userId=${userId}`,
      headers: { authorization: `Bearer ${apiKey}` },
    });
    expect(JSON.parse(countRes.payload).unreadCount).toBe(0);
  });

  it('4. SECURITY: Tenant isolation prevents cross-tenant access to in-app inbox', async () => {
    const tenantBNotif = await prisma.inAppNotification.create({
      data: {
        tenantId: otherTenantId,
        userId: otherUserId,
        title: 'Confidential B',
        body: 'Secret message for Tenant B only',
      },
    });

    // Tenant A tries to mark Tenant B's notification as read
    const hackRes = await app.inject({
      method: 'POST',
      url: `/v1/in-app-notifications/${tenantBNotif.id}/read`,
      headers: { authorization: `Bearer ${apiKey}` }, // Tenant A's key!
    });
    expect(hackRes.statusCode).toBe(404);

    // Tenant A tries to query Tenant B's user inbox
    const queryHackRes = await app.inject({
      method: 'GET',
      url: `/v1/in-app-notifications?userId=${otherUserId}`,
      headers: { authorization: `Bearer ${apiKey}` }, // Tenant A's key!
    });
    expect(queryHackRes.statusCode).toBe(404);
  });
});
