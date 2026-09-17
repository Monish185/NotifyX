import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/app.js';
import { prisma } from '@notifyx/database';

describe('Notification Domain Endpoints', () => {
  let app: FastifyInstance;
  let tenantId: string;
  let apiKey: string;
  let userId: string;

  beforeAll(async () => {
    app = await buildApp();
    await app.ready();

    // 1. Create tenant
    const tenant = await prisma.tenant.create({
      data: {
        name: 'Notification Test Tenant',
        slug: `notif-tenant-${Date.now()}`,
      },
    });
    tenantId = tenant.id;

    // 2. Create API key
    const keyRes = await app.inject({
      method: 'POST',
      url: '/v1/api-keys',
      payload: {
        name: 'Notif Key',
        env: 'TEST',
        tenantId,
      },
    });
    apiKey = JSON.parse(keyRes.payload).key;

    // 3. Create a user
    const userRes = await app.inject({
      method: 'POST',
      url: '/v1/users',
      headers: { authorization: `Bearer ${apiKey}` },
      payload: {
        externalId: 'cust_001',
        email: 'monish@example.com',
        phone: '+1234567890',
      },
    });
    userId = JSON.parse(userRes.payload).id;
  });

  afterAll(async () => {
    await app.close();
  });

  it('should create a notification and atomic delivery records with status PENDING', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/notifications',
      headers: { authorization: `Bearer ${apiKey}` },
      payload: {
        userId,
        templateId: 'order-shipped',
        channels: ['EMAIL', 'IN_APP'],
        priority: 'HIGH',
        payload: {
          orderId: 'ORD-999',
          amount: 1500,
        },
      },
    });

    expect(res.statusCode).toBe(202);
    const body = JSON.parse(res.payload);
    expect(body.id).toBeDefined();
    expect(body.status).toBe('PENDING');

    // Verify in database that notification and deliveries exist with status PENDING
    const dbNotif = await prisma.notification.findUnique({
      where: { id: body.id },
      include: { deliveries: true },
    });

    expect(dbNotif).not.toBeNull();
    expect(dbNotif!.status).toBe('PENDING');
    expect(dbNotif!.priority).toBe('HIGH');
    expect(dbNotif!.deliveries).toHaveLength(2);
    expect(dbNotif!.deliveries[0].status).toBe('PENDING');
    expect(dbNotif!.deliveries[1].status).toBe('PENDING');
  });

  it('should support finding user by externalId when creating notification', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/notifications',
      headers: { authorization: `Bearer ${apiKey}` },
      payload: {
        userId: 'cust_001', // externalId
        channels: ['EMAIL'],
        payload: { test: true },
      },
    });

    expect(res.statusCode).toBe(202);
    const body = JSON.parse(res.payload);
    expect(body.id).toBeDefined();
  });

  it('should retrieve notification by ID with delivery records', async () => {
    const createRes = await app.inject({
      method: 'POST',
      url: '/v1/notifications',
      headers: { authorization: `Bearer ${apiKey}` },
      payload: {
        userId,
        channels: ['EMAIL', 'PUSH'],
        priority: 'NORMAL',
        payload: { message: 'Hello' },
      },
    });
    const { id: createdId } = JSON.parse(createRes.payload);

    const getRes = await app.inject({
      method: 'GET',
      url: `/v1/notifications/${createdId}`,
      headers: { authorization: `Bearer ${apiKey}` },
    });

    expect(getRes.statusCode).toBe(200);
    const body = JSON.parse(getRes.payload);
    expect(body.id).toBe(createdId);
    expect(body.userId).toBe(userId);
    expect(body.status).toBe('PENDING');
    expect(body.deliveries).toHaveLength(2);
    const channels = body.deliveries.map((d: any) => d.channel);
    expect(channels).toContain('EMAIL');
    expect(channels).toContain('PUSH');
  });

  it('should prevent duplicate notifications when idempotencyKey is used', async () => {
    const idempotencyKey = `idemp-${Date.now()}`;
    const payload = {
      userId,
      channels: ['EMAIL'],
      payload: { order: 123 },
      idempotencyKey,
    };

    // First request
    const res1 = await app.inject({
      method: 'POST',
      url: '/v1/notifications',
      headers: { authorization: `Bearer ${apiKey}`, 'idempotency-key': idempotencyKey },
      payload,
    });
    expect(res1.statusCode).toBe(202);
    const body1 = JSON.parse(res1.payload);

    // Second request with same idempotency key
    const res2 = await app.inject({
      method: 'POST',
      url: '/v1/notifications',
      headers: { authorization: `Bearer ${apiKey}`, 'idempotency-key': idempotencyKey },
      payload,
    });
    expect(res2.statusCode).toBe(202);
    const body2 = JSON.parse(res2.payload);

    // Must return the exact same notification ID
    expect(body2.id).toBe(body1.id);
  });

  it('should list notifications with pagination', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/notifications?page=1&limit=2',
      headers: { authorization: `Bearer ${apiKey}` },
    });

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.payload);
    expect(Array.isArray(body.data)).toBe(true);
    expect(body.data.length).toBeLessThanOrEqual(2);
    expect(body.pagination).toBeDefined();
    expect(body.pagination.page).toBe(1);
    expect(body.pagination.limit).toBe(2);
    expect(body.pagination.total).toBeGreaterThanOrEqual(1);
  });

  it('should reject invalid channels with 400 Bad Request', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/notifications',
      headers: { authorization: `Bearer ${apiKey}` },
      payload: {
        userId,
        channels: ['CARRIER_PIGEON'],
      },
    });

    expect(res.statusCode).toBe(400);
  });

  it('should reject empty channels array with 400 Bad Request', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/v1/notifications',
      headers: { authorization: `Bearer ${apiKey}` },
      payload: {
        userId,
        channels: [],
      },
    });

    expect(res.statusCode).toBe(400);
  });
});
