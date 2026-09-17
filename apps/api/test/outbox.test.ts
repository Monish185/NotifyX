import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/app.js';
import { prisma } from '@notifyx/database';
import { EVENT_TYPES, OutboxStatus } from '@notifyx/shared';

describe('Transactional Outbox Pattern Invariants', () => {
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
        name: 'Outbox Test Tenant',
        slug: `outbox-tenant-${Date.now()}`,
      },
    });
    tenantId = tenant.id;

    // 2. Create API key
    const keyRes = await app.inject({
      method: 'POST',
      url: '/v1/api-keys',
      payload: {
        name: 'Outbox Key',
        env: 'TEST',
        tenantId,
      },
    });
    apiKey = JSON.parse(keyRes.payload).key;

    // 3. Create user
    const userRes = await app.inject({
      method: 'POST',
      url: '/v1/users',
      headers: { authorization: `Bearer ${apiKey}` },
      payload: {
        externalId: 'outbox_user_001',
        email: 'outbox.user@example.com',
      },
    });
    userId = JSON.parse(userRes.payload).id;
  });

  afterAll(async () => {
    await app.close();
  });

  it('should atomically create notification, deliveries, and outbox events with 1:1 mapping', async () => {
    const channels = ['EMAIL', 'PUSH', 'IN_APP'];
    const res = await app.inject({
      method: 'POST',
      url: '/v1/notifications',
      headers: { authorization: `Bearer ${apiKey}` },
      payload: {
        userId,
        templateId: 'order-confirmed',
        channels,
        priority: 'HIGH',
        payload: {
          orderId: 'ORD-OUTBOX-1',
          total: 499,
        },
      },
    });

    expect(res.statusCode).toBe(202);
    const body = JSON.parse(res.payload);
    const notificationId = body.id;

    // 1. Verify notification was created
    const notif = await prisma.notification.findUnique({
      where: { id: notificationId },
      include: { deliveries: true },
    });
    expect(notif).not.toBeNull();
    expect(notif!.tenantId).toBe(tenantId);
    expect(notif!.status).toBe('PENDING');

    // 2. Verify delivery records created
    expect(notif!.deliveries).toHaveLength(3);
    const deliveryIds = notif!.deliveries.map((d) => d.id);

    // 3. Verify outbox events created in PostgreSQL
    const outboxEvents = await prisma.outboxEvent.findMany({
      where: { aggregateId: notificationId },
      orderBy: { createdAt: 'asc' },
    });

    // 4. Number of outbox events matches number of deliveries
    expect(outboxEvents).toHaveLength(3);

    // 5. Each outbox event contains correct deliveryId, tenantId, and event contract payload
    for (const outbox of outboxEvents) {
      expect(outbox.tenantId).toBe(tenantId);
      expect(outbox.aggregateType).toBe('NOTIFICATION');
      expect(outbox.aggregateId).toBe(notificationId);
      expect(outbox.status).toBe(OutboxStatus.PENDING);
      expect(outbox.attempts).toBe(0);
      expect(outbox.eventType).toBe(EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED);
      expect(deliveryIds).toContain(outbox.deliveryId);

      const payload = outbox.payload as any;
      expect(payload.eventId).toBe(outbox.id);
      expect(payload.eventType).toBe(EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED);
      expect(payload.version).toBe(1);
      expect(payload.tenantId).toBe(tenantId);
      expect(payload.notificationId).toBe(notificationId);
      expect(payload.deliveryId).toBe(outbox.deliveryId);
      expect(payload.userId).toBe(userId);
      expect(channels).toContain(payload.channel);
      expect(payload.priority).toBe('HIGH');
      expect(payload.payload.orderId).toBe('ORD-OUTBOX-1');
      expect(payload.occurredAt).toBeDefined();
    }
  });

  it('should ensure atomicity: rollback leaves zero notifications, deliveries, or outbox records', async () => {
    const initialNotificationCount = await prisma.notification.count({ where: { tenantId } });
    const initialDeliveryCount = await prisma.notificationDelivery.count({
      where: { notification: { tenantId } },
    });
    const initialOutboxCount = await prisma.outboxEvent.count({ where: { tenantId } });

    // Simulate an atomic transaction failure
    await expect(
      prisma.$transaction(async (tx) => {
        const notif = await tx.notification.create({
          data: {
            tenantId,
            userId,
            channels: ['EMAIL'],
            status: 'PENDING',
          },
        });

        const deliveries = await tx.notificationDelivery.createManyAndReturn({
          data: [{
            notificationId: notif.id,
            channel: 'EMAIL',
            status: 'PENDING',
          }],
        });

        await tx.outboxEvent.create({
          data: {
            tenantId,
            aggregateType: 'NOTIFICATION',
            aggregateId: notif.id,
            deliveryId: deliveries[0].id,
            eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
            payload: {},
            status: OutboxStatus.PENDING,
          },
        });

        // Intentional abort to verify all 3 entities roll back together
        throw new Error('Simulated atomic failure');
      })
    ).rejects.toThrow('Simulated atomic failure');

    // Invariant check: All rolled back, no orphan rows
    const finalNotificationCount = await prisma.notification.count({ where: { tenantId } });
    const finalDeliveryCount = await prisma.notificationDelivery.count({
      where: { notification: { tenantId } },
    });
    const finalOutboxCount = await prisma.outboxEvent.count({ where: { tenantId } });

    expect(finalNotificationCount).toBe(initialNotificationCount);
    expect(finalDeliveryCount).toBe(initialDeliveryCount);
    expect(finalOutboxCount).toBe(initialOutboxCount);
  });
});
