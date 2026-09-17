import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import { prisma, OutboxStatus } from '@notifyx/database';
import { EVENT_TYPES, Channel } from '@notifyx/shared';
import { handleInAppDelivery } from '../src/handler.js';
import type { ConsumerMessageContext } from '@notifyx/kafka';

describe('In-App Worker Handler', () => {
  let tenantId: string;
  let userId: string;

  beforeAll(async () => {
    const tenant = await prisma.tenant.create({
      data: {
        name: 'In-App Worker Test Tenant',
        slug: `inapp-tenant-${Date.now()}`,
      },
    });
    tenantId = tenant.id;

    const user = await prisma.user.create({
      data: {
        tenantId,
        externalId: 'inapp_user_test',
        email: 'inapp@example.com',
      },
    });
    userId = user.id;
  });

  function createMockContext(payload: any, partition = 0, offset = '10'): ConsumerMessageContext<any> {
    return {
      topic: 'notification.delivery.requested',
      partition,
      offset,
      key: payload?.deliveryId || null,
      value: Buffer.from(JSON.stringify(payload)),
      headers: { 'tenant-id': payload?.tenantId },
      timestamp: String(Date.now()),
      parsedPayload: payload,
      heartbeat: async () => {},
    };
  }

  it('1. should process a valid IN_APP event, persist InAppNotification, and mark delivery as DELIVERED', async () => {
    // Setup Notification + Delivery in PENDING state
    const notification = await prisma.notification.create({
      data: {
        tenantId,
        userId,
        channels: ['IN_APP'],
        status: 'PENDING',
        priority: 'NORMAL',
        payload: { title: 'Welcome Gift', body: 'Claim your 50 credits!' },
      },
    });

    const delivery = await prisma.notificationDelivery.create({
      data: {
        notificationId: notification.id,
        channel: 'IN_APP',
        status: 'PENDING',
      },
    });

    const event = {
      eventId: `evt_${Date.now()}`,
      eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
      version: 1,
      occurredAt: new Date().toISOString(),
      tenantId,
      notificationId: notification.id,
      deliveryId: delivery.id,
      userId,
      channel: 'IN_APP',
      priority: 'NORMAL',
      payload: { title: 'Welcome Gift', body: 'Claim your 50 credits!' },
    };

    const ctx = createMockContext(event);
    const result = await handleInAppDelivery(ctx);

    expect(result.status).toBe('PROCESSED');
    expect(result.inAppNotificationId).toBeDefined();

    // Verify InAppNotification was created
    const inApp = await prisma.inAppNotification.findUnique({
      where: { id: result.inAppNotificationId! },
    });
    expect(inApp).not.toBeNull();
    expect(inApp!.tenantId).toBe(tenantId);
    expect(inApp!.userId).toBe(userId);
    expect(inApp!.deliveryId).toBe(delivery.id);
    expect(inApp!.title).toBe('Welcome Gift');
    expect(inApp!.body).toBe('Claim your 50 credits!');
    expect(inApp!.readAt).toBeNull(); // Starts unread

    // Verify NotificationDelivery updated to DELIVERED
    const updatedDelivery = await prisma.notificationDelivery.findUnique({
      where: { id: delivery.id },
    });
    expect(updatedDelivery!.status).toBe('DELIVERED');
    expect(updatedDelivery!.deliveredAt).not.toBeNull();

    // Verify parent Notification updated
    const updatedNotif = await prisma.notification.findUnique({
      where: { id: notification.id },
    });
    expect(updatedNotif!.status).toBe('DELIVERED');
  });

  it('2. should skip non-IN_APP events (e.g. EMAIL) without creating in-app notifications', async () => {
    const event = {
      eventId: `evt_email_${Date.now()}`,
      eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
      version: 1,
      occurredAt: new Date().toISOString(),
      tenantId,
      notificationId: 'ntf_email_test',
      deliveryId: 'del_email_test',
      userId,
      channel: 'EMAIL',
      priority: 'NORMAL',
      payload: { subject: 'Monthly Statement' },
    };

    const ctx = createMockContext(event);
    const result = await handleInAppDelivery(ctx);

    expect(result.status).toBe('SKIPPED');

    // Ensure no in-app notifications exist for this deliveryId
    const inApp = await prisma.inAppNotification.findFirst({
      where: { deliveryId: 'del_email_test' },
    });
    expect(inApp).toBeNull();
  });

  it('3. CRITICAL IDEMPOTENCY: should safely handle duplicate delivery events without duplicate records', async () => {
    const notification = await prisma.notification.create({
      data: {
        tenantId,
        userId,
        channels: ['IN_APP'],
        status: 'PENDING',
        payload: { title: 'Order Update', body: 'Your package is arriving.' },
      },
    });

    const delivery = await prisma.notificationDelivery.create({
      data: {
        notificationId: notification.id,
        channel: 'IN_APP',
        status: 'PENDING',
      },
    });

    const event = {
      eventId: `evt_dup_${Date.now()}`,
      eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
      version: 1,
      occurredAt: new Date().toISOString(),
      tenantId,
      notificationId: notification.id,
      deliveryId: delivery.id,
      userId,
      channel: 'IN_APP',
      priority: 'HIGH',
      payload: { title: 'Order Update', body: 'Your package is arriving.' },
    };

    const ctx1 = createMockContext(event, 0, '50');
    const result1 = await handleInAppDelivery(ctx1);
    expect(result1.status).toBe('PROCESSED');

    // Simulate Kafka at-least-once redelivery of the exact same event
    const ctx2 = createMockContext(event, 0, '51');
    const result2 = await handleInAppDelivery(ctx2);
    expect(result2.status).toBe('ALREADY_DELIVERED');

    // Verify EXACTLY ONE InAppNotification exists in database
    const matchingInApp = await prisma.inAppNotification.findMany({
      where: { deliveryId: delivery.id },
    });
    expect(matchingInApp).toHaveLength(1);
    expect(matchingInApp[0].id).toBe(result1.inAppNotificationId);
  });

  it('4. SECURITY: should reject event when tenantId does not match database record', async () => {
    // Create another tenant
    const otherTenant = await prisma.tenant.create({
      data: { name: 'Other Tenant', slug: `other-${Date.now()}` },
    });

    const notification = await prisma.notification.create({
      data: {
        tenantId,
        userId,
        channels: ['IN_APP'],
        status: 'PENDING',
      },
    });

    const delivery = await prisma.notificationDelivery.create({
      data: {
        notificationId: notification.id,
        channel: 'IN_APP',
        status: 'PENDING',
      },
    });

    // Event claiming to be for otherTenant
    const forgedEvent = {
      eventId: `evt_forge_${Date.now()}`,
      eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
      version: 1,
      occurredAt: new Date().toISOString(),
      tenantId: otherTenant.id, // Forged tenant!
      notificationId: notification.id,
      deliveryId: delivery.id,
      userId,
      channel: 'IN_APP',
      priority: 'NORMAL',
      payload: {},
    };

    const ctx = createMockContext(forgedEvent);
    const result = await handleInAppDelivery(ctx);

    expect(result.status).toBe('INVALID');
    expect(result.reason).toContain('Tenant mismatch');

    // Verify delivery was NOT marked DELIVERED
    const checkDelivery = await prisma.notificationDelivery.findUnique({
      where: { id: delivery.id },
    });
    expect(checkDelivery!.status).toBe('PENDING');
  });

  it('5. should reject malformed event payloads', async () => {
    const malformedEvent = {
      eventId: 'evt_bad',
      // missing eventType, tenantId, deliveryId, etc.
    };

    const ctx = createMockContext(malformedEvent);
    const result = await handleInAppDelivery(ctx);

    expect(result.status).toBe('INVALID');
  });
});
