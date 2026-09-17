import { describe, it, expect, vi, beforeEach, beforeAll, afterAll } from 'vitest';
import { prisma, OutboxStatus } from '@notifyx/database';
import { KafkaProducer } from '@notifyx/kafka';
import { OutboxPublisher } from '../src/publisher.js';
import { EVENT_TYPES, TOPICS } from '@notifyx/shared';

describe('OutboxPublisher Engine', () => {
  let mockProducer: KafkaProducer;
  let tenantId: string;

  beforeAll(async () => {
    await prisma.outboxEvent.deleteMany({});
    const tenant = await prisma.tenant.create({
      data: {
        name: 'Publisher Test Tenant',
        slug: `pub-tenant-${Date.now()}`,
      },
    });
    tenantId = tenant.id;
  });

  beforeEach(async () => {
    await prisma.outboxEvent.deleteMany({});
    mockProducer = {
      connect: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
      publish: vi.fn().mockResolvedValue(undefined),
      publishBatch: vi.fn().mockResolvedValue(undefined),
      isConnected: vi.fn().mockReturnValue(true),
      ensureTopicExists: vi.fn().mockResolvedValue(undefined),
    } as unknown as KafkaProducer;
  });

  it('1-4. should select pending event, publish to Kafka, mark as PUBLISHED, and populate publishedAt', async () => {
    const outbox = await prisma.outboxEvent.create({
      data: {
        tenantId,
        aggregateType: 'NOTIFICATION',
        aggregateId: 'ntf_pub_1',
        deliveryId: 'del_pub_1',
        eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
        payload: {
          eventId: 'evt_pub_1',
          eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
          deliveryId: 'del_pub_1',
          tenantId,
          version: 1,
          occurredAt: new Date().toISOString(),
          userId: 'usr_1',
          channel: 'EMAIL',
          priority: 'NORMAL',
          payload: { test: true },
        },
        status: OutboxStatus.PENDING,
        availableAt: new Date(Date.now() - 1000), // available now
      },
    });

    const publisher = new OutboxPublisher(mockProducer, { batchSize: 10 });
    const count = await publisher.processBatch();

    expect(count).toBeGreaterThanOrEqual(1);
    expect(mockProducer.publish).toHaveBeenCalledWith(
      TOPICS.NOTIFICATION_DELIVERY_REQUESTED,
      expect.objectContaining({ eventId: 'evt_pub_1' }),
      'del_pub_1'
    );

    // Verify updated state in database
    const updated = await prisma.outboxEvent.findUnique({
      where: { id: outbox.id },
    });
    expect(updated).not.toBeNull();
    expect(updated!.status).toBe(OutboxStatus.PUBLISHED);
    expect(updated!.publishedAt).not.toBeNull();
    expect(updated!.lastError).toBeNull();
  });

  it('5-7. failed publish should increment attempts, record lastError, and set future availableAt', async () => {
    (mockProducer.publish as any).mockRejectedValueOnce(
      new Error('Broker connection timeout')
    );

    const outbox = await prisma.outboxEvent.create({
      data: {
        tenantId,
        aggregateType: 'NOTIFICATION',
        aggregateId: 'ntf_fail_1',
        deliveryId: 'del_fail_1',
        eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
        payload: {
          eventId: 'evt_fail_1',
          eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
          deliveryId: 'del_fail_1',
          tenantId,
          version: 1,
          occurredAt: new Date().toISOString(),
          userId: 'usr_1',
          channel: 'SMS',
          priority: 'NORMAL',
          payload: {},
        },
        status: OutboxStatus.PENDING,
        attempts: 0,
        availableAt: new Date(Date.now() - 1000),
      },
    });

    const publisher = new OutboxPublisher(mockProducer, { batchSize: 10 });
    const count = await publisher.processBatch();

    // 0 succeeded
    expect(count).toBe(0);

    const updated = await prisma.outboxEvent.findUnique({
      where: { id: outbox.id },
    });
    expect(updated).not.toBeNull();
    expect(updated!.status).toBe(OutboxStatus.PENDING); // remain retryable
    expect(updated!.attempts).toBe(1);
    expect(updated!.lastError).toContain('Broker connection timeout');
    expect(updated!.availableAt.getTime()).toBeGreaterThan(Date.now());
  });

  it('8. retryable event with future availableAt is NOT claimed until available', async () => {
    // Event scheduled for 1 minute in future
    const futureEvent = await prisma.outboxEvent.create({
      data: {
        tenantId,
        aggregateType: 'NOTIFICATION',
        aggregateId: 'ntf_future_1',
        deliveryId: 'del_future_1',
        eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
        payload: {},
        status: OutboxStatus.PENDING,
        attempts: 1,
        availableAt: new Date(Date.now() + 60000),
      },
    });

    const publisher = new OutboxPublisher(mockProducer, { batchSize: 10 });
    await publisher.processBatch();

    // Event should still be untouched
    const after = await prisma.outboxEvent.findUnique({
      where: { id: futureEvent.id },
    });
    expect(after!.status).toBe(OutboxStatus.PENDING);
    expect(after!.attempts).toBe(1);
    expect(mockProducer.publish).not.toHaveBeenCalled();

    // Now advance availableAt into past:
    await prisma.outboxEvent.update({
      where: { id: futureEvent.id },
      data: { availableAt: new Date(Date.now() - 1000) },
    });

    // Run publisher again
    await publisher.processBatch();

    const afterRetry = await prisma.outboxEvent.findUnique({
      where: { id: futureEvent.id },
    });
    expect(afterRetry!.status).toBe(OutboxStatus.PUBLISHED);
  });

  it('9. safe event claiming: concurrent publishers skip locked rows and avoid duplicate claim', async () => {
    const event1 = await prisma.outboxEvent.create({
      data: {
        tenantId,
        aggregateType: 'NOTIFICATION',
        aggregateId: 'ntf_conc_1',
        deliveryId: 'del_conc_1',
        eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
        payload: { eventId: 'evt_conc_1' },
        status: OutboxStatus.PENDING,
        availableAt: new Date(Date.now() - 1000),
      },
    });

    // Publisher A locks row in a transaction
    let releaseLock: () => void;
    const lockAcquiredPromise = new Promise<void>((resolve) => {
      // Background transaction holding lock
      prisma.$transaction(async (tx: any) => {
        const rows = await tx.$queryRaw<Array<{ id: string }>>`
          SELECT id FROM outbox_events
          WHERE id = ${event1.id}
          FOR UPDATE SKIP LOCKED
        `;
        expect(rows).toHaveLength(1);
        resolve();

        // Hold lock until told to release
        await new Promise<void>((res) => {
          releaseLock = res;
        });
      });
    });

    await lockAcquiredPromise;

    // Publisher B tries to claim batch while Publisher A holds lock
    const publisherB = new OutboxPublisher(mockProducer, { batchSize: 10 });
    // B will skip the locked row thanks to SKIP LOCKED
    await publisherB.processBatch();

    // mockProducer should NOT have been called for event1 by Publisher B
    expect(mockProducer.publish).not.toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ eventId: 'evt_conc_1' }),
      expect.anything()
    );

    // Release lock
    releaseLock!();
  });

  it('10. demonstrates exponential backoff calculation', () => {
    const d1 = OutboxPublisher.calculateNextRetry(1);
    const d2 = OutboxPublisher.calculateNextRetry(2);
    const d3 = OutboxPublisher.calculateNextRetry(3);
    const d4 = OutboxPublisher.calculateNextRetry(4);

    const now = Date.now();
    expect(d1.getTime() - now).toBeGreaterThanOrEqual(4000); // ~5s
    expect(d2.getTime() - now).toBeGreaterThanOrEqual(14000); // ~15s
    expect(d3.getTime() - now).toBeGreaterThanOrEqual(29000); // ~30s
    expect(d4.getTime() - now).toBeGreaterThanOrEqual(59000); // ~60s
  });
});
