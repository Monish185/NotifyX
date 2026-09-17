import { describe, it, expect, vi, beforeEach } from 'vitest';
import { BaseChannelWorker, type ChannelProvider } from '../src/channel-worker.js';
import { Channel, EVENT_TYPES, TOPICS, type NotificationDeliveryRequestedEvent } from '@notifyx/shared';
import { prisma } from '@notifyx/database';
import { logger } from '@notifyx/logger';
import {
  deliveriesAttemptedCounter,
  deliveriesSucceededCounter,
  deliveriesFailedCounter,
  retriesScheduledCounter,
  dlqEventsCounter,
  getMetrics,
} from '@notifyx/metrics';

// Mock kafkajs
vi.mock('kafkajs', () => {
  const consumerMock = vi.fn(() => ({
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    subscribe: vi.fn().mockResolvedValue(undefined),
    run: vi.fn().mockResolvedValue(undefined),
    commitOffsets: vi.fn().mockResolvedValue(undefined),
  }));

  return {
    Kafka: vi.fn(() => ({
      consumer: consumerMock,
    })),
    logLevel: { NOTHING: 0 },
  };
});

// Mock @notifyx/database
vi.mock('@notifyx/database', () => {
  const mockPrisma: any = {
    notificationDelivery: {
      findUnique: vi.fn(),
      update: vi.fn(),
      count: vi.fn().mockResolvedValue(0),
    },
    notification: {
      update: vi.fn(),
    },
    retryRecord: {
      findUnique: vi.fn(),
      create: vi.fn(),
    },
    deadLetterEvent: {
      findUnique: vi.fn(),
      create: vi.fn(),
    },
    outboxEvent: {
      create: vi.fn(),
    },
    $queryRaw: vi.fn().mockResolvedValue([{ '1': 1 }]),
    $transaction: vi.fn(async (cb: any) => cb(mockPrisma)),
  };

  return {
    prisma: mockPrisma,
    Prisma: {},
  };
});

describe('Phase 7: Observability, Correlation Propagation & Metrics', () => {
  let mockProvider: ChannelProvider;
  let worker: BaseChannelWorker;

  beforeEach(() => {
    vi.clearAllMocks();

    mockProvider = {
      name: 'MockEmailProvider',
      send: vi.fn().mockResolvedValue({
        success: true,
        providerMessageId: 'msg_test_123',
      }),
    };

    worker = new BaseChannelWorker({
      channel: Channel.EMAIL,
      serviceName: 'email-worker-obs',
      consumerGroup: 'notifyx-email-obs-group',
      brokers: ['localhost:9092'],
      healthPort: 3999,
      provider: mockProvider,
      retry: {
        maxAttempts: 3,
        baseDelayMs: 1000,
      },
      resolveRecipient: () => ({ recipient: 'user@example.com' }),
      validatePayload: () => ({ valid: true, data: { subject: 'Hi' } }),
    });
  });

  it('1. Retains and propagates event.correlationId into retry and dlq payloads', async () => {
    (mockProvider.send as any).mockResolvedValueOnce({
      success: false,
      retryable: true,
      error: 'Rate limit exceeded',
    });

    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      id: 'del_1',
      status: 'PENDING',
      attemptCount: 0,
      notification: {
        id: 'notif_1',
        tenantId: 'tenant_1',
        userId: 'user_1',
        user: { id: 'user_1', email: 'user@example.com' },
      },
    });

    const event: NotificationDeliveryRequestedEvent = {
      eventId: 'evt_alpha',
      correlationId: 'corr_alpha_999',
      eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
      version: 1,
      occurredAt: new Date().toISOString(),
      tenantId: 'tenant_1',
      notificationId: 'notif_1',
      deliveryId: 'del_1',
      userId: 'user_1',
      channel: Channel.EMAIL,
      payload: { subject: 'Hi' },
    };

    const result = await worker.processEvent({
      topic: TOPICS.NOTIFICATION_DELIVERY_REQUESTED,
      partition: 0,
      offset: '1',
      key: 'del_1',
      headers: {},
      rawPayload: event,
      parsedPayload: event,
    });

    expect(result.status).toBe('RETRY_SCHEDULED');

    // Verify OutboxEvent for retry was created WITH correlationId
    expect(prisma.outboxEvent.create).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          payload: expect.objectContaining({
            correlationId: 'corr_alpha_999',
          }),
        }),
      })
    );
  });

  it('2. Resolves correlationId from Kafka header if missing from event payload', async () => {
    (mockProvider.send as any).mockResolvedValueOnce({
      success: false,
      retryable: false, // Terminal failure -> DLQ
      error: 'Invalid recipient mailbox',
    });

    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      id: 'del_2',
      status: 'PENDING',
      attemptCount: 0,
      notification: {
        id: 'notif_2',
        tenantId: 'tenant_2',
        userId: 'user_2',
        user: { id: 'user_2', email: 'user@example.com' },
      },
    });

    const eventWithoutCorrelation: any = {
      eventId: 'evt_beta',
      // correlationId omitted intentionally
      eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
      version: 1,
      occurredAt: new Date().toISOString(),
      tenantId: 'tenant_2',
      notificationId: 'notif_2',
      deliveryId: 'del_2',
      userId: 'user_2',
      channel: Channel.EMAIL,
      payload: { subject: 'Hi' },
    };

    const result = await worker.processEvent({
      topic: TOPICS.NOTIFICATION_DELIVERY_REQUESTED,
      partition: 0,
      offset: '2',
      key: 'del_2',
      headers: { 'correlation-id': 'corr_header_888' },
      rawPayload: eventWithoutCorrelation,
      parsedPayload: eventWithoutCorrelation,
    });

    expect(result.status).toBe('FAILED');

    // Verify DLQ OutboxEvent was created with correlation-id from header
    expect(prisma.outboxEvent.create).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          payload: expect.objectContaining({
            correlationId: 'corr_header_888',
          }),
        }),
      })
    );
  });

  it('3. Assigns "unknown" and logs explicit warning if correlationId is completely missing (NEVER falls back to eventId)', async () => {
    const warnSpy = vi.spyOn(logger, 'warn');

    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      id: 'del_3',
      status: 'PENDING',
      attemptCount: 0,
      notification: {
        id: 'notif_3',
        tenantId: 'tenant_3',
        userId: 'user_3',
        user: { id: 'user_3', email: 'user@example.com' },
      },
    });

    const eventWithoutCorrelation: any = {
      eventId: 'evt_gamma_unique',
      eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
      version: 1,
      occurredAt: new Date().toISOString(),
      tenantId: 'tenant_3',
      notificationId: 'notif_3',
      deliveryId: 'del_3',
      userId: 'user_3',
      channel: Channel.EMAIL,
      payload: { subject: 'Hi' },
    };

    const result = await worker.processEvent({
      topic: TOPICS.NOTIFICATION_DELIVERY_REQUESTED,
      partition: 0,
      offset: '3',
      key: 'del_3',
      headers: {},
      rawPayload: eventWithoutCorrelation,
      parsedPayload: eventWithoutCorrelation,
    });

    expect(result.status).toBe('PROCESSED');

    // Check that explicit warning was logged
    expect(warnSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        deliveryId: 'del_3',
        eventId: 'evt_gamma_unique',
      }),
      'Missing correlationId on delivery event. Assigned fallback "unknown".'
    );
  });

  it('4. Successfully registers metrics and exports Prometheus text format', async () => {
    const metricsOutput = await getMetrics();
    expect(metricsOutput).toContain('deliveries_attempted_total');
    expect(metricsOutput).toContain('deliveries_succeeded_total');
    expect(metricsOutput).toContain('deliveries_failed_total');
    expect(metricsOutput).toContain('retries_scheduled_total');
    expect(metricsOutput).toContain('dlq_events_total');
    expect(metricsOutput).toContain('processing_latency_seconds');
  });
});
